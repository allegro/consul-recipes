package pl.allegro.tech.discovery.consul.recipes.watch;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class ConsulLongPollCallback implements Callback {

    private static final Logger logger = LoggerFactory.getLogger(ConsulLongPollCallback.class);

    private final ExecutorService workerPool;

    private final HttpUrl endpoint;

    private final Consumer<WatchResult<String>> consumer;

    private final Consumer<Exception> failureConsumer;

    private final ReconnectCallback reconnect;

    private final BackoffRunner backoffRunner;

    private AtomicReference<byte[]> lastValue = new AtomicReference<>(new byte[]{0});

    private final AtomicLong currentIndex = new AtomicLong(0);

    private final AtomicInteger retryCount = new AtomicInteger(0);

    private final ConsulWatcherStats stats;

    private final Canceller callbackCanceller;

    ConsulLongPollCallback(ExecutorService workerPool,
                           BackoffRunner backoffRunner,
                           HttpUrl endpoint,
                           Consumer<WatchResult<String>> consumer,
                           Consumer<Exception> failureConsumer,
                           ReconnectCallback reconnect,
                           ConsulWatcherStats stats,
                           Canceller callbackCanceller) {
        this.workerPool = workerPool;
        this.backoffRunner = backoffRunner;
        this.endpoint = endpoint;
        this.consumer = consumer;
        this.failureConsumer = failureConsumer;
        this.reconnect = reconnect;
        this.stats = stats;
        this.callbackCanceller = callbackCanceller;
    }

    @Override
    public void onResponse(Call call, Response response) {
        if (isCancelled()) {
            if (response.body() != null) {
                response.close();
            }
            return;
        }

        if (response.isSuccessful()) {
            onSuccessfulResponse(call, response);
        } else {
            onNonOkHttpResponse(response);
        }
    }

    @Override
    public void onFailure(Call call, IOException exception) {
        if (isCancelled()) {
            return;
        }

        failureConsumer.accept(exception);
        reconnectAfterFailureAndRun(
                backoff -> logger.error("Long poll failed on endpoint {}, retrying with {}ms backoff",
                        endpoint, backoff, exception)
        );
    }

    boolean isCancelled() {
        return callbackCanceller.isCancelled();
    }

    private void onSuccessfulResponse(Call call, Response response) {
        stats.eventReceived();
        try (ResponseBody body = response.body()) {
            String indexString = response.header("X-Consul-Index");
            if (indexString == null) {
                stats.indexMissing();
                logger.error("There was no X-Consul-Index header in response for {} endpoint, retrying", endpoint);
            } else {
                long newIndex = Long.parseLong(indexString);
                handleUpdate(newIndex, body);
            }

            reconnectAfterSuccessfulResponse();
        } catch (IOException exception) {
            handleSucessfulResponseProcessingException(exception);
        }
    }

    private void reconnectAfterFailureAndRun(Consumer<Long> backoffConsumer) {
        stats.failed();
        try {
            long backoff = reconnectWithBackoff();
            backoffConsumer.accept(backoff);
        } catch (RejectedExecutionException e) {
            logger.warn("Can't reconnect. Executor probably closed.", e);
        }
    }

    private void onNonOkHttpResponse(Response response) {
        reconnectAfterFailureAndRun((backoff) -> {
            try (ResponseBody body = response.body()) {
                logNonOkHttpResponseWithBody(response, backoff, body.string());
            } catch (IOException e) {
                logNonOkHttpResponseWithException(response, backoff, e);
            }
        });
    }

    private void reconnectAfterSuccessfulResponse() {
        reconnect.reconnect(endpoint, currentIndex.get(), this);
        retryCount.set(0);
    }

    private void handleSucessfulResponseProcessingException(IOException exception) {
        reconnectAfterFailureAndRun(
                backoff -> logger.error("Failed to submit work after reading from {}, retrying with {}ms backoff",
                        endpoint, backoff, exception)
        );
    }

    private void handleContentChanged(long index, byte[] content) {
        stats.callbackCalled();
        if (logger.isTraceEnabled()) {
            logger.trace("Dispatching work on endpoint {} index {} to worker, text: {}",
                    endpoint, index, new String(content, StandardCharsets.UTF_8));
        }
        workerPool.submit(() -> consumer.accept(new WatchResult<>(index, new String(content, StandardCharsets.UTF_8))));
    }

    private void handleContentUnchanged(long index) {
        stats.contentNotChanged();
        logger.trace("Discarding event on endpoint {} index {} as content did not change", endpoint, index);
    }

    private void handleIndexNotChanged(long index) {
        stats.indexNotChanged();
        logger.trace("Discarding event on endpoint {} index {} as index did not change", endpoint, index);
    }
    private void handleIndexBackwards(long lastIndex, long newIndex) {
        stats.indexBackwards();
        logger.warn(
                "Resetting index and discarding event on endpoint {} as new index index {} is lower than previous {}",
                endpoint, newIndex, lastIndex
        );
    }

    private void logNonOkHttpResponseWithException(Response response, long backoff, IOException e) {
        logger.error(
                "Long poll on endpoint {} returned non-ok response." +
                        " Code: [{}], Failed to read body. Retrying with {}ms backoff",
                endpoint, response.code(), backoff, e
        );
    }

    private void logNonOkHttpResponseWithBody(Response response, long backoff, String bodyString) {
        logger.error(
                "Long poll on endpoint {} returned non-ok response." +
                        " Code: [{}], Body: [{}]. Retrying with {}ms backoff",
                endpoint, response.code(), bodyString, backoff
        );
    }

    private boolean contentChanged(byte[] newContent) {
        byte[] oldContent = lastValue.getAndSet(newContent);
        return !Arrays.equals(oldContent, newContent);
    }

    private void handleUpdate(long newIndex, ResponseBody body) throws IOException {
        // update index and process content
        long lastIndex = currentIndex.get();

        if (newIndex == lastIndex) {
            handleIndexNotChanged(newIndex);
        } else if (newIndex < lastIndex) { // index backwards -> reset
            currentIndex.set(0);
            handleIndexBackwards(lastIndex, newIndex);
        } else { // index moving forward
            currentIndex.set(newIndex);
            byte[] content = body.bytes();
            if (contentChanged(content)) {
                handleContentChanged(newIndex, content);
            } else {
                handleContentUnchanged(newIndex);
            }
        }
    }

    long reconnectWithBackoff() {
        currentIndex.set(0);
        return backoffRunner.runWithBackoff(retryCount.getAndIncrement(), () ->
                reconnect.reconnect(endpoint, currentIndex.get(), this));
    }
}
