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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class ConsulLongPollCallback implements Callback {

    enum UpdateIndexStatus {
        UPDATED,
        UNCHANGED,
        RESET
    }

    private static final Logger logger = LoggerFactory.getLogger(ConsulLongPollCallback.class);

    private final ExecutorService workerPool;

    private final HttpUrl endpoint;

    private final Consumer<WatchResult<String>> consumer;

    private final Consumer<Exception> failureConsumer;

    private final ReconnectCallback reconnect;

    private final BackoffRunner backoffRunner;

    private final AtomicReference<byte[]> lastValue = new AtomicReference<>(new byte[]{0});

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
            Optional<String> consulIndexHeaderValue = Optional.ofNullable(response.header("X-Consul-Index"));
            if (consulIndexHeaderValue.isPresent()) {
                long newIndexValue = Long.parseLong(consulIndexHeaderValue.get());
                handleIndexUpdate(body, newIndexValue);
            } else {
                logger.error("There was no X-Consul-Index header in response for {} endpoint, retrying", endpoint);
            }

            reconnectAfterSuccessfulResponse();
        } catch (IOException exception) {
            handleSucessfulResponseProcessingException(exception);
        }
    }

    private void handleIndexUpdate(ResponseBody body, long newIndexValue) throws IOException {
        switch (defineIndexUpdateStatus(newIndexValue)) {
            case UPDATED:
                currentIndex.set(newIndexValue);
                byte[] content = body.bytes();
                if (contentChanged(content)) {
                    handleContentChanged(content);
                } else {
                    handleContentUnchanged();
                }
                break;

            case UNCHANGED:
                stats.indexNotChanged();
                handleContentUnchanged();
                break;

            case RESET:
                stats.indexBackwards();
                logger.warn("Discarding event on endpoint {} as new index index {} is lower than previous {} - resetting index", endpoint, newIndexValue, currentIndex.get());
                currentIndex.set(0);
                break;
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

    private void handleContentChanged(byte[] content) {
        stats.callbackCalled();
        long index = currentIndex.get();
        if (logger.isTraceEnabled()) {
            logger.trace("Dispatching work on endpoint {} index {} to worker, text: {}",
                    endpoint, index, new String(content, StandardCharsets.UTF_8));
        }
        workerPool.submit(() -> consumer.accept(new WatchResult<>(index, new String(content, StandardCharsets.UTF_8))));
    }

    private void handleContentUnchanged() {
        stats.contentNotChanged();
        logger.trace("Discarding event on endpoint {} index {} as content did not change", endpoint, currentIndex.get());
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

    private UpdateIndexStatus defineIndexUpdateStatus(long newIndexValue) {
        long previousIndexValue = currentIndex.get();
        if (newIndexValue == previousIndexValue) {
            return UpdateIndexStatus.UNCHANGED;
        } else if (newIndexValue < previousIndexValue) {
            return UpdateIndexStatus.RESET;
        } else {
            return UpdateIndexStatus.UPDATED;
        }
    }

    long reconnectWithBackoff() {
        currentIndex.set(0);
        return backoffRunner.runWithBackoff(retryCount.getAndIncrement(), () ->
                reconnect.reconnect(endpoint, currentIndex.get(), this));
    }
}
