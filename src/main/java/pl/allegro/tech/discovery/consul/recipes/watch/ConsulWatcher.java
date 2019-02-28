package pl.allegro.tech.discovery.consul.recipes.watch;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ConsulWatcher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ConsulWatcher.class);

    private final ExecutorService workerPool;

    private final OkHttpClient httpClient;

    private final HttpUrl baseUrl;

    private final BackoffRunner backoffRunner;

    private final boolean allowStale;

    private final ConsulWatcherStats stats;

    private ConsulWatcher(URI uri, ExecutorService workerPool, OkHttpClient httpClient,
                          Clock clock,
                          boolean allowStale,
                          long initialBackoff,
                          long maxBackoff,
                          long recentStatsMillis) {
        this.baseUrl = HttpUrl.get(uri);
        this.workerPool = workerPool;
        this.httpClient = httpClient;
        this.backoffRunner = new BackoffRunner(initialBackoff, maxBackoff);
        this.allowStale = allowStale;
        this.stats = new ConsulWatcherStats(clock, recentStatsMillis);
    }

    public static ConsulWatcher.Builder consulWatcher(OkHttpClient httpClient, ExecutorService workerPool) {
        return new ConsulWatcher.Builder(httpClient, workerPool);
    }

    public Disposable watchEndpoint(String endpoint, Consumer<WatchResult<String>> consumer, Consumer<Exception> failureConsumer) {
        HttpUrl normalizedEndpoint = normalizeEndpoint(endpoint);
        logger.info("Starting HTTP long poll for endpoint: {}", normalizedEndpoint);
        Disposable disposable = new Disposable();
        watchAtIndex(
                normalizedEndpoint,
                new ConsulLongPollCallback(
                        workerPool,
                        backoffRunner,
                        normalizedEndpoint,
                        consumer,
                        failureConsumer,
                        this::reconnect,
                        stats,
                        disposable),
                0
        );

        return disposable;
    }

    private HttpUrl normalizeEndpoint(String endpoint) {
        HttpUrl.Builder builder = baseUrl.newBuilder(endpoint)
                .addQueryParameter("wait", "5m");

        if (allowStale) {
            builder.addQueryParameter("stale", "");
        }

        return builder.build();
    }

    private void watchAtIndex(HttpUrl endpoint, ConsulLongPollCallback callback, long index) {
        if (!callback.isCancelled()) {
            logger.trace("Starting long poll at endpoint {} with index {}", endpoint, index);

            HttpUrl url = endpoint.newBuilder()
                    .addQueryParameter("index", Long.toString(index))
                    .build();

            Request request = new Request.Builder().get().url(url).build();
            httpClient.newCall(request).enqueue(callback);
        }
    }

    private void reconnect(HttpUrl endpoint, long index, ConsulLongPollCallback callback) {
        watchAtIndex(endpoint, callback, index);
    }

    public ConsulWatcherStats stats() {
        return stats;
    }

    @Override
    public void close() throws Exception {
        httpClient.dispatcher().cancelAll();
        this.backoffRunner.close();
    }

    public static class Builder {

        private URI agentUri = URI.create("http://localhost:8500");

        private final ExecutorService workerPool;

        private final OkHttpClient httpClient;

        private Clock clock = Clock.systemDefaultZone();

        private boolean allowStale = true;

        private int initialReconnectBackoffMillis = 100;

        private int maxReconnectBackoffMillis = 60 * 1000;

        private long recentStatsMillis = TimeUnit.MINUTES.toMillis(1);

        private Builder(OkHttpClient httpClient, ExecutorService workerPool) {
            this.workerPool = workerPool;
            this.httpClient = httpClient;
        }

        public ConsulWatcher build() {
            return new ConsulWatcher(agentUri, workerPool, httpClient, clock, allowStale,
                    initialReconnectBackoffMillis, maxReconnectBackoffMillis, recentStatsMillis);
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder withAgentUri(URI agentUri) {
            this.agentUri = agentUri;
            return this;
        }

        public Builder withBackoff(int initialReconnectBackoffMillis, int maxReconnectBackoffMillis) {
            this.initialReconnectBackoffMillis = initialReconnectBackoffMillis;
            this.maxReconnectBackoffMillis = maxReconnectBackoffMillis;
            return this;
        }

        public Builder requireDefaultConsistency() {
            this.allowStale = false;
            return this;
        }

        public Builder withRecentStatsMillis(long recentStatsMillis) {
            this.recentStatsMillis = recentStatsMillis;
            return this;
        }
    }
}
