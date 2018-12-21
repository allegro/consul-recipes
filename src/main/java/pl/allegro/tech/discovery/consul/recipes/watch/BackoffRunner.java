package pl.allegro.tech.discovery.consul.recipes.watch;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class BackoffRunner implements AutoCloseable {

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final long initialBackoff;

    private final long maxBackoff;

    BackoffRunner(long initialBackoff, long maxBackoff) {
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
    }

    long runWithBackoff(int retry, Runnable action) {
        long backoff = Math.min(
                initialBackoff << retry,
                maxBackoff
        );

        executorService.schedule(action, backoff, TimeUnit.MILLISECONDS);
        return backoff;
    }

    @Override
    public void close() throws Exception {
        executorService.shutdownNow();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
}
