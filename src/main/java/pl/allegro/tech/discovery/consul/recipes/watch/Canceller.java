package pl.allegro.tech.discovery.consul.recipes.watch;

public class Canceller {
    volatile boolean cancelled;

    public final void cancel() {
        cancelled = true;
    }

    public final boolean isCancelled() {
        return cancelled;
    }
}
