package pl.allegro.tech.discovery.consul.recipes.watch;

public class Disposable {
    volatile boolean cancelled;

    public final void dispose() {
        cancelled = true;
    }

    public final boolean isCancelled() {
        return cancelled;
    }
}
