package pl.allegro.tech.discovery.consul.recipes.watch;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConsulWatcherStats {

    private final AtomicLong eventsTotal = new AtomicLong(0);

    private final AtomicLong actionableEvents = new AtomicLong(0);

    private final AtomicLong contentNotChangedEvents = new AtomicLong(0);

    private final AtomicLong indexNotChangedEvents = new AtomicLong(0);

    private final AtomicLong failures = new AtomicLong(0);

    private final RecentCounter recentFailures;

    public ConsulWatcherStats(Clock clock, long recentStatsMillis) {
        this.recentFailures = new RecentCounter(clock, recentStatsMillis);
    }

    public ConsulWatcherStats() {
        this.recentFailures = new RecentCounter(Clock.systemDefaultZone(), TimeUnit.MINUTES.toMillis(1));
    }

    void eventReceived() {
        eventsTotal.incrementAndGet();
    }

    void callbackCalled() {
        actionableEvents.incrementAndGet();
    }

    void contentNotChanged() {
        contentNotChangedEvents.incrementAndGet();
    }

    void indexNotChanged() {
        indexNotChangedEvents.incrementAndGet();
    }

    void failed() {
        failures.incrementAndGet();
        recentFailures.increment();
    }

    public long getEventsTotal() {
        return eventsTotal.get();
    }

    public long getActionableEvents() {
        return actionableEvents.get();
    }

    public long getContentNotChangedEvents() {
        return contentNotChangedEvents.get();
    }

    public long getIndexNotChangedEvents() {
        return indexNotChangedEvents.get();
    }

    public long getFailures() {
        return failures.get();
    }

    public long getRecentFailures() {
        return recentFailures.lastCompletedCount();
    }

}
