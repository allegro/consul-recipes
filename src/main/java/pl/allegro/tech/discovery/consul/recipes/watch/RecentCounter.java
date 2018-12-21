package pl.allegro.tech.discovery.consul.recipes.watch;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class RecentCounter {

    private final Clock clock;

    private final long intervalMillis;

    private AtomicLong lastUpdate;

    private AtomicInteger lastIndex;

    private LongAdder[] counts = new LongAdder[] { new LongAdder(), new LongAdder(), new LongAdder()};

    public RecentCounter(Clock clock, long intervalMillis) {
        if (intervalMillis < TimeUnit.SECONDS.toMillis(1)) {
            throw new IllegalArgumentException("Interval needs to be at least 1 second. "
                    + intervalMillis + "ms provided.");
        }

        this.clock = clock;
        this.intervalMillis = intervalMillis;
        this.lastUpdate = new AtomicLong(clock.millis());
        this.lastIndex = new AtomicInteger(currentIndex(lastUpdate.get()));
    }

    public void increment() {
        long now = clock.millis();
        int index = currentIndex(now);
        int nextIndex = nextIndex(now);
        int previousIndex = previousIndex(now);

        if (lastUpdate.get() <= now - intervalMillis * 2) {
            counts[index].reset();
            counts[nextIndex].reset();
            counts[previousIndex].reset();
        } else {
            if (lastIndex.get() != index) {
                counts[index].reset();
            }
        }
        counts[index].increment();

        lastUpdate.set(now);
        lastIndex.set(index);
    }

    public long lastCompletedCount() {
        long now = clock.millis();
        if (lastUpdate.get() <= now - 2 * intervalMillis) {
            return 0;
        }
        return counts[previousIndex(now)].sum();
    }

    public long currentCount() {
        long now = clock.millis();
        if (lastUpdate.get() <= now - intervalMillis) {
            return 0;
        }
        return counts[currentIndex(now)].sum();
    }

    private int currentIndex(long millis) {
        return offsetIndex(millis, 0);
    }

    private int previousIndex(long millis) {
        return offsetIndex(millis, 2);
    }

    private int nextIndex(long millis) {
        return offsetIndex(millis, 1);
    }

    private int offsetIndex(long millis, int offset) {
        return ((int) (millis / intervalMillis) + offset) % 3;
    }
}
