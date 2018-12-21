package pl.allegro.tech.discovery.consul.recipes.watch

import spock.lang.Specification

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.TimeUnit

class RecentCounterTest extends Specification {

    def intervalMillis = TimeUnit.MINUTES.toMillis(1)
    def clock = new TickClock(Instant.ofEpochMilli((long) (intervalMillis / 2L)))
    def counter = new RecentCounter(clock, intervalMillis)

    def "should count current instant"() {
        when:
        counter.increment()
        counter.increment()

        then:
        counter.currentCount() == 2
    }

    def "should count only last minute"() {
        given:
        counter.increment()
        counter.increment()
        tick()

        when:
        counter.increment()

        then:
        counter.currentCount() == 1
        counter.lastCompletedCount() == 2
    }

    def "should clear older interval counts"() {
        given:
        counter.increment()
        tick()

        counter.increment()
        counter.increment()
        tick()

        tick()

        expect:
        counter.currentCount() == 0
        counter.lastCompletedCount() == 0
    }

    def "should clear counter on index clash at later time"() {
        given:
        3.times {
            counter.increment()
            tick()
        }


        3.times { tick() }

        expect:
        counter.currentCount() == 0
        counter.lastCompletedCount() == 0
    }

    def "should keep counting new values after a pause in usage"() {
        def completedCounts = []
        def currentCounts = []

        given:
        10.times { counter.increment() }
        currentCounts << counter.currentCount()
        completedCounts << counter.lastCompletedCount()

        tick()

        15.times { counter.increment() }
        currentCounts << counter.currentCount()
        completedCounts << counter.lastCompletedCount()

        tick()

        7.times { counter.increment() }
        currentCounts << counter.currentCount()
        completedCounts << counter.lastCompletedCount()

        tick()

        3.times { counter.increment() }
        currentCounts << counter.currentCount()
        completedCounts << counter.lastCompletedCount()

        tick()
        currentCounts << counter.currentCount()
        completedCounts << counter.lastCompletedCount()

        tick()
        currentCounts << counter.currentCount()
        completedCounts << counter.lastCompletedCount()

        2.times { counter.increment() }
        currentCounts << counter.currentCount()
        completedCounts << counter.lastCompletedCount()

        tick()
        currentCounts << counter.currentCount()
        completedCounts << counter.lastCompletedCount()

        expect:
        currentCounts == [10, 15, 7, 3, 0, 0, 2, 0]
        completedCounts == [0, 10, 15, 7, 3, 0, 0, 2]
    }

    def "should throw exception if interval is too low"() {
        when:
        new RecentCounter(clock, 500)

        then:
        thrown(IllegalArgumentException)
    }

    private void tick() {
        clock.advance(intervalMillis)
    }

    private static class TickClock extends Clock {

        Instant instant

        TickClock(Instant instant) {
            this.instant = instant
        }

        @Override
        ZoneId getZone() {
            return ZoneId.systemDefault()
        }

        @Override
        Clock withZone(ZoneId zone) {
            return fixed(instant, this.getZone())
        }

        @Override
        Instant instant() {
            return instant
        }

        void advance(long millis) {
            instant = instant + Duration.ofMillis(millis)
        }
    }
}
