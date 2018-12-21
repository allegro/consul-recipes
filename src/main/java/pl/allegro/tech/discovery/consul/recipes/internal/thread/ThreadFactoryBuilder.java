package pl.allegro.tech.discovery.consul.recipes.internal.thread;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadFactoryBuilder {

    private final String nameFormat;
    private ThreadFactory backingThreadFactory;

    public ThreadFactoryBuilder(String nameFormat) {
        throwOnInvalidFormat(nameFormat);
        this.nameFormat = nameFormat;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void throwOnInvalidFormat(String nameFormat) {
        String.format(nameFormat, 0);
    }

    public ThreadFactoryBuilder withBackingThreadFactory(ThreadFactory threadFactory) {
        this.backingThreadFactory = threadFactory;
        return this;
    }

    public ThreadFactory build() {
        AtomicInteger count = new AtomicInteger();
        ThreadFactory backingThreadFactory = this.backingThreadFactory == null ?
                Executors.defaultThreadFactory() : this.backingThreadFactory;

        return (runnable) -> {
            Thread thread = backingThreadFactory.newThread(runnable);
            String name = String.format(nameFormat, count.getAndIncrement());
            thread.setName(name);
            return thread;
        };
    }
}
