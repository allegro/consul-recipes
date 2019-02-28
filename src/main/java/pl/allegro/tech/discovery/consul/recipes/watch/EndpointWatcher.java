package pl.allegro.tech.discovery.consul.recipes.watch;

import pl.allegro.tech.discovery.consul.recipes.json.JsonDecoder;

import java.util.function.Consumer;

public class EndpointWatcher<T> {
    private final String endpoint;
    private final ConsulWatcher watcher;
    private final JsonDecoder<T> decoder;

    public EndpointWatcher(String endpoint, ConsulWatcher watcher, JsonDecoder<T> decoder) {
        this.endpoint = endpoint;
        this.watcher = watcher;
        this.decoder = decoder;
    }

    public Disposable watch(Consumer<WatchResult<T>> consumer, Consumer<Exception> failureConsumer) {
        return watcher.watchEndpoint(endpoint, (watchResult) -> {
            try {
                consumer.accept(watchResult.map(decoder::decode));
            } catch (Exception e) {
                failureConsumer.accept(e);
            }
        }, failureConsumer);
    }

    public ConsulWatcherStats stats() {
        return watcher.stats();
    }

    public String endpoint() {
        return endpoint;
    }
}
