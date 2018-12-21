package pl.allegro.tech.discovery.consul.recipes.watch.catalog;

import pl.allegro.tech.discovery.consul.recipes.json.JsonDecoder;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDeserializer;
import pl.allegro.tech.discovery.consul.recipes.watch.ConsulWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.EndpointWatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ServicesWatcher extends EndpointWatcher<Services> {

    public ServicesWatcher(ConsulWatcher watcher, JsonDeserializer jsonDeserializer) {
        super("/v1/catalog/services", watcher, decoder(jsonDeserializer));
    }

    @SuppressWarnings("unchecked")
    private static JsonDecoder<Services> decoder(JsonDeserializer jsonDeserializer) {
        if (jsonDeserializer == null) {
            throw new IllegalStateException("Configured JsonDeserializer required.");
        }
        return s -> {
            Map serviceNamesToTags;
            try {
                serviceNamesToTags = jsonDeserializer.deserializeMap(s);
            } catch (IOException e) {
                throw new JsonDecoder.JsonDecodeException("Cannot deserialize JSON", e);
            }
            return new Services((Map<String, List<String>>) serviceNamesToTags);
        };
    }

}
