package pl.allegro.tech.discovery.consul.recipes.watch.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDecoder;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDeserializer;
import pl.allegro.tech.discovery.consul.recipes.watch.ConsulWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.EndpointWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.catalog.ServiceInstance;
import pl.allegro.tech.discovery.consul.recipes.watch.catalog.ServiceInstances;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static pl.allegro.tech.discovery.consul.recipes.json.JsonValueReader.requiredValue;

public class HealthServiceInstancesWatcher extends EndpointWatcher<ServiceInstances> {

    private static final Logger logger = LoggerFactory.getLogger(HealthServiceInstancesWatcher.class);

    public HealthServiceInstancesWatcher(String serviceName, ConsulWatcher watcher, JsonDeserializer jsonDeserializer) {
        super("/v1/health/service/" + serviceName + "?passing=true", watcher,
                new ServiceInstancesJsonDecoder(serviceName, jsonDeserializer));
    }

    private static class ServiceInstancesJsonDecoder implements JsonDecoder<ServiceInstances> {

        private final String serviceName;
        private final JsonDeserializer jsonDeserializer;

        ServiceInstancesJsonDecoder(String serviceName, JsonDeserializer jsonDeserializer) {
            if (jsonDeserializer == null) {
                throw new IllegalStateException("Configured JsonDeserializer required.");
            }
            this.serviceName = serviceName;
            this.jsonDeserializer = jsonDeserializer;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ServiceInstances decode(String s) {
            List<Map<String, Object>> services;
            try {
                services = jsonDeserializer.deserializeMapList(s);
            } catch (IOException e) {
                throw new JsonDecodeException("Cannot deserialize JSON", e);
            }

            List<ServiceInstance> instances = services.stream()
                    .map(props -> {
                        try {
                            Map<String, ?> service = requiredValue(props, "Service", Map.class);
                            return new ServiceInstance(
                                    requiredValue(service, "ID", String.class),
                                    requiredValue(service, "Tags", List.class),
                                    requiredValue(service, "Address", String.class),
                                    requiredValue(service, "Port", Integer.class)
                            );
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e.getCause());
                            return null;
                        }
                    }).filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return new ServiceInstances(serviceName, instances);
        }
    }
}
