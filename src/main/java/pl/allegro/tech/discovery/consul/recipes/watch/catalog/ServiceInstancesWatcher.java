package pl.allegro.tech.discovery.consul.recipes.watch.catalog;

import pl.allegro.tech.discovery.consul.recipes.json.JsonDecoder;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDeserializer;
import pl.allegro.tech.discovery.consul.recipes.watch.ConsulWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.EndpointWatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ServiceInstancesWatcher extends EndpointWatcher<ServiceInstances> {

    public ServiceInstancesWatcher(String serviceName, ConsulWatcher watcher, JsonDeserializer jsonDeserializer) {
        super("/v1/catalog/service/" + serviceName, watcher, new ServiceInstancesJsonDecoder(serviceName, jsonDeserializer));
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
                    .map(props -> new ServiceInstance(
                            requiredValue(props, "ServiceID", String.class),
                            requiredValue(props, "ServiceTags", List.class),
                            requiredValue(props, "ServiceAddress", String.class),
                            requiredValue(props, "ServicePort", Integer.class)
                    ))
                    .collect(Collectors.toList());
            return new ServiceInstances(serviceName, instances);
        }

        @SuppressWarnings("unchecked")
        private <T> T requiredValue(Map serviceProps, String property, Class<T> clazz) {
            Object value = serviceProps.get(property);
            if (value == null) {
                throw new JsonDecodeException(property + " property is missing in JSON. " +
                        "This may indicate that there are incompatible changes in Consul API.");
            }
            if (!clazz.isAssignableFrom(value.getClass())) {
                throw new JsonDecodeException("Invalid type of a property: " + property + ". Expected " + clazz + " got " + value.getClass()
                        + " This may indicate that there are incompatible changes in Consul API.");
            }
            return (T) value;
        }
    }
}
