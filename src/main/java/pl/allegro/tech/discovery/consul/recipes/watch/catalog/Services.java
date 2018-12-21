package pl.allegro.tech.discovery.consul.recipes.watch.catalog;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Services {
    private final Map<String, List<String>> serviceNamesToTags;

    public Services(Map<String, List<String>> serviceNamesToTags) {
        this.serviceNamesToTags = serviceNamesToTags;
    }

    boolean containsService(String serviceName) {
        return serviceNamesToTags.containsKey(serviceName);
    }

    List<String> tagsForService(String serviceName) {
        if (!containsService(serviceName)) {
            throw new IllegalArgumentException("Service " + serviceName + " is not found in catalog.");
        }
        return serviceNamesToTags.get(serviceName);
    }

    public Set<String> serviceNames() {
        return serviceNamesToTags.keySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Services services = (Services) o;
        return Objects.equals(serviceNamesToTags, services.serviceNamesToTags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceNamesToTags);
    }
}
