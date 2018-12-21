package pl.allegro.tech.discovery.consul.recipes.watch.catalog;

import java.util.List;
import java.util.Objects;

public class ServiceInstances {
    private final String serviceName;
    private final List<ServiceInstance> instances;

    public ServiceInstances(String serviceName, List<ServiceInstance> instances) {
        this.serviceName = serviceName;
        this.instances = instances;
    }

    public String getServiceName() {
        return serviceName;
    }

    public List<ServiceInstance> getInstances() {
        return instances;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstances that = (ServiceInstances) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(instances, that.instances);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, instances);
    }
}
