package pl.allegro.tech.discovery.consul.recipes.watch.catalog;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ServiceInstance {
    private final String serviceId;
    private final List<String> serviceTags;
    private final String serviceAddress;
    private final Integer servicePort;

    public ServiceInstance(String serviceId, List<String> serviceTags, String serviceAddress, Integer servicePort) {
        this.serviceId = serviceId;
        this.serviceTags = serviceTags != null ? serviceTags : Collections.emptyList();
        this.serviceAddress = serviceAddress;
        this.servicePort = servicePort;
    }

    public String getServiceId() {
        return serviceId;
    }

    public List<String> getServiceTags() {
        return serviceTags;
    }

    public Optional<String> getServiceAddress() {
        return Optional.ofNullable(serviceAddress);
    }

    public Optional<Integer> getServicePort() {
        return Optional.ofNullable(servicePort);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstance that = (ServiceInstance) o;
        return servicePort == that.servicePort &&
                Objects.equals(serviceId, that.serviceId) &&
                Objects.equals(serviceTags, that.serviceTags) &&
                Objects.equals(serviceAddress, that.serviceAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceId, serviceTags, serviceAddress, servicePort);
    }
}
