package pl.allegro.tech.discovery.consul.recipes.watch.catalog;

import java.util.List;
import java.util.Objects;

public class ServiceInstance {
    private final String serviceId;
    private final List<String> serviceTags;
    private final String serviceAddress;
    private final int servicePort;

    public ServiceInstance(String serviceId, List<String> serviceTags, String serviceAddress, int servicePort) {
        this.serviceId = serviceId;
        this.serviceTags = serviceTags;
        this.serviceAddress = serviceAddress;
        this.servicePort = servicePort;
    }

    public String getServiceId() {
        return serviceId;
    }

    public List<String> getServiceTags() {
        return serviceTags;
    }

    public String getServiceAddress() {
        return serviceAddress;
    }

    public int getServicePort() {
        return servicePort;
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
