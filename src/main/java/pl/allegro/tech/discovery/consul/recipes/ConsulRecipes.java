package pl.allegro.tech.discovery.consul.recipes;

import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import pl.allegro.tech.discovery.consul.recipes.datacenter.ConsulDatacenterReader;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDeserializer;
import pl.allegro.tech.discovery.consul.recipes.json.JsonSerializer;
import pl.allegro.tech.discovery.consul.recipes.leader.LeaderElector;
import pl.allegro.tech.discovery.consul.recipes.locate.ConsulAgentLocator;
import pl.allegro.tech.discovery.consul.recipes.session.Session;
import pl.allegro.tech.discovery.consul.recipes.watch.ConsulWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.EndpointWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.catalog.ServiceInstances;
import pl.allegro.tech.discovery.consul.recipes.watch.catalog.ServiceInstancesWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.catalog.Services;
import pl.allegro.tech.discovery.consul.recipes.watch.catalog.ServicesWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.health.HealthServiceInstancesWatcher;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsulRecipes {

    private OkHttpClient simpleClient = null;

    private OkHttpClient watchesClient = null;

    private final URI localAgentUri;

    private final JsonSerializer jsonSerializer;

    private final JsonDeserializer jsonDeserializer;

    private final int watchesMaxConnectionPerRoute;

    private final int watchesMaxConnectionsTotal;

    private final long simpleReadTimeoutMillis;

    private final long simpleConnectionTimeoutMillis;

    private final long watchesReadTimeoutMillis;

    private final long watchesConnectionTimeoutMillis;

    private ConsulRecipes(URI localAgentUri,
                          JsonSerializer jsonSerializer,
                          JsonDeserializer jsonDeserializer,
                          int watchesMaxConnectionPerRoute,
                          int watchesMaxConnectionsTotal,
                          long simpleReadTimeoutMillis,
                          long simpleConnectionTimeoutMillis,
                          OkHttpClient simpleClient,
                          OkHttpClient watchesClient,
                          long watchesReadTimeoutMillis,
                          long watchesConnectionTimeoutMillis) {
        this.localAgentUri = localAgentUri;
        this.jsonSerializer = jsonSerializer;
        this.jsonDeserializer = jsonDeserializer;
        this.watchesMaxConnectionPerRoute = watchesMaxConnectionPerRoute;
        this.watchesMaxConnectionsTotal = watchesMaxConnectionsTotal;
        this.simpleReadTimeoutMillis = simpleReadTimeoutMillis;
        this.simpleConnectionTimeoutMillis = simpleConnectionTimeoutMillis;
        this.simpleClient = simpleClient;
        this.watchesClient = watchesClient;
        this.watchesReadTimeoutMillis = watchesReadTimeoutMillis;
        this.watchesConnectionTimeoutMillis = watchesConnectionTimeoutMillis;
    }

    public static Builder consulRecipes() {
        return new Builder();
    }

    public ConsulDatacenterReader.Builder consulDatacenterReader() {
        return ConsulDatacenterReader.consulDatacenterReader(jsonDeserializer, getSimpleClient())
                .withAgentUri(localAgentUri);
    }

    public ConsulAgentLocator.Builder consulAgentLocator() {
        return ConsulAgentLocator.consulAgentLocator(jsonDeserializer, consulDatacenterReader().build(), getSimpleClient())
                .withAgentUri(localAgentUri);
    }

    public ConsulWatcher.Builder consulWatcher(ExecutorService workerPool) {
        return ConsulWatcher.consulWatcher(getWatchesClient(), workerPool).withAgentUri(localAgentUri);
    }

    @SuppressWarnings("unchecked")
    public EndpointWatcher<Services> catalogServicesWatcher(ConsulWatcher watcher) {
        return new ServicesWatcher(watcher, jsonDeserializer);
    }

    public EndpointWatcher<ServiceInstances> catalogServiceInstancesWatcher(String serviceName, ConsulWatcher watcher) {
        return new ServiceInstancesWatcher(serviceName, watcher, jsonDeserializer);
    }

    public EndpointWatcher<ServiceInstances> healthServiceInstancesWatcher(String serviceName, ConsulWatcher watcher) {
        return new HealthServiceInstancesWatcher(serviceName, watcher, jsonDeserializer);
    }

    public LeaderElector.Builder leaderElector(String serviceName) {
        return LeaderElector.forService(serviceName, getSimpleClient(), jsonSerializer, jsonDeserializer)
                .withAgentUri(localAgentUri);
    }

    public Session.Builder session(String serviceName) {
        return Session.forService(serviceName, getSimpleClient(), jsonSerializer, jsonDeserializer)
                .withAgentUri(localAgentUri);
    }

    private OkHttpClient getSimpleClient() {
        if (simpleClient == null) {
            this.simpleClient = new OkHttpClient.Builder()
                    .readTimeout(simpleReadTimeoutMillis, TimeUnit.MILLISECONDS)
                    .connectTimeout(simpleConnectionTimeoutMillis, TimeUnit.MILLISECONDS)
                    .build();
        }
        return simpleClient;
    }

    private OkHttpClient getWatchesClient() {
        if (watchesClient == null) {
            Dispatcher dispatcher = new Dispatcher();
            dispatcher.setMaxRequests(watchesMaxConnectionsTotal);
            dispatcher.setMaxRequestsPerHost(watchesMaxConnectionPerRoute);

            this.watchesClient = new OkHttpClient.Builder()
                    .readTimeout(watchesReadTimeoutMillis, TimeUnit.MILLISECONDS)
                    .connectTimeout(watchesConnectionTimeoutMillis, TimeUnit.MILLISECONDS)
                    .dispatcher(dispatcher)
                    .build();
        }
        return watchesClient;
    }

    public static class Builder {
        private JsonDeserializer jsonDeserializer;

        private JsonSerializer jsonSerializer;

        private OkHttpClient simpleClient;

        private OkHttpClient watchesClient;

        private URI localAgentUri = URI.create("http://localhost:8500");

        private int watchesMaxConnectionPerRoute = 1000;

        private int watchesMaxConnectionsTotal = 1000;

        private long simpleReadTimeout = 2000;

        private long simpleConnectionTimeout = 2000;

        private long watchesReadTimeout = Duration.ofMinutes(6).toMillis();

        private long watchesConnectionTimeout = Duration.ofSeconds(2).toMillis();

        private Builder() {
        }

        public ConsulRecipes build() {
            return new ConsulRecipes(
                    localAgentUri, jsonSerializer, jsonDeserializer, watchesMaxConnectionPerRoute,
                    watchesMaxConnectionsTotal, simpleReadTimeout, simpleConnectionTimeout, simpleClient, watchesClient,
                    watchesReadTimeout, watchesConnectionTimeout
            );
        }

        public Builder withJsonSerializer(JsonSerializer jsonSerializer) {
            this.jsonSerializer = jsonSerializer;
            return this;
        }

        public Builder withJsonDeserializer(JsonDeserializer jsonDeserializer) {
            this.jsonDeserializer = jsonDeserializer;
            return this;
        }

        public Builder withSimpleHttpClient(OkHttpClient simpleClient) {
            this.simpleClient = simpleClient;
            return this;
        }

        public Builder withWatchesHttpClient(OkHttpClient watchesClient) {
            this.watchesClient = watchesClient;
            return this;
        }

        public Builder withAgentUri(URI localAgentUri) {
            this.localAgentUri = localAgentUri;
            return this;
        }

        public Builder withMaxWatchedEndpoints(int maxObservedEndpoints) {
            withWatchesMaxConnectionsPerRoute(maxObservedEndpoints);
            withWatchesMaxConnectionsTotal(maxObservedEndpoints);
            return this;
        }

        public Builder withWatchesMaxConnectionsPerRoute(int maxConnectionsPerRoute) {
            this.watchesMaxConnectionPerRoute = maxConnectionsPerRoute;
            return this;
        }

        public Builder withWatchesMaxConnectionsTotal(int maxConnectionsTotal) {
            this.watchesMaxConnectionsTotal = maxConnectionsTotal;
            return this;
        }

        public Builder withSimpleConnectionTimeoutMillis(long connectionTimeout) {
            this.simpleConnectionTimeout = connectionTimeout;
            return this;
        }

        public Builder withSimpleReadTimeoutMillis(long readTimeout) {
            this.simpleReadTimeout = readTimeout;
            return this;
        }

        public Builder withWatchesConnectionTimeoutMillis(long connectionTimeout) {
            this.watchesConnectionTimeout = connectionTimeout;
            return this;
        }

        public Builder withWatchesReadTimeoutMillis(int readTimeout) {
            this.watchesReadTimeout = readTimeout;
            return this;
        }
    }
}
