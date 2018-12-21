package pl.allegro.tech.discovery.consul.recipes.locate;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.discovery.consul.recipes.datacenter.ConsulDatacenterReader;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDeserializer;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class ConsulAgentLocator {

    private static final Logger logger = LoggerFactory.getLogger(ConsulAgentLocator.class);

    private static final String CONSUL_SERVICE_NAME = "consul";

    private final HttpUrl agentUri;

    private final JsonDeserializer jsonDeserializer;

    private final ConsulDatacenterReader locationReader;

    private final OkHttpClient httpClient;

    private final int agentPort;

    private ConsulAgentLocator(URI agentUri, JsonDeserializer jsonDeserializer, ConsulDatacenterReader locationReader, OkHttpClient httpClient, int agentPort) {
        this.agentUri = HttpUrl.get(agentUri);
        this.jsonDeserializer = jsonDeserializer;
        this.locationReader = locationReader;
        this.httpClient = httpClient;
        this.agentPort = agentPort;
    }

    public static ConsulAgentLocator.Builder consulAgentLocator(JsonDeserializer jsonDeserializer, ConsulDatacenterReader locationReader, OkHttpClient httpClient) {
        if (jsonDeserializer == null) {
            throw new IllegalStateException("Configured JsonDeserializer required.");
        }

        return new Builder(jsonDeserializer, locationReader, httpClient);
    }

    public Map<String, AgentData> locateDatacenterAgents() {
        return locateDatacenterAgents(CONSUL_SERVICE_NAME);
    }

    /**
     * Returns URI to agent in each DC for given service name.
     * If more than one service instances is found, random agent is chosen.
     */
    public Map<String, AgentData> locateDatacenterAgents(String serviceName) {
        return locateDatacenterAgents(serviceName, (data) -> {
            int agentIndex = ThreadLocalRandom.current().nextInt(data.size());
            return data.get(agentIndex);
        });
    }

    /**
     * Returns URI to agent in each DC for given service name.
     * It is possible to customize function that chooses the agent among service instances. Function receives raw
     * JSON mapped to Java structures and should return node chosen from the list.
     * <p>
     */
    public Map<String, AgentData> locateDatacenterAgents(String serviceName, Function<List<Map<String, Object>>, Map<String, Object>> preferredAgent) {
        String localDatacenter = locationReader.localDatacenter();
        List<String> knownDatacenters = locationReader.knownDatacenters();

        Map<String, AgentData> agents = new HashMap<>();
        for (String datacenter : knownDatacenters) {
            if (localDatacenter.equals(datacenter)) {
                agents.put(datacenter, new AgentData("localhost", agentUri.uri()));
            } else {
                AgentData agentData = readAgentAddress(datacenter, serviceName, preferredAgent);
                if (agentData != null) {
                    agents.put(datacenter, agentData);
                }

            }
        }

        if (agents.isEmpty()) {
            throw new FailedToFindAgentsException("Failed to find any agents for service name: " + serviceName);
        }

        return agents;
    }

    private AgentData readAgentAddress(String datacenter, String serviceName, Function<List<Map<String, Object>>, Map<String, Object>> preferredAgent) {
        HttpUrl uri = agentUri.newBuilder("v1/health/service/" + serviceName)
                .addQueryParameter("passing", null).addQueryParameter("dc", datacenter)
                .build();

        try {
            Response response = httpClient.newCall(new Request.Builder().get().url(uri).build()).execute();
            if (!response.isSuccessful()) {
                logger.error("Received status code other than 2xx when asking for service {} at {}", serviceName, uri);
                return null;
            }

            List<Map<String, Object>> data = jsonDeserializer.deserializeMapList(response.body().string());
            if (data.isEmpty()) {
                logger.error("No healthy instances of service {} found at {}", serviceName, uri);
                return null;
            }

            String address = (String) ((Map) preferredAgent.apply(data).get("Node")).get("Address");
            String name = (String) ((Map) preferredAgent.apply(data).get("Node")).get("Node");
            return new AgentData(name, URI.create("http://" + address + ":" + agentPort));
        } catch (Exception exception) {
            logger.error("Failed to read information about remote agent from: {}", uri, exception);
            return null;
        }
    }

    public static class Builder {

        private final JsonDeserializer jsonDeserializer;

        private final ConsulDatacenterReader locationReader;

        private final OkHttpClient httpClient;

        private URI agentUri = URI.create("http://localhost:8500");

        private int agentPort = 8500;

        private Builder(JsonDeserializer jsonDeserializer, ConsulDatacenterReader locationReader, OkHttpClient httpClient) {
            this.jsonDeserializer = jsonDeserializer;
            this.locationReader = locationReader;
            this.httpClient = httpClient;
        }

        public ConsulAgentLocator build() {
            return new ConsulAgentLocator(agentUri, jsonDeserializer, locationReader, httpClient, agentPort);
        }

        public Builder withAgentUri(URI localAgentUri) {
            this.agentUri = localAgentUri;
            return this;
        }

        public Builder withAgentPort(int agentPort) {
            this.agentPort = agentPort;
            return this;
        }
    }

    public static class AgentData {

        private final String name;
        private final URI uri;

        AgentData(String name, URI uri) {
            this.name = name;
            this.uri = uri;
        }

        public String getName() {
            return name;
        }

        public URI getUri() {
            return uri;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AgentData)) return false;
            AgentData agentData = (AgentData) o;
            return Objects.equals(name, agentData.name) &&
                    Objects.equals(uri, agentData.uri);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, uri);
        }
    }
}
