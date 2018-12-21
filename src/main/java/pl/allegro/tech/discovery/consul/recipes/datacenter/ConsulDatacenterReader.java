package pl.allegro.tech.discovery.consul.recipes.datacenter;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDeserializer;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class ConsulDatacenterReader {

    private final HttpUrl agentUri;

    private final JsonDeserializer jsonDeserializer;

    private final OkHttpClient httpClient;

    private ConsulDatacenterReader(URI agentUri, JsonDeserializer jsonDeserializer, OkHttpClient httpClient) {
        this.agentUri = HttpUrl.get(agentUri);
        this.jsonDeserializer = jsonDeserializer;
        this.httpClient = httpClient;
    }

    public static ConsulDatacenterReader.Builder consulDatacenterReader(JsonDeserializer jsonDeserializer, OkHttpClient httpClient) {
        if (jsonDeserializer == null) {
            throw new IllegalStateException("Configured JsonDeserializer required.");
        }

        return new Builder(jsonDeserializer, httpClient);
    }

    public String localDatacenter() {
        try {
            String content = callEndpoint("v1/agent/self");
            Map<String, Object> data = jsonDeserializer.deserializeMap(content);

            return (String) ((Map) data.get("Config")).get("Datacenter");
        } catch (IOException exception) {
            throw new FailedToRetrieveDatacenterException("Failed to read information about local datacenter", exception);
        }
    }

    public List<String> knownDatacenters() {
        try {
            String content = callEndpoint("v1/catalog/datacenters");
            return jsonDeserializer.deserializeList(content);
        } catch (IOException exception) {
            throw new FailedToRetrieveDatacenterException("Failed to read information about known datacenters", exception);
        }
    }

    private String callEndpoint(String endpoint) throws IOException {
        HttpUrl url = agentUri.newBuilder(endpoint).build();
        Response response = httpClient.newCall(new Request.Builder().get().url(url).build()).execute();

        return response.body().string();
    }

    public static class Builder {

        private final JsonDeserializer jsonDeserializer;

        private final OkHttpClient httpClient;

        private URI agentUri = URI.create("http://localhost:8500");

        private Builder(JsonDeserializer jsonDeserializer, OkHttpClient httpClient) {
            this.jsonDeserializer = jsonDeserializer;
            this.httpClient = httpClient;
        }

        public ConsulDatacenterReader build() {
            return new ConsulDatacenterReader(agentUri, jsonDeserializer, httpClient);
        }

        public Builder withAgentUri(URI localAgentUri) {
            this.agentUri = localAgentUri;
            return this;
        }
    }
}
