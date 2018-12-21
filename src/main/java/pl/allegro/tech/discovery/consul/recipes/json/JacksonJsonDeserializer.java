package pl.allegro.tech.discovery.consul.recipes.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JacksonJsonDeserializer implements JsonDeserializer {

    private static final TypeReference<List<String>> LIST_TYPE = new TypeReference<List<String>>() {
    };

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
    };

    private static final TypeReference<List<Map<String, Object>>> MAP_LIST_TYPE = new TypeReference<List<Map<String, Object>>>() {
    };

    private final ObjectMapper objectMapper;

    public JacksonJsonDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public List<String> deserializeList(String content) throws IOException {
        return objectMapper.readValue(content, LIST_TYPE);
    }

    @Override
    public Map<String, Object> deserializeMap(String content) throws IOException {
        return objectMapper.readValue(content, MAP_TYPE);
    }

    @Override
    public List<Map<String, Object>> deserializeMapList(String content) throws IOException {
        return objectMapper.readValue(content, MAP_LIST_TYPE);
    }

}
