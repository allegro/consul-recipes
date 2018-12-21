package pl.allegro.tech.discovery.consul.recipes.json;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class JacksonJsonSerializer implements JsonSerializer {

    private final ObjectMapper objectMapper;

    public JacksonJsonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String serializeMap(Map<String, Object> map) throws IOException {
        return objectMapper.writeValueAsString(map);
    }
}
