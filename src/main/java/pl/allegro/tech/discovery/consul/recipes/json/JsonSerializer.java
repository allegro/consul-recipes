package pl.allegro.tech.discovery.consul.recipes.json;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public interface JsonSerializer {

    String serializeMap(Map<String, Object> map) throws IOException;

}
