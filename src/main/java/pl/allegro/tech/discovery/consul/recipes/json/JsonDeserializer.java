package pl.allegro.tech.discovery.consul.recipes.json;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface JsonDeserializer {

    List<String> deserializeList(String content) throws IOException;

    Map<String, Object> deserializeMap(String content) throws IOException;

    List<Map<String, Object>> deserializeMapList(String content) throws IOException;
}
