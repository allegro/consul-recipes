package pl.allegro.tech.discovery.consul.recipes.json;

import java.util.Map;

public class JsonValueReader {

    @SuppressWarnings("unchecked")
    public static <T> T requiredValue(Map serviceProps, String property, Class<T> clazz) {
        Object value = serviceProps.get(property);
        if (value == null) {
            throw new JsonDecoder.JsonDecodeException(property + " property is missing in JSON. " +
                    "This may indicate that there are incompatible changes in Consul API.");
        }
        mustBeAbleToCastToClass(property, clazz, value);
        return (T) value;
    }


    @SuppressWarnings("unchecked")
    public static <T> T optionalValue(Map serviceProps, String property, Class<T> clazz) {
        Object value = serviceProps.get(property);
        if (value == null) {
            return null;
        }
        mustBeAbleToCastToClass(property, clazz, value);
        return (T) value;
    }

    private static <T> void mustBeAbleToCastToClass(String property, Class<T> clazz, Object value) {
        if (!clazz.isAssignableFrom(value.getClass())) {
            throw new JsonDecoder.JsonDecodeException("Invalid type of a property: " + property + ". Expected " + clazz + " got " + value.getClass()
                    + " This may indicate that there are incompatible changes in Consul API.");
        }
    }
}
