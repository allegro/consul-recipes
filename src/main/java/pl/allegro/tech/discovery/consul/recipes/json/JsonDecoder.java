package pl.allegro.tech.discovery.consul.recipes.json;

public interface JsonDecoder<T> {
    T decode(String s) throws JsonDecodeException;

    public class JsonDecodeException extends RuntimeException {

        public JsonDecodeException(String message) {
            super(message);
        }

        public JsonDecodeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
