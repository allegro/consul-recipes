package pl.allegro.tech.discovery.consul.recipes.locate;

public class FailedToFindAgentsException extends RuntimeException {

    public FailedToFindAgentsException(String message) {
        super(message);
    }

    public FailedToFindAgentsException(String message, Throwable cause) {
        super(message, cause);
    }
}
