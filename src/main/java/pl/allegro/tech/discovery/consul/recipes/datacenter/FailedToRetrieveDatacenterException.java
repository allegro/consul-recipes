package pl.allegro.tech.discovery.consul.recipes.datacenter;

public class FailedToRetrieveDatacenterException extends RuntimeException {

    public FailedToRetrieveDatacenterException(String message, Throwable cause) {
        super(message, cause);
    }
}
