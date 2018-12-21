package pl.allegro.tech.discovery.consul.recipes.session;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.discovery.consul.recipes.internal.thread.ThreadFactoryBuilder;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDeserializer;
import pl.allegro.tech.discovery.consul.recipes.json.JsonSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static pl.allegro.tech.discovery.consul.recipes.internal.http.BodyParser.readBodyOrFallback;
import static pl.allegro.tech.discovery.consul.recipes.internal.http.MediaType.JSON_MEDIA_TYPE;

public class Session implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private static final int DEFAULT_SESSION_RENEW_WINDOW_SECONDS = 2;

    private final String serviceName;
    private final OkHttpClient httpClient;
    private final HttpUrl baseUrl;
    private final int sessionTTLSeconds;
    private final int lockDelaySeconds;
    private final int sessionRenewSeconds;
    private final ScheduledExecutorService sessionRenewPool = Executors.newSingleThreadScheduledExecutor();
    private final JsonSerializer jsonSerializer;
    private final JsonDeserializer jsonDeserializer;

    private final AtomicReference<String> currentSessionId = new AtomicReference<>();

    private Session(String serviceName,
                    OkHttpClient httpClient,
                    URI baseUri,
                    int sessionTTLSeconds,
                    int lockDelaySeconds,
                    JsonSerializer jsonSerializer,
                    JsonDeserializer jsonDeserializer) {
        this.serviceName = serviceName;
        this.httpClient = httpClient;
        this.baseUrl = HttpUrl.get(baseUri);
        this.sessionTTLSeconds = sessionTTLSeconds;
        this.sessionRenewSeconds = Math.max(1, sessionTTLSeconds - DEFAULT_SESSION_RENEW_WINDOW_SECONDS);
        this.lockDelaySeconds = lockDelaySeconds;
        this.jsonSerializer = jsonSerializer;
        this.jsonDeserializer = jsonDeserializer;
    }

    public static Builder forService(String serviceName, OkHttpClient httpClient,
                                     JsonSerializer jsonSerializer, JsonDeserializer jsonDeserializer) {
        if (jsonDeserializer == null) {
            throw new IllegalStateException("Configured JsonDeserializer required.");
        }
        if (jsonSerializer == null) {
            throw new IllegalStateException("Configured JsonSerializer required.");
        }
        return new Builder(serviceName, httpClient, jsonSerializer, jsonDeserializer);
    }

    public void start() {
        newSession();
        sessionRenewPool.scheduleAtFixedRate(() -> renewSession(),
                sessionRenewSeconds, sessionRenewSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        deleteSession();
        sessionRenewPool.shutdown();
        try {
            sessionRenewPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void deleteSession() {
        final String sessionId = currentSessionId.get();
        if (sessionId == null) {
            return;
        }
        try {
            Request request = new Request.Builder()
                    .url(baseUrl.newBuilder("/v1/session/destroy/" + sessionId).build())
                    .put(RequestBody.create(JSON_MEDIA_TYPE, ""))
                    .build();

            httpClient.newCall(request).execute();
        } catch (Exception ex) {
            logger.error("Error while deleting session ", ex);
        }
    }

    public void refresh() {
        sessionRenewPool.submit(this::newSession);
    }

    private String createSession() {
        try {
            String body = jsonSerializer.serializeMap(consulSessionInfo(serviceName, sessionTTLSeconds));
            RequestBody requestBody = RequestBody.create(JSON_MEDIA_TYPE, body);

            Request request = new Request.Builder()
                    .url(baseUrl.newBuilder("/v1/session/create").build())
                    .put(requestBody)
                    .build();

            String responseBody = httpClient.newCall(request).execute().body().string();
            return (String) jsonDeserializer.deserializeMap(responseBody).get("ID");
        } catch (IOException e) {
            throw new SessionCreationException("Failed to create session", e);
        }
    }

    private Map<String, Object> consulSessionInfo(String serviceName, int sessionTTLSeconds) {
        Map<String, Object> info = new HashMap<>(2);
        info.put("Name", serviceName);
        info.put("LockDelay", Integer.toString(lockDelaySeconds) + "s");
        info.put("TTL", Integer.toString(sessionTTLSeconds) + "s");
        return info;
    }

    private void renewSession() {
        String sessionId;
        try {
            sessionId = currentId();
        } catch (SessionUninitializedException e) {
            logger.warn("Session renewal failed - not initialized yet", e);
            return;
        }

        try (Response response = callSessionRenew(sessionId)) {
            // TODO: response can contain a different TTL, which means consul needs a break and we should adjust.

            if (!response.isSuccessful()) {
                if (response.code() == HTTP_NOT_FOUND) {
                    newSession();
                } else {
                    logger.warn("Unsuccessful session renewal HTTP response. Code: {}; Body: {}",
                            response.code(), readBodyOrFallback(response, "(failed to read body)"));
                }
            }
        } catch (Exception e) {
            logger.error("Couldn't renew session {}", sessionId == null ? "(empty)" : sessionId, e);
        }
    }

    private Response callSessionRenew(String sessionId) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl.newBuilder("/v1/session/renew/" + sessionId).build())
                .put(RequestBody.create(JSON_MEDIA_TYPE, new byte[0]))
                .build();

        return httpClient.newCall(request).execute();
    }

    private void newSession() {
        try {
            this.currentSessionId.set(createSession());
        } catch (Exception e) {
            logger.warn("Creating new session failed", e);
        }
    }

    public String currentId() {
        String current = this.currentSessionId.get();
        if (current == null) {
            throw new SessionUninitializedException();
        }
        return current;
    }

    static class SessionCreationException extends RuntimeException {
        private SessionCreationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static class SessionUninitializedException extends RuntimeException {
        private SessionUninitializedException() {
            super("Session not yet obtained");
        }
    }

    public static class Builder {
        private final String serviceName;
        private final OkHttpClient httpClient;
        private final JsonSerializer jsonSerializer;
        private final JsonDeserializer jsonDeserializer;
        private URI agentUri = URI.create("http://localhost:8500");
        private int sessionTTLSeconds = 60;
        private int lockDelaySeconds = 15;
        private ScheduledExecutorService sessionRenewPool = null;

        private Builder(String serviceName, OkHttpClient httpClient,
                        JsonSerializer jsonSerializer, JsonDeserializer jsonDeserializer) {
            this.serviceName = serviceName;
            this.httpClient = httpClient;
            this.jsonSerializer = jsonSerializer;
            this.jsonDeserializer = jsonDeserializer;
        }

        public Builder withAgentUri(URI uri) {
            this.agentUri = uri;
            return this;
        }

        public Builder withSessionTTLSeconds(int seconds) {
            this.sessionTTLSeconds = seconds;
            return this;
        }

        public Builder withSessionRenewPool(ScheduledExecutorService pool) {
            this.sessionRenewPool = pool;
            return this;
        }

        public Builder withLockDelaySeconds(int seconds) {
            this.lockDelaySeconds = seconds;
            return this;
        }

        public Session build() {
            if (this.sessionRenewPool == null) {
                this.sessionRenewPool = Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder("consul-recipes-session-%d").build());
            }

            return new Session(this.serviceName,
                    this.httpClient, this.agentUri, sessionTTLSeconds,
                    lockDelaySeconds, jsonSerializer, jsonDeserializer);
        }
    }
}
