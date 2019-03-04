package pl.allegro.tech.discovery.consul.recipes.leader;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.discovery.consul.recipes.ConsulRecipes;
import pl.allegro.tech.discovery.consul.recipes.internal.thread.ThreadFactoryBuilder;
import pl.allegro.tech.discovery.consul.recipes.json.JsonDeserializer;
import pl.allegro.tech.discovery.consul.recipes.json.JsonSerializer;
import pl.allegro.tech.discovery.consul.recipes.session.Session;
import pl.allegro.tech.discovery.consul.recipes.watch.Canceller;
import pl.allegro.tech.discovery.consul.recipes.watch.ConsulWatcher;
import pl.allegro.tech.discovery.consul.recipes.watch.WatchResult;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static pl.allegro.tech.discovery.consul.recipes.internal.http.BodyParser.readBodyOrFallback;
import static pl.allegro.tech.discovery.consul.recipes.internal.http.MediaType.JSON_MEDIA_TYPE;

public class LeaderElector implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElector.class);

    private final String serviceName;
    private final String nodeId;
    private final OkHttpClient httpClient;
    private final HttpUrl baseUrl;
    private final ConsulWatcher consulWatcher;
    private final JsonDeserializer jsonDeserializer;
    private final List<LeadershipObserver> observers = new CopyOnWriteArrayList<>();
    private final Session session;
    private final ScheduledExecutorService acquirementPool;
    private final int lockDelaySeconds;
    private final int lockRescueDelaySeconds;
    private final LockAcquirer lockAcquirer;

    private Canceller watchCanceller;

    private volatile boolean isLeader = false;

    private LeaderElector(String serviceName,
                          String nodeId,
                          URI baseUri,
                          OkHttpClient httpClient,
                          Session session,
                          ScheduledExecutorService acquirementPool,
                          ConsulWatcher consulWatcher,
                          JsonDeserializer jsonDeserializer,
                          int lockDelaySeconds,
                          int lockRescueDelaySeconds
    ) {
        this.serviceName = serviceName;
        this.nodeId = nodeId;
        this.baseUrl = HttpUrl.get(baseUri);
        this.httpClient = httpClient;
        this.session = session;
        this.acquirementPool = acquirementPool;
        this.consulWatcher = consulWatcher;
        this.jsonDeserializer = jsonDeserializer;
        this.lockDelaySeconds = lockDelaySeconds;
        this.lockRescueDelaySeconds = lockRescueDelaySeconds;
        this.lockAcquirer = new LockAcquirer(lockUrl(serviceName));
    }

    public static Builder forService(String serviceName,
                                     OkHttpClient httpClient,
                                     JsonSerializer jsonSerializer,
                                     JsonDeserializer jsonDeserializer) {
        if (jsonDeserializer == null) {
            throw new IllegalStateException("Configured JsonDeserializer required.");
        }
        if (jsonSerializer == null) {
            throw new IllegalStateException("Configured JsonSerializer required.");
        }
        return new Builder(serviceName, httpClient, jsonSerializer, jsonDeserializer);
    }

    public void start() {
        session.start();
        lockAcquirer.start();
        watchCanceller = consulWatcher.watchEndpoint(lockEndpoint(serviceName), lockAcquirer::leaderNodeUpdate, this::watchException);
    }

    @Override
    public void close() {
        session.close();
        lockAcquirer.close();
        if (watchCanceller != null) {
            watchCanceller.cancel();
        }
        notALeader();
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void registerObserver(LeadershipObserver observer) {
        observers.add(observer);
    }

    public void unregisterObserver(LeadershipObserver observer) {
        observers.remove(observer);
    }

    private void watchException(Exception exception) {
        try {
            logger.info("Got a leadership watch exception. Clearing leadership status", exception);
            notALeader();
        } catch (Exception e) {
            logger.warn("Issue notifying about leader node watch exception", e);
        }
    }

    private void becameLeader() {
        boolean wasLeader = isLeader;
        this.isLeader = true;
        if (!wasLeader) {
            logger.info("Node({}) became a leader", nodeId);
            observers.forEach(LeadershipObserver::leadershipAcquired);
        }
    }

    private void notALeader() {
        boolean wasLeader = isLeader;
        this.isLeader = false;
        if (wasLeader) {
            logger.info("Node({}) is no longer a leader.", nodeId);
            observers.forEach(LeadershipObserver::leadershipLost);
        }
    }

    private String lockEndpoint(String serviceName) {
        return "/v1/kv/service/" + serviceName + "/leader";
    }

    private HttpUrl lockUrl(String serviceName) {
        return baseUrl.newBuilder(lockEndpoint(serviceName)).build();
    }

    private class LockAcquirer implements Closeable {
        private final HttpUrl lockUrl;

        private LockAcquirer(HttpUrl lockUrl) {
            this.lockUrl = lockUrl;
        }

        private void start() {
            acquirementPool.scheduleAtFixedRate(this::acquireLock,
                    0, lockRescueDelaySeconds, TimeUnit.SECONDS);
        }

        private void leaderNodeUpdate(WatchResult<String> watchResult) {
            try {
                boolean shouldAcquireLock = false;

                String nodeBody = watchResult.getBody();
                List<Map<String, Object>> nodeValue = jsonDeserializer.deserializeMapList(nodeBody);

                if (nodeValue == null || nodeValue.size() < 1) {
                    logger.warn("Empty leader node value");
                    shouldAcquireLock = true;
                } else {
                    Map<String, Object> leaderInfo = nodeValue.get(0);

                    String currentLeaderSession = (String) leaderInfo.get("Session");
                    String currentLeaderValue = (String) leaderInfo.get("Value");
                    String currentLeader =
                            Optional.ofNullable(currentLeaderValue)
                                    .map(leader -> new String(
                                            Base64.getMimeDecoder().decode(leader),
                                            Charset.forName("UTF-8")
                                    ))
                                    .orElse("");

                    logger.debug("Leader session changed to {}", currentLeaderSession);

                    if (!nodeId.equals(currentLeader)) {
                        logger.info("This node({}) is not a leader. Current leader is {}.",
                                nodeId, currentLeader);
                        notALeader();
                    }
                    if (currentLeaderSession == null || currentLeaderSession.equals("")) {
                        shouldAcquireLock = true;
                    }
                }

                if (shouldAcquireLock) {
                    acquirementPool.schedule(this::acquireLock, lockDelaySeconds, TimeUnit.SECONDS);
                }
            } catch (IOException e) {
                logger.error("Couldn't deserialize lock body", e);
            } catch (Exception e) {
                logger.error("Unexpected issue on leader node update", e);
            }
        }

        private void acquireLock() {
            try {
                String body = nodeId;

                Request request = new Request.Builder()
                        .url(lockUrl.newBuilder().addQueryParameter("acquire", session.currentId()).build())
                        .put(RequestBody.create(JSON_MEDIA_TYPE, body))
                        .build();

                httpClient.newCall(request)
                        .enqueue(new LockAcquisitionCallback(LeaderElector.this::becameLeader, acquirementPool));
            } catch (Exception e) {
                logger.error("Couldn't acquire lock", e);
            }
        }

        @Override
        public void close() {
            acquirementPool.shutdown();
            try {
                acquirementPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class LockAcquisitionCallback implements Callback {
        private static final Logger logger = LoggerFactory.getLogger(LockAcquisitionCallback.class);

        private final Runnable becameLeaderCallback;
        private final ExecutorService lockAcquisitionPool;

        private LockAcquisitionCallback(Runnable becameLeaderCallback, ExecutorService lockAcquisitionPool) {
            this.becameLeaderCallback = becameLeaderCallback;
            this.lockAcquisitionPool = lockAcquisitionPool;
        }

        @Override
        public void onFailure(Call call, IOException e) {
            logger.error("Failed HTTP call on lock attempt", e);
        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {
            if (!response.isSuccessful()) {
                logger.warn("Unsuccessful HTTP response on lock attempt. Code: {}; Body: {}", response.code(),
                        readBodyOrFallback(response, "(couldn't parse)"));
                return;
            }
            try (ResponseBody body = response.body()) {
                String result = body.string();
                if ("true".equals(result.toLowerCase().trim())) {
                    lockAcquisitionPool.submit(becameLeaderCallback::run);
                }
            }
        }
    }

    public static class Builder {
        private final String serviceName;
        private final OkHttpClient httpClient;
        private final JsonSerializer jsonSerializer;
        private final JsonDeserializer jsonDeserializer;

        private ConsulWatcher consulWatcher = null;
        private ScheduledExecutorService lockAcquirementPool = null;
        private Session session = null;
        private URI agentUri = URI.create("http://localhost:8500");
        private String nodeId = UUID.randomUUID().toString();
        private int lockDelaySeconds = 16;
        private int lockRescueDelaySeconds = (int) Duration.ofMinutes(5).getSeconds();

        private Builder(String serviceName,
                        OkHttpClient httpClient,
                        JsonSerializer jsonSerializer,
                        JsonDeserializer jsonDeserializer) {
            this.serviceName = serviceName;
            this.httpClient = httpClient;
            this.jsonSerializer = jsonSerializer;
            this.jsonDeserializer = jsonDeserializer;
        }

        public LeaderElector build() {
            if (this.consulWatcher == null) {
                ExecutorService workerPool = Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder("consul-recipes-leader-watch-%d").build());
                this.consulWatcher = ConsulRecipes.consulRecipes()
                        .withAgentUri(agentUri)
                        .withJsonSerializer(jsonSerializer)
                        .withJsonDeserializer(jsonDeserializer)
                        .build()
                        .consulWatcher(workerPool)
                        .build();
            }

            if (this.lockAcquirementPool == null) {
                this.lockAcquirementPool = Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder("consul-recipes-leader-lock-%d").build());
            }

            if (this.session == null) {
                this.session = Session.forService(serviceName, httpClient, jsonSerializer, jsonDeserializer)
                        .withAgentUri(agentUri)
                        .build();
            }

            return new LeaderElector(
                    serviceName,
                    nodeId,
                    agentUri,
                    httpClient,
                    session,
                    lockAcquirementPool,
                    consulWatcher,
                    jsonDeserializer,
                    lockDelaySeconds,
                    lockRescueDelaySeconds
            );
        }

        public Builder withAgentUri(URI agentUri) {
            this.agentUri = agentUri;
            return this;
        }

        public Builder withNodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder withConsulWatcher(ConsulWatcher consulWatcher) {
            this.consulWatcher = consulWatcher;
            return this;
        }

        public Builder withLockDelaySeconds(int seconds) {
            this.lockDelaySeconds = seconds;
            return this;
        }

        public Builder withLockRescueDelaySeconds(int seconds) {
            this.lockRescueDelaySeconds = seconds;
            return this;
        }

        public Builder withLockAcquirementPool(ScheduledExecutorService lockAcquirementPool) {
            this.lockAcquirementPool = lockAcquirementPool;
            return this;
        }

        public Builder withSession(Session session) {
            this.session = session;
            return this;
        }
    }
}
