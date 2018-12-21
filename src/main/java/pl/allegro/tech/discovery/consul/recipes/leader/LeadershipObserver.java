package pl.allegro.tech.discovery.consul.recipes.leader;

public interface LeadershipObserver {
    void leadershipAcquired();
    void leadershipLost();
}
