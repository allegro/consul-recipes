package pl.allegro.tech.discovery.consul.recipes.watch

import org.junit.ClassRule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pl.allegro.tech.discovery.consul.recipes.ConsulCluster
import pl.allegro.tech.discovery.consul.recipes.ConsulRecipes
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.Executors

class ConsulWatcherIntTest extends Specification {

    static Logger logger = LoggerFactory.getLogger(ConsulWatcherIntTest)

    @Shared
    @ClassRule
    ConsulCluster consulCluster = new ConsulCluster.Builder()
            .withNode("dc1", "node1-dc1")
            .build()

    private ConsulWatcher watcher = ConsulRecipes.consulRecipes()
            .withAgentUri(URI.create("http://localhost:${consulCluster.getHttpPort("dc1", "node1-dc1")}"))
            .build()
            .consulWatcher(Executors.newFixedThreadPool(1))
            .withBackoff(100, 1000)
            .build()

    def setup() {
       consulCluster.reset()
    }

    def "should watch services registration and deregistration"() {
        given: "services watcher"
        Deque<WatchResult> latestState = new ArrayDeque<>();
        watcher.watchEndpoint("/v1/catalog/services",
                { wr -> latestState.push(wr) },
                { logger.error("Error while watching", it) })

        expect: "watcher caught first state of services with one service - consul"
        new PollingConditions(timeout: 10).eventually {
            !latestState.empty
            latestState.head().body.contains("consul")
        }

        when: "new service is registered"
        def serviceId = consulCluster.registerHealthyServiceInstance("my-service", "dc1", "node1-dc1")

        then: "watcher caught state with new service instance"
        new PollingConditions(timeout: 10).eventually {
            latestState.head().body.contains("my-service")
        }

        when: "service is unregistered"
        consulCluster.deregisterService(serviceId, "dc1", "node1-dc1")

        then: "watcher caught new services state without previously registered service"
        new PollingConditions(timeout: 10).eventually {
            !latestState.head().body.contains("my-service")
        }
    }

}
