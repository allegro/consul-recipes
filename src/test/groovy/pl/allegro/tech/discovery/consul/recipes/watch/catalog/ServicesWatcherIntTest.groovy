package pl.allegro.tech.discovery.consul.recipes.watch.catalog

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.ClassRule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pl.allegro.tech.discovery.consul.recipes.ConsulCluster
import pl.allegro.tech.discovery.consul.recipes.ConsulRecipes
import pl.allegro.tech.discovery.consul.recipes.json.JacksonJsonDeserializer
import pl.allegro.tech.discovery.consul.recipes.watch.EndpointWatcher
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.Executors

class ServicesWatcherIntTest extends Specification {

    static Logger logger = LoggerFactory.getLogger(ServicesWatcherIntTest)

    @Shared
    @ClassRule
    ConsulCluster consulCluster = new ConsulCluster.Builder()
            .withNode("dc1", "node1-dc1")
            .build()

    private ConsulRecipes recipes = ConsulRecipes.consulRecipes()
            .withAgentUri(URI.create("http://localhost:${consulCluster.getHttpPort("dc1", "node1-dc1")}"))
            .withJsonDeserializer(new JacksonJsonDeserializer(new ObjectMapper()))
            .build()

    private EndpointWatcher<Services> servicesWatcher = recipes.catalogServicesWatcher(
            recipes.consulWatcher(Executors.newFixedThreadPool(1))
                    .withBackoff(100, 1000)
                    .build())

    def "should watch services registration and deregistration"() {
        given: "services watcher"
        Deque<Services> latestState = new ArrayDeque<>()
        servicesWatcher.watch(
                { latestState.push(it.body) },
                { logger.error("Error while watching", it) })

        expect: "watcher caught first state of services with one service - consul"
        new PollingConditions(timeout: 10).eventually {
            !latestState.empty
            latestState.head().containsService("consul")
        }

        when: "new service is registered"
        def serviceId = consulCluster.registerHealthyServiceInstance("my-service", "dc1", "node1-dc1")

        then: "watcher caught state with new service instance"
        new PollingConditions(timeout: 10).eventually {
            latestState.head().containsService("consul")
            latestState.head().containsService("my-service")
        }

        when: "service is unregistered"
        consulCluster.deregisterService(serviceId, "dc1", "node1-dc1")

        then: "watcher caught new services state without previously registered service"
        new PollingConditions(timeout: 10).eventually {
            !latestState.head().containsService("my-service")
        }
    }

    def "should call exception handler on error"() {
        given:
        Exception exception = null

        when: "watch endpoint that catches exception"
        servicesWatcher.watch({}, { exception = it })

        and: "consul node is stopped"
        consulCluster.stopNode("dc1", "node1-dc1")

        then: "exception is caught"
        new PollingConditions(timeout: 10).eventually {
            exception != null
        }
    }
}
