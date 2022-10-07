package pl.allegro.tech.discovery.consul.recipes.watch.health

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.ClassRule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pl.allegro.tech.discovery.consul.recipes.ConsulCluster
import pl.allegro.tech.discovery.consul.recipes.ConsulRecipes
import pl.allegro.tech.discovery.consul.recipes.json.JacksonJsonDeserializer
import pl.allegro.tech.discovery.consul.recipes.watch.EndpointWatcher
import pl.allegro.tech.discovery.consul.recipes.watch.catalog.ServiceInstances
import pl.allegro.tech.discovery.consul.recipes.watch.catalog.Services
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.Executors


class HealthServiceInstancesWatcherIntTest extends Specification {

    static Logger logger = LoggerFactory.getLogger(HealthServiceInstancesWatcherIntTest)
    public static final String GIVEN_DC = "dc1"
    public static final String GIVEN_NODE = "node1-dc1"

    @Shared
    @ClassRule
    ConsulCluster consulCluster = new ConsulCluster.Builder()
            .withNode(GIVEN_DC, GIVEN_NODE)
            .build()

    private ConsulRecipes recipes = ConsulRecipes.consulRecipes()
            .withAgentUri(URI.create("http://localhost:${consulCluster.getHttpPort(GIVEN_DC, GIVEN_NODE)}"))
            .withJsonDeserializer(new JacksonJsonDeserializer(new ObjectMapper()))
            .build()

    private EndpointWatcher<Services> healthServiceInstancesWatcher = recipes.healthServiceInstancesWatcher("my-service",
            recipes.consulWatcher(Executors.newFixedThreadPool(1))
                    .withBackoff(100, 1000)
                    .build()) as EndpointWatcher<Services>

    def cleanup() {
        consulCluster.deregisterAllServices(GIVEN_DC, GIVEN_NODE)
    }

    def "should watch only healthy services"() {
        given: "watcher on my-service details"
        Deque<ServiceInstances> latestState = new ArrayDeque<>()
        healthServiceInstancesWatcher.watch(
                { latestState.push(it.body as ServiceInstances) },
                { logger.error("Error while watching", it) })

        expect: "watcher caught first empty state of service details"
        new PollingConditions(timeout: 10).eventually {
            !latestState.empty
            latestState.head().instances.empty
        }

        when: "unhealthy service instance is registered"
        consulCluster.registerUnhealthyServiceInstance("my-service", GIVEN_DC, GIVEN_NODE)

        and: "healthy service instance is registered"
        consulCluster.registerHealthyServiceInstance("my-service", GIVEN_DC, GIVEN_NODE, ["tag1", "tag2"])

        then: "watcher caught new state with only healthy service instance"
        new PollingConditions(timeout: 10).eventually {
            latestState.head().instances.size() == 1
            def instance = latestState.head().instances.first()
            instance.serviceId != null
            instance.serviceAddress == "localhost"
            instance.servicePort == 1234
            instance.serviceTags == ["tag1", "tag2"]
        }
    }

    def "should watch only parsable instances"() {
        given: "watcher on my-service details"
        Deque<ServiceInstances> latestState = new ArrayDeque<>()
        healthServiceInstancesWatcher.watch(
                { latestState.push(it.body as ServiceInstances) },
                { logger.error("Error while watching", it) })

        expect: "watcher caught first empty state of service details"
        new PollingConditions(timeout: 10).eventually {
            !latestState.empty
            latestState.head().instances.empty
        }

        when: "unhealthy service instance is registered"
        consulCluster.registerInstanceLackingPortNumber("my-service", GIVEN_DC, GIVEN_NODE)

        and: "healthy service instance is registered"
        consulCluster.registerHealthyServiceInstance("my-service", GIVEN_DC, GIVEN_NODE, ["tag1", "tag2"])

        then: "watcher caught new state with only healthy service instance"
        new PollingConditions(timeout: 10).eventually {
            latestState.head().instances.size() == 1
            def instance = latestState.head().instances.first()
            instance.serviceId != null
            instance.serviceAddress == "localhost"
            instance.servicePort == 1234
            instance.serviceTags == ["tag1", "tag2"]
        }
    }
}
