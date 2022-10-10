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

class ServiceInstancesWatcherIntTest extends Specification {

    static Logger logger = LoggerFactory.getLogger(ServiceInstancesWatcherIntTest)

    @Shared
    @ClassRule
    ConsulCluster consulCluster = new ConsulCluster.Builder()
            .withNode("dc1", "node1-dc1")
            .build()

    private ConsulRecipes recipes = ConsulRecipes.consulRecipes()
            .withAgentUri(URI.create("http://localhost:${consulCluster.getHttpPort("dc1", "node1-dc1")}"))
            .withJsonDeserializer(new JacksonJsonDeserializer(new ObjectMapper()))
            .build()

    private EndpointWatcher<ServiceInstances> serviceInstancesWatcher = recipes.catalogServiceInstancesWatcher("my-service",
            recipes.consulWatcher(Executors.newFixedThreadPool(1))
                    .withBackoff(100, 1000)
                    .build())

    def "should watch service details"() {
        given: "watcher on my-service details"
        Deque<ServiceInstances> latestState = new ArrayDeque<>()
        serviceInstancesWatcher.watch(
                { latestState.push(it.body) },
                { logger.error("Error while watching", it) })

        expect: "watcher caught first empty state of service details"
        new PollingConditions(timeout: 10).eventually {
            !latestState.empty
            latestState.head().instances.empty
        }

        when: "service is registered"
        def serviceId = consulCluster.registerHealthyServiceInstance("my-service", "dc1", "node1-dc1", ["tag1", "tag2"])

        then: "watcher caught new state with new registered service instance"
        new PollingConditions(timeout: 10).eventually {
            !latestState.head().instances.empty
            def instance = latestState.head().instances.first()
            instance.serviceId != null
            instance.serviceAddress.get() == "localhost"
            instance.servicePort.get() == 1234
            instance.serviceTags == ["tag1", "tag2"]
        }

        when: "service is unregistered"
        consulCluster.deregisterService(serviceId, "dc1", "node1-dc1")

        then: "watcher caught new state with removed service"
        new PollingConditions(timeout: 10).eventually {
            latestState.head().instances.empty
        }
    }

    def "should call exception handler on error"() {
        given:
        Exception exception = null

        when: "watch endpoint that catches exception"
        serviceInstancesWatcher.watch( {}, { exception = it })

        and: "consul node is stopped"
        consulCluster.stopNode("dc1", "node1-dc1")

        then: "exception is caught"
        new PollingConditions(timeout: 10).eventually {
            exception != null
        }
    }
}
