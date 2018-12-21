package pl.allegro.tech.discovery.consul.recipes.locate

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.ClassRule
import pl.allegro.tech.discovery.consul.recipes.ConsulCluster
import pl.allegro.tech.discovery.consul.recipes.ConsulRecipes
import pl.allegro.tech.discovery.consul.recipes.json.JacksonJsonDeserializer
import spock.lang.Shared
import spock.lang.Specification

class ConsulAgentLocatorIntTest extends Specification {

    @Shared
    @ClassRule
    ConsulCluster consulCluster = new ConsulCluster.Builder()
            .withNode("dc1", "node1-dc1")
            .withNode("dc2", "node1-dc2")
            .withNode("dc2", "node2-dc2")
            .build()

    URI localAgentUri = URI.create("http://localhost:${consulCluster.getHttpPort("dc1", "node1-dc1")}/")

    private ConsulAgentLocator locator = ConsulRecipes.consulRecipes()
            .withJsonDeserializer(new JacksonJsonDeserializer(new ObjectMapper()))
            .withAgentUri(localAgentUri)
            .build()
            .consulAgentLocator()
            .withAgentPort(1234)
            .build()

    def setup() {
        consulCluster.reset()
    }

    def "should return agent URIs from all datacenters"() {
        given: "healthy service in each dc"
        consulCluster.registerHealthyServiceInstance("my-service", "dc1", "node1-dc1")
        consulCluster.registerHealthyServiceInstance("my-service", "dc2", "node1-dc2")

        when: "located agents for service"
        Map<String, ConsulAgentLocator.AgentData> agents = locator.locateDatacenterAgents('my-service')

        then: "there is a local agent with localhost name"
        agents['dc1'].uri == localAgentUri
        // the name for local agent is always "localhost"
        agents['dc1'].name == 'localhost'

        and: "there is a remote agent"
        // there is no info of remote agent's port so we deduce it by convention passed to
        // .withAgentPort() to ConsulAgentLocator
        agents['dc2'].uri == URI.create("http://127.0.0.1:1234")
        agents['dc2'].name == 'node1-dc2'
    }

    def "should use custom strategy to choose preferred agent"() {
        given: "healthy service in each dc"
        consulCluster.registerHealthyServiceInstance("my-service", "dc2", "node1-dc2")
        consulCluster.registerHealthyServiceInstance("my-service", "dc2", "node2-dc2")

        when: "located agents for services that nodes starts with node1"
        Map<String, ConsulAgentLocator.AgentData> agents = locator.locateDatacenterAgents(
                'my-service',
                { data -> data.find({
                    i -> i['Node']['Node'].startsWith('node1')
                }) }
        )

        then: "locator chosen agent with agent1 node name"
        agents['dc2'].name == 'node1-dc2'
    }

    def "should skip datacenter when no healthy instance of service found"() {
        given: "healthy service in dc1 and unhealthy in dc2"
        consulCluster.registerHealthyServiceInstance("my-service", "dc1", "node1-dc1")
        consulCluster.registerUnhealthyServiceInstance("my-service", "dc2", "node1-dc2")

        when: "located agents for service"
        Map<String, ConsulAgentLocator.AgentData> agents = locator.locateDatacenterAgents('my-service')

        then: "there is agent in dc1 but agent for dc2 is skipped"
        agents['dc1'].uri == localAgentUri
        !agents['dc2']
    }

    def "should skip datacenter when remote datacenter is unhealthy"() {
        given: "non responding second dc"
        consulCluster.stopNode("dc2", "node1-dc2")

        when: "located agents for service"
        Map<String, ConsulAgentLocator.AgentData> agents = locator.locateDatacenterAgents('my-service')

        then: "there is an agent in dc1 but no agent in dc2"
        agents['dc1'].uri == localAgentUri
        !agents['dc2']
    }
}
