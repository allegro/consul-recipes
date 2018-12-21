package pl.allegro.tech.discovery.consul.recipes.datacenter

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.ClassRule
import pl.allegro.tech.discovery.consul.recipes.ConsulCluster
import pl.allegro.tech.discovery.consul.recipes.ConsulRecipes
import pl.allegro.tech.discovery.consul.recipes.json.JacksonJsonDeserializer
import spock.lang.Shared
import spock.lang.Specification

class ConsulDatacenterReaderIntTest extends Specification {

    @ClassRule
    @Shared
    ConsulCluster cluster = new ConsulCluster.Builder()
            .withNode("dc1", "dc1-node1")
            .withNode("dc2", "dc2-node1")
            .build()

    @Shared
    ConsulDatacenterReader datacenterReader = ConsulRecipes.consulRecipes()
            .withJsonDeserializer(new JacksonJsonDeserializer(new ObjectMapper()))
            .withAgentUri(URI.create("http://localhost:${cluster.getHttpPort("dc1", "dc1-node1")}"))
            .build()
            .consulDatacenterReader().build()

    def "should return current consul datacenter"() {
        expect:
        datacenterReader.localDatacenter() == "dc1"
    }

    def "should return list of consul datacenters"() {
        expect:
        datacenterReader.knownDatacenters() == ['dc1', 'dc2']
    }

}
