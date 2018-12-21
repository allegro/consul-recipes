package pl.allegro.tech.discovery.consul.recipes.leader

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.ClassRule
import pl.allegro.tech.discovery.consul.recipes.ConsulCluster
import pl.allegro.tech.discovery.consul.recipes.ConsulRecipes
import pl.allegro.tech.discovery.consul.recipes.json.JacksonJsonDeserializer
import pl.allegro.tech.discovery.consul.recipes.json.JacksonJsonSerializer
import pl.allegro.tech.discovery.consul.recipes.session.Session
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.Executors

class LeaderElectorIntTest extends Specification {

    @ClassRule
    @Shared
    ConsulCluster cluster = new ConsulCluster.Builder()
            .withNode("dc1", "dc1-node1")
            .withNode("dc1", "dc1-node2")
            .build()

    def "should choose a leader"() {
        given: "two leader electors"
        LeaderElector leaderElector1 = createElector("dc1", "dc1-node1")
        LeaderElector leaderElector2 = createElector("dc1", "dc1-node2")

        expect: "only one is chosen as a leader"
        new PollingConditions(timeout: 10).eventually {
            leader(leaderElector1, leaderElector2).isPresent()
            followers(leaderElector1, leaderElector2).size() == 1
        }

        when: "leader disconnects"
        def currentLeader = leader(leaderElector1, leaderElector2).get()
        currentLeader.close()
        
        then: "current leader is no longer a leader"
        !currentLeader.leader

        and: "new leader is elected"
        new PollingConditions(timeout: 10).eventually {
            def newLeader = leader(leaderElector1, leaderElector2)
            newLeader.isPresent()
            newLeader.get() != currentLeader
        }
    }

    Optional<LeaderElector> leader(LeaderElector... electors) {
        List<LeaderElector> leaders = electors.findAll { it.leader }
        if (leaders.size() > 1) {
            throw new IllegalStateException("There are multiple leaders " + leaders)
        }
        return leaders.stream().findFirst()
    }

    List<LeaderElector> followers(LeaderElector... electors) {
        return electors.findAll { !it.leader }
    }

    LeaderElector createElector(String dc, String nodeName) {
        ConsulRecipes recipes = ConsulRecipes.consulRecipes()
                .withJsonDeserializer(new JacksonJsonDeserializer(new ObjectMapper()))
                .withJsonSerializer(new JacksonJsonSerializer(new ObjectMapper()))
                .withAgentUri(URI.create("http://localhost:${cluster.getHttpPort(dc, nodeName)}"))
                .build()

        Session session = recipes.session("my-service")
                .withLockDelaySeconds(0)
                .build();

        LeaderElector leaderElector = recipes.leaderElector("my-service")
                .withConsulWatcher(recipes.consulWatcher(Executors.newFixedThreadPool(1)).build())
                .withLockDelaySeconds(1)
                .withSession(session)
                .build()

        leaderElector.start()

        return leaderElector
    }
}
