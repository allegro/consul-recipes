package pl.allegro.tech.discovery.consul.recipes

import com.ecwid.consul.v1.agent.AgentConsulClient
import com.ecwid.consul.v1.agent.model.NewService
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.Multimap
import com.pszymczyk.consul.ConsulPorts
import com.pszymczyk.consul.ConsulStarterBuilder
import com.pszymczyk.consul.infrastructure.Ports
import com.pszymczyk.consul.junit.ConsulResource
import groovy.transform.Canonical
import org.junit.rules.ExternalResource

class ConsulCluster extends ExternalResource {

    private final Map<NodeKey, ConsulResource> resources
    private final Map<NodeKey, ConsulPorts> portsForNodes

    private ConsulCluster(Map<NodeKey, ConsulResource> resources, Map<NodeKey, ConsulPorts> portsForNodes) {
        this.resources = resources
        this.portsForNodes = portsForNodes
    }

    int getHttpPort(String dc, String nodeName) {
        def resource = portsForNodes[new NodeKey(dc, nodeName)]
        if (resource == null) {
            throw new IllegalArgumentException("Node for $dc and $nodeName not found in Cluster")
        }
        return resource.httpPort
    }

    void reset() {
        resources.values().each { it.reset() }
    }

    void stopNode(String dc, String nodeName) {
        def resource = resources[new NodeKey(dc, nodeName)]
        if (resource == null) {
            throw new IllegalArgumentException("Node for $dc and $nodeName not found in Cluster")
        }
        resource.after()
    }

    String registerHealthyServiceInstance(String name, String dc, String nodeName, List<String> tags = []) {
        def client = new AgentConsulClient("localhost", getHttpPort(dc, nodeName))
        def newService = new NewService()
        newService.id = UUID.randomUUID().toString()
        newService.name = name
        newService.address = "localhost"
        newService.port = 1234
        newService.tags = tags
        client.agentServiceRegister(newService)

        return newService.id
    }

    String registerUnhealthyServiceInstance(String name, String dc, String nodeName) {
        def client = new AgentConsulClient("localhost", getHttpPort(dc, nodeName))
        def newService = new NewService()
        newService.id = UUID.randomUUID().toString()
        newService.name = name
        newService.address = "localhost"
        newService.port = 1234

        def check = new NewService.Check()
        check.http = "http://localhost:" + Ports.nextAvailable()
        check.interval = "1s"

        newService.check = check

        client.agentServiceRegister(newService)
        return newService.id
    }

    String registerInstanceLackingPortNumber(String name, String dc, String nodeName, List<String> tags = []) {
        def client = new AgentConsulClient("localhost", getHttpPort(dc, nodeName))
        def newService = new NewService()
        newService.id = UUID.randomUUID().toString()
        newService.name = name
        newService.address = "localhost"
        newService.tags = tags
        client.agentServiceRegister(newService)

        return newService.id
    }

    void deregisterService(String serviceId, String dc, String nodeName) {
        def client = new AgentConsulClient("localhost", getHttpPort(dc, nodeName))
        client.agentServiceDeregister(serviceId)
    }

    static class Builder {
        List<NodeKey> nodeKeys = []

        def withNode(String dc, String nodeName) {
            nodeKeys << new NodeKey(dc, nodeName)
            return this
        }

        def build() {
            Map<NodeKey, ConsulPorts> portsForNodes = nodeKeys.collectEntries {
                [(it): ConsulPorts.consulPorts()
                        .withHttpPort(Ports.nextAvailable())
                        .withSerfLanPort(Ports.nextAvailable())
                        .withSerfWanPort(Ports.nextAvailable())
                        .build()]
            }

            Map<NodeKey, ConsulResource> resources = nodeKeys.collectEntries { nodeKey ->
                [(nodeKey): new ConsulResource(ConsulStarterBuilder.consulStarter()
                        .withConsulPorts(portsForNodes[nodeKey])
                        .withConsulVersion("1.10.12")
                        .withCustomConfig("""
                        {
                            "node_name": "${nodeKey.name}",
                            "datacenter": "${nodeKey.dc}",
                            "retry_join": [${lanHosts(portsForNodes, nodeKey)}],
                            "retry_join_wan": [${wanHosts(portsForNodes, nodeKey)}]
                        }
                        """)
                        .build())]
            }
            new ConsulCluster(resources, portsForNodes)
        }

        private String wanHosts(Map<NodeKey, ConsulPorts> portsForNodes, NodeKey nodeKey) {
            List<ConsulPorts> portsForOneNodeOfEveryOtherDc = portsForNodes
                    .groupBy { otherNodeKey, _ -> otherNodeKey.dc }
                    .findAll { dc, _ -> dc != nodeKey.dc }
                    .collect { _, nodeKeyWithPorts -> nodeKeyWithPorts.values().first() }
            return portsForOneNodeOfEveryOtherDc
                    .collect { consulResource -> "\"localhost:${consulResource.serfWanPort}\"" }
                    .join(",")
        }

        private String lanHosts(Map<NodeKey, ConsulPorts> portsForNodes, NodeKey nodeKey) {
            Collection<ConsulPorts> portsForOtherNodesInSameDc = portsForNodes
                    .findAll { otherNodeKey, _ -> otherNodeKey.dc == nodeKey.dc && otherNodeKey != nodeKey }
                    .values()
            return portsForOtherNodesInSameDc
                    .collect { consulResource -> "\"localhost:${consulResource.serfLanPort}\"" }
                    .join(",")
        }
    }

    @Override
    protected void before() throws Throwable {
        resources.each { _, resource -> resource.before() }
    }

    @Override
    protected void after() {
        resources.each { _, resource -> resource.after() }
    }

    @Canonical
    private static class NodeKey {
        String dc
        String name
    }
}
