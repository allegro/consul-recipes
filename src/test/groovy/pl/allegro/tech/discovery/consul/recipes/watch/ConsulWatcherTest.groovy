package pl.allegro.tech.discovery.consul.recipes.watch

import com.github.tomakehurst.wiremock.http.Fault
import com.github.tomakehurst.wiremock.junit.WireMockRule
import org.awaitility.Duration
import org.junit.ClassRule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pl.allegro.tech.discovery.consul.recipes.ConsulRecipes
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.Executors

import static com.github.tomakehurst.wiremock.client.WireMock.*
import static org.awaitility.Awaitility.await

class ConsulWatcherTest extends Specification {

    static Logger logger = LoggerFactory.getLogger(ConsulWatcherTest)

    @ClassRule
    @Shared
    WireMockRule consul = new WireMockRule(0)

    private ConsulWatcher watcher = ConsulRecipes.consulRecipes()
            .withAgentUri(URI.create("http://localhost:${consul.port()}"))
            .build()
            .consulWatcher(Executors.newFixedThreadPool(1))
            .withBackoff(100, 1000)
            .build()

    def cleanup() {
        watcher.close()
        consul.resetAll()
    }

    def "should react to consecutive changes on given endpoint"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('0'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '123')
                .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('123'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '126')
                .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('126'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                .withHeader('X-Consul-Index', '126')
                .withBody('finalize')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({ consumedMessages == ['123', '126'] })
    }

    def "should not run callback if the content did not change"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('0'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '123')
                .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('123'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '126')
                .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('126'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '127')
                .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('127'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                .withHeader('X-Consul-Index', '127')
                .withBody('finalize')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({ consumedMessages == ['123', '126'] })
    }

    def "should not call callback if X-Consul-Index did not change"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('0'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '123')
                .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('123'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '126')
                .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('126'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '126')
                .withBody('index did not change')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('127'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                .withHeader('X-Consul-Index', '127')
                .withBody('finalize')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({ consumedMessages == ['123', '126'] })
    }

    def "should accept endpoints with query parameters"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam("someParam", equalTo("something"))
                .withQueryParam('index', equalTo('0'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '123')
                .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('123'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                .withHeader('X-Consul-Index', '126')
                .withBody('finalize')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint?someParam=something', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({ consumedMessages == ['123'] })
    }

    def "should reconnect on failure to fetch X-Consul-Index header"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('0'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withBody('error')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('0'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '127')
                .withBody('success')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('127'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                .withHeader('X-Consul-Index', '127')
                .withBody('success')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({ consumedMessages == ['success'] })
    }

    def "should reconnect after receiving garbage"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('0'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(aResponse()
                .withFault(Fault.RANDOM_DATA_THEN_CLOSE)))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('0'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '1')
                .withBody('success')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('1'))
                .willReturn(aResponse().withFixedDelay(10000)
                .withHeader('X-Consul-Index', '2')
                .withBody('success')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({ consumedMessages == ['success'] })
    }

    @Unroll
    def "should reconnect with delay after receiving #errorCode http code"() {
        given: "error code is returned"
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam('index', equalTo('0'))
                .withQueryParam('wait', equalTo('5m'))
                .willReturn(
                    aResponse()
                            .withStatus(errorCode)
                            .withHeader('X-Consul-Index', '0')
                            .withBody("Access denied")
                )
        )

        def consumedMessages = []
        def consumer = { consumedMessages += it }

        when: "watching started"
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then: "assuming 100ms exponentatial backoff, expect around 5 requests matched after 1500ms"
        await().between(Duration.ONE_SECOND, Duration.TWO_SECONDS).until({
            def hits = consul.countRequestsMatching(getRequestedFor(urlPathEqualTo('/endpoint')).build()).count
            5 <= hits && hits < 10
        })

        where:
        errorCode << [403, 500]
    }

    def "should not invoke callbacks for disposed watch"() {
        given:
        def first = 0
        def second = 0

        consul.stubFor(get(urlPathEqualTo('/cancel'))
                .willReturn(aResponse()
                .withHeader('X-Consul-Index', '123')
                .withFixedDelay(100)
                .withBody('123')))

        when:
        def canceller = watcher
                .watchEndpoint("/cancel", { it -> first += 1 }, { it -> first += 1 })

        def canceller2 = watcher
                .watchEndpoint("/cancel", { it -> second += 1}, { it -> second += 1})

        canceller.cancel()

        then:
        await().until({
            verify(exactly(2), getRequestedFor(urlPathEqualTo("/cancel")))
            canceller2.cancel()
            true
        })

        first == 0
        second == 1
    }

}
