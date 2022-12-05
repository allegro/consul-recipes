package pl.allegro.tech.discovery.consul.recipes.watch

import com.github.tomakehurst.wiremock.http.Fault
import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.github.tomakehurst.wiremock.stubbing.Scenario
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

    private static Logger logger = LoggerFactory.getLogger(ConsulWatcherTest)

    private static final String CONSUL_INDEX_HEADER = 'X-Consul-Index'
    private static final String QUERY_PARAM_WAIT = 'wait'
    private static final String QUERY_PARAM_INDEX = 'index'

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
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '123')
                        .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('123'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '126')
                        .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('126'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                        .withHeader(CONSUL_INDEX_HEADER, '126')
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
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '123')
                        .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('123'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '126')
                        .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('126'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '127')
                        .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('127'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                        .withHeader(CONSUL_INDEX_HEADER, '127')
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
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '123')
                        .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('123'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '126')
                        .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('126'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '126')
                        .withBody('index did not change')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('127'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                        .withHeader(CONSUL_INDEX_HEADER, '127')
                        .withBody('finalize')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({ consumedMessages == ['123', '126'] })
    }

    def "should discard message and reset index if it went backwards"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .inScenario("index_backwards")
                .whenScenarioStateIs(Scenario.STARTED)
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '123')
                        .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .inScenario("index backwards")
                .whenScenarioStateIs(Scenario.STARTED)
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('123'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '120')
                        .withBody('120'))
                .willSetStateTo('after rewind'))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .inScenario('index backwards')
                .whenScenarioStateIs('after rewind')
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '126')
                        .withBody('126')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .inScenario('index backwards')
                .whenScenarioStateIs('after rewind')
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('126'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                        .withHeader(CONSUL_INDEX_HEADER, '127')
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
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '123')
                        .withBody('123')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('123'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                        .withHeader(CONSUL_INDEX_HEADER, '126')
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
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withBody('error')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '127')
                        .withBody('success')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('127'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse().withFixedDelay(10000)
                        .withHeader(CONSUL_INDEX_HEADER, '127')
                        .withBody('success')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({ consumedMessages == ['success'] })
    }

    def "should reset index on failure"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .inScenario("reset_index")
                .whenScenarioStateIs(Scenario.STARTED)
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '1')
                        .withBody('before-error')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .inScenario("reset_index")
                .whenScenarioStateIs(Scenario.STARTED)
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('1'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withFixedDelay(1000)
                        .withFault(Fault.CONNECTION_RESET_BY_PEER))
                .willSetStateTo("AFTER_ERROR")
        )


        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .inScenario("reset_index")
                .whenScenarioStateIs("AFTER_ERROR")
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '2')
                        .withBody('after-error')))

        def consumedMessages = []
        def consumer = { consumedMessages += it.body }

        when:
        watcher.watchEndpoint('/endpoint', consumer, { logger.error("Error while watching", it) })

        then:
        await().atMost(Duration.FIVE_SECONDS).until({
            consumedMessages == ['before-error', 'after-error']
        })
    }

    def "should reconnect after receiving garbage"() {
        given:
        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(aResponse()
                        .withFault(Fault.RANDOM_DATA_THEN_CLOSE)))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '1')
                        .withBody('success')))

        consul.stubFor(get(urlPathEqualTo('/endpoint'))
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('1'))
                .willReturn(aResponse().withFixedDelay(10000)
                        .withHeader(CONSUL_INDEX_HEADER, '2')
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
                .withQueryParam(QUERY_PARAM_INDEX, equalTo('0'))
                .withQueryParam(QUERY_PARAM_WAIT, equalTo('5m'))
                .willReturn(
                        aResponse()
                                .withStatus(errorCode)
                                .withHeader(CONSUL_INDEX_HEADER, '0')
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
        def counter = 0

        consul.stubFor(get(urlPathEqualTo('/cancel'))
                .willReturn(aResponse()
                        .withHeader(CONSUL_INDEX_HEADER, '123')
                        .withFixedDelay(100)
                        .withBody('123')))

        when:
        def canceller = watcher
                .watchEndpoint("/cancel", { it -> counter += 1}, { it -> counter += 1 })

        def beforeCancel = counter
        canceller.cancel()

        then:
        await().pollDelay(Duration.FIVE_SECONDS).until({
            canceller.isCancelled()
            beforeCancel == counter
        })
    }

}
