package org.openprojectx.hadoop.yarn.log.api.engine

import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.scheduler.Schedulers
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

@WireMockTest
class NodeManagerLogClientWireMockTest {
    @Test
    fun `requests a tail window and parses the NodeManager response`(wireMock: WireMockRuntimeInfo) {
        val logBytes = "tail".encodeToByteArray()
        val response = buildString {
            appendLine("Container: container_123_0001_01_000001 on node:8042")
            appendLine("LogAggregationType: LOCAL")
            appendLine("LogType:stdout")
            appendLine("LogLength:10")
            appendLine("LogContents:")
        }.encodeToByteArray() + logBytes + "End of LogType:stdout\n".encodeToByteArray()
        wireMock.wireMock.register(
            get(urlPathEqualTo("/ws/v1/node/containers/container_123_0001_01_000001/logs/stdout"))
                .withQueryParam("size", equalTo("-4"))
                .willReturn(com.github.tomakehurst.wiremock.client.WireMock.ok().withBody(response)),
        )
        val scheduler = Schedulers.newSingle("wiremock-hadoop")

        try {
            val client = NodeManagerLogClient(
                webClient = WebClient.builder().build(),
                cookieProvider = HadoopSpnegoCookieProvider(scheduler),
            )

            val result = client.fetchTail(
                nodeHttpAddress = "localhost:${wireMock.httpPort}",
                containerId = "container_123_0001_01_000001",
                logType = "stdout",
                windowBytes = 4,
            ).block()!!

            assertEquals(10, result.fileLength)
            assertEquals(6, result.responseStartOffset)
            assertContentEquals(logBytes, result.bytes)
        } finally {
            scheduler.dispose()
        }
    }
}
