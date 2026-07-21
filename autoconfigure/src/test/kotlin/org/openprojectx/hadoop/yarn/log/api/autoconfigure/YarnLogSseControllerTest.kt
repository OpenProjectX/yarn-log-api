package org.openprojectx.hadoop.yarn.log.api.autoconfigure

import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.slot
import org.openprojectx.hadoop.yarn.log.api.YarnLogEvent
import org.openprojectx.hadoop.yarn.log.api.YarnLogEventType
import org.openprojectx.hadoop.yarn.log.api.YarnLogSource
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamRequest
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.web.reactive.server.WebTestClient
import reactor.core.publisher.Flux
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@WebFluxTest(YarnLogSseController::class)
@Import(YarnLogApiProperties::class)
@ContextConfiguration(
    classes = [YarnLogSseControllerTest.TestApplication::class, YarnLogSseController::class],
)
class YarnLogSseControllerTest {
    @Autowired
    private lateinit var webTestClient: WebTestClient

    @MockkBean
    private lateinit var service: YarnLogStreamService

    @Test
    fun `binds named parameters and streams readable SSE events`() {
        val requestSlot = slot<YarnLogStreamRequest>()
        every { service.stream(capture(requestSlot)) } returns Flux.just(
            YarnLogEvent(
                type = YarnLogEventType.LOG,
                sequence = 1,
                applicationId = APPLICATION_ID,
                containerId = "container_123_0001_01_000001",
                logType = "stdout",
                source = YarnLogSource.NODE_MANAGER,
                offset = 12,
                encoding = "BASE64",
                data = "aGVsbG8K",
                text = "hello\\n",
            ),
            YarnLogEvent(
                type = YarnLogEventType.COMPLETE,
                sequence = 2,
                applicationId = APPLICATION_ID,
                state = "FINISHED",
            ),
        )

        val events = webTestClient.get()
            .uri { builder ->
                builder.path("/api/v1/yarn/applications/$APPLICATION_ID/logs")
                    .queryParam("follow", true)
                    .queryParam("logFiles", "stdout,stderr")
                    .build()
            }
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk
            .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
            .returnResult(String::class.java)
            .responseBody
            .collectList()
            .block()!!

        assertEquals(APPLICATION_ID, requestSlot.captured.applicationId)
        assertEquals(setOf("stdout", "stderr"), requestSlot.captured.logFiles)
        assertTrue(requestSlot.captured.follow)
        assertTrue(events.any { it.contains("hello\\\\n") })
    }

    private companion object {
        const val APPLICATION_ID = "application_123_0001"
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    class TestApplication
}
