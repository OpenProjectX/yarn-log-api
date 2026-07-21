package org.openprojectx.hadoop.yarn.log.api.autoconfigure

import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationAccessDeniedException
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationAttemptInfo
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationInfo
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationQueryService
import org.openprojectx.hadoop.yarn.log.api.YarnContainerInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.web.reactive.server.WebTestClient
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.test.Test

@WebFluxTest(YarnApplicationController::class)
@ContextConfiguration(
    classes = [YarnApplicationControllerTest.TestApplication::class, YarnApplicationController::class],
)
class YarnApplicationControllerTest {
    @Autowired
    private lateinit var webTestClient: WebTestClient

    @MockkBean
    private lateinit var service: YarnApplicationQueryService

    @Test
    fun `returns application attempts and containers as API models`() {
        every { service.application(null, APPLICATION_ID) } returns Mono.just(application())
        every { service.attempts(null, APPLICATION_ID) } returns Flux.just(attempt())
        every { service.containers(null, APPLICATION_ID) } returns Flux.just(container())

        webTestClient.get().uri("/api/v1/yarn/applications/$APPLICATION_ID")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.applicationId").isEqualTo(APPLICATION_ID)
            .jsonPath("$.state").isEqualTo("RUNNING")
            .jsonPath("$.user").isEqualTo("alice")

        webTestClient.get().uri("/api/v1/yarn/applications/$APPLICATION_ID/attempts")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$[0].attemptId").isEqualTo(ATTEMPT_ID)

        webTestClient.get().uri("/api/v1/yarn/applications/$APPLICATION_ID/containers")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$[0].containerId").isEqualTo(CONTAINER_ID)
            .jsonPath("$[0].memoryMb").isEqualTo(1024)
    }

    @Test
    fun `maps authorization denial to forbidden`() {
        every { service.application(null, APPLICATION_ID) } returns
            Mono.error(YarnApplicationAccessDeniedException(APPLICATION_ID))

        webTestClient.get().uri("/api/v1/yarn/applications/$APPLICATION_ID")
            .exchange()
            .expectStatus().isForbidden
    }

    private fun application() = YarnApplicationInfo(
        applicationId = APPLICATION_ID,
        currentAttemptId = ATTEMPT_ID,
        name = "example",
        applicationType = "SPARK",
        user = "alice",
        queue = "default",
        state = "RUNNING",
        finalStatus = "UNDEFINED",
        progress = 0.5f,
        trackingUrl = "http://rm/proxy/$APPLICATION_ID",
        originalTrackingUrl = null,
        diagnostics = null,
        submitTime = 1,
        startTime = 2,
        launchTime = 3,
        finishTime = 0,
        logAggregationStatus = "RUNNING_WITH_FAILURE",
        tags = setOf("team=data"),
    )

    private fun attempt() = YarnApplicationAttemptInfo(
        attemptId = ATTEMPT_ID,
        state = "RUNNING",
        amContainerId = CONTAINER_ID,
        host = "worker.example.com",
        rpcPort = 8042,
        trackingUrl = null,
        originalTrackingUrl = null,
        diagnostics = null,
        startTime = 2,
        finishTime = 0,
    )

    private fun container() = YarnContainerInfo(
        containerId = CONTAINER_ID,
        attemptId = ATTEMPT_ID,
        state = "RUNNING",
        nodeId = "worker.example.com:45454",
        nodeHttpAddress = "worker.example.com:8042",
        logUrl = null,
        diagnostics = null,
        exitStatus = -1000,
        memoryMb = 1024,
        virtualCores = 1,
        creationTime = 3,
        finishTime = 0,
    )

    private companion object {
        const val APPLICATION_ID = "application_123_0001"
        const val ATTEMPT_ID = "appattempt_123_0001_000001"
        const val CONTAINER_ID = "container_123_0001_01_000001"
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    class TestApplication
}
