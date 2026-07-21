package org.openprojectx.hadoop.yarn.log.api.autoconfigure

import tools.jackson.databind.ObjectMapper
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamRequest
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamService
import org.slf4j.LoggerFactory
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import java.time.Duration

class YarnLogWebSocketHandler(
    private val service: YarnLogStreamService,
    private val objectMapper: ObjectMapper,
    private val properties: YarnLogApiProperties,
) : WebSocketHandler {
    override fun handle(session: WebSocketSession): Mono<Void> {
        val requester = session.handshakeInfo.principal.map { it.name }.defaultIfEmpty("")
        val outgoing = session.receive()
            .map { objectMapper.readValue(it.payloadAsText, Subscription::class.java) }
            .switchMap { subscription ->
                requester.flatMapMany { principal ->
                    val interval = Duration.ofMillis(subscription.pollIntervalMs ?: properties.pollInterval.toMillis())
                    require(interval >= properties.minimumPollInterval) {
                        "pollInterval must be at least ${properties.minimumPollInterval}"
                    }
                    val request = YarnLogStreamRequest(
                        applicationId = requireNotNull(subscription.applicationId) { "applicationId is required" },
                        requester = principal.ifEmpty { null },
                        follow = subscription.follow,
                        logFiles = subscription.logFiles.toSet().ifEmpty { setOf("stdout", "stderr") },
                        containerIds = subscription.containers.toSet(),
                        tailBytes = subscription.tailBytes ?: properties.initialTailBytes.toBytes(),
                        pollInterval = interval,
                    )
                    logger.info(
                        "Opening YARN log WebSocket stream: sessionId={}, applicationId={}, requester={}, " +
                            "follow={}, logFiles={}, containers={}, tailBytes={}, pollInterval={}",
                        session.id,
                        request.applicationId,
                        request.requester ?: "anonymous",
                        request.follow,
                        request.logFiles,
                        request.containerIds,
                        request.tailBytes,
                        request.pollInterval,
                    )
                    service.stream(request)
                }
            }
            .map { event -> session.textMessage(objectMapper.writeValueAsString(event)) }
        return session.send(outgoing)
            .doOnSuccess { logger.info("Completed YARN log WebSocket session: sessionId={}", session.id) }
            .doOnCancel { logger.info("Client cancelled YARN log WebSocket session: sessionId={}", session.id) }
            .doOnError { error -> logger.error("YARN log WebSocket session failed: sessionId={}", session.id, error) }
    }

    class Subscription {
        var applicationId: String? = null
        var follow: Boolean = true
        var logFiles: List<String> = emptyList()
        var containers: List<String> = emptyList()
        var tailBytes: Long? = null
        var pollIntervalMs: Long? = null
    }

    private companion object {
        val logger = LoggerFactory.getLogger(YarnLogWebSocketHandler::class.java)
    }
}
