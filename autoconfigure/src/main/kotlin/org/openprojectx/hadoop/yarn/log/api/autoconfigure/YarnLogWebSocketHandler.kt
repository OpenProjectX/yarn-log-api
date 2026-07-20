package org.openprojectx.hadoop.yarn.log.api.autoconfigure

import tools.jackson.databind.ObjectMapper
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamRequest
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamService
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
                    service.stream(
                        YarnLogStreamRequest(
                            applicationId = requireNotNull(subscription.applicationId) { "applicationId is required" },
                            requester = principal.ifEmpty { null },
                            follow = subscription.follow,
                            logFiles = subscription.logFiles.toSet().ifEmpty { setOf("stdout", "stderr") },
                            containerIds = subscription.containers.toSet(),
                            tailBytes = subscription.tailBytes ?: properties.initialTailBytes.toBytes(),
                            pollInterval = interval,
                        ),
                    )
                }
            }
            .map { event -> session.textMessage(objectMapper.writeValueAsString(event)) }
        return session.send(outgoing)
    }

    class Subscription {
        var applicationId: String? = null
        var follow: Boolean = true
        var logFiles: List<String> = emptyList()
        var containers: List<String> = emptyList()
        var tailBytes: Long? = null
        var pollIntervalMs: Long? = null
    }
}
