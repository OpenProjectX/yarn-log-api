package org.openprojectx.hadoop.yarn.log.api.autoconfigure

import org.openprojectx.hadoop.yarn.log.api.YarnLogEvent
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamRequest
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamService
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import java.security.Principal
import java.time.Duration

@RestController
class YarnLogSseController(
    private val service: YarnLogStreamService,
    private val properties: YarnLogApiProperties,
) {
    @GetMapping(
        value = ["/api/v1/yarn/applications/{applicationId}/logs"],
        produces = [MediaType.TEXT_EVENT_STREAM_VALUE],
    )
    fun stream(
        @PathVariable applicationId: String,
        @RequestParam(defaultValue = "true") follow: Boolean,
        @RequestParam(required = false) logFiles: Set<String>?,
        @RequestParam(required = false) containers: Set<String>?,
        @RequestParam(required = false) tailBytes: Long?,
        @RequestParam(required = false) pollIntervalMs: Long?,
        principal: Principal?,
    ): Flux<ServerSentEvent<YarnLogEvent>> {
        val interval = pollIntervalMs?.let(Duration::ofMillis) ?: properties.pollInterval
        require(interval >= properties.minimumPollInterval) {
            "pollInterval must be at least ${properties.minimumPollInterval}"
        }
        val request = YarnLogStreamRequest(
            applicationId = applicationId,
            requester = principal?.name,
            follow = follow,
            logFiles = logFiles?.filter { it.isNotBlank() }?.toSet().orEmpty()
                .ifEmpty { setOf("stdout", "stderr") },
            containerIds = containers?.filter { it.isNotBlank() }?.toSet().orEmpty(),
            tailBytes = tailBytes ?: properties.initialTailBytes.toBytes(),
            pollInterval = interval,
        )
        return service.stream(request).map { event ->
            ServerSentEvent.builder(event)
                .id(event.sequence.toString())
                .event(event.type.name.lowercase())
                .build()
        }
    }
}
