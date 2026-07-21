package org.openprojectx.hadoop.yarn.log.api.autoconfigure

import org.openprojectx.hadoop.yarn.log.api.YarnApplicationAccessDeniedException
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationAttemptInfo
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationInfo
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationQueryService
import org.openprojectx.hadoop.yarn.log.api.YarnContainerInfo
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.security.Principal

@RestController
class YarnApplicationController(
    private val service: YarnApplicationQueryService,
) {
    @GetMapping("/api/v1/yarn/applications/{applicationId}")
    fun application(
        @PathVariable("applicationId") applicationId: String,
        principal: Principal?,
    ): Mono<YarnApplicationInfo> = service.application(principal?.name, applicationId).mapAccessDenied()

    @GetMapping("/api/v1/yarn/applications/{applicationId}/attempts")
    fun attempts(
        @PathVariable("applicationId") applicationId: String,
        principal: Principal?,
    ): Flux<YarnApplicationAttemptInfo> = service.attempts(principal?.name, applicationId).mapAccessDenied()

    @GetMapping("/api/v1/yarn/applications/{applicationId}/containers")
    fun containers(
        @PathVariable("applicationId") applicationId: String,
        principal: Principal?,
    ): Flux<YarnContainerInfo> = service.containers(principal?.name, applicationId).mapAccessDenied()

    private fun <T : Any> Mono<T>.mapAccessDenied(): Mono<T> = onErrorMap(YarnApplicationAccessDeniedException::class.java) {
        ResponseStatusException(HttpStatus.FORBIDDEN, it.message, it)
    }

    private fun <T : Any> Flux<T>.mapAccessDenied(): Flux<T> = onErrorMap(YarnApplicationAccessDeniedException::class.java) {
        ResponseStatusException(HttpStatus.FORBIDDEN, it.message, it)
    }
}
