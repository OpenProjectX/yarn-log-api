package org.openprojectx.hadoop.yarn.log.api.engine

import org.openprojectx.hadoop.yarn.log.api.YarnApplicationAccessDeniedException
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationAttemptInfo
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationInfo
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationQueryService
import org.openprojectx.hadoop.yarn.log.api.YarnContainerInfo
import org.openprojectx.hadoop.yarn.log.api.YarnLogAuthorizer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class DefaultYarnApplicationQueryService(
    private val yarnGateway: HadoopYarnGateway,
    private val authorizer: YarnLogAuthorizer,
) : YarnApplicationQueryService {
    override fun application(requester: String?, applicationId: String): Mono<YarnApplicationInfo> =
        authorizedApplication(requester, applicationId)
            .doOnSubscribe { logQuery("application", applicationId, requester) }
            .doOnError { error -> logFailure("application", applicationId, requester, error) }

    override fun attempts(requester: String?, applicationId: String): Flux<YarnApplicationAttemptInfo> =
        authorizedApplication(requester, applicationId)
            .flatMapMany { yarnGateway.attempts(applicationId) }
            .doOnSubscribe { logQuery("attempts", applicationId, requester) }
            .doOnError { error -> logFailure("attempts", applicationId, requester, error) }

    override fun containers(requester: String?, applicationId: String): Flux<YarnContainerInfo> =
        authorizedApplication(requester, applicationId)
            .flatMapMany { yarnGateway.containers(applicationId) }
            .doOnSubscribe { logQuery("containers", applicationId, requester) }
            .doOnError { error -> logFailure("containers", applicationId, requester, error) }

    private fun authorizedApplication(requester: String?, applicationId: String): Mono<YarnApplicationInfo> =
        yarnGateway.application(applicationId).flatMap { application ->
            authorizer.authorize(requester, application.applicationId, application.user)
                .flatMap { allowed ->
                    if (allowed) Mono.just(application)
                    else Mono.error(YarnApplicationAccessDeniedException(applicationId))
                }
        }

    private fun logQuery(operation: String, applicationId: String, requester: String?) {
        logger.info(
            "Querying YARN application: operation={}, applicationId={}, requester={}",
            operation,
            applicationId,
            requester ?: "anonymous",
        )
    }

    private fun logFailure(operation: String, applicationId: String, requester: String?, error: Throwable) {
        logger.error(
            "YARN application query failed: operation={}, applicationId={}, requester={}",
            operation,
            applicationId,
            requester ?: "anonymous",
            error,
        )
    }

    private companion object {
        val logger = LoggerFactory.getLogger(DefaultYarnApplicationQueryService::class.java)
    }
}
