package org.openprojectx.hadoop.yarn.log.api.engine

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.client.api.YarnClient
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationAttemptInfo
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationInfo
import org.openprojectx.hadoop.yarn.log.api.YarnContainerInfo
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

class HadoopYarnGateway(
    private val yarnClient: YarnClient,
    private val hadoopScheduler: Scheduler,
) {
    fun application(applicationId: String): Mono<YarnApplicationInfo> =
        Mono.fromCallable {
            val report = yarnClient.getApplicationReport(ApplicationId.fromString(applicationId))
            YarnApplicationInfo(
                applicationId = report.applicationId.toString(),
                currentAttemptId = report.currentApplicationAttemptId?.toString(),
                name = report.name.orEmpty(),
                applicationType = report.applicationType.orEmpty(),
                user = report.user.orEmpty(),
                queue = report.queue.orEmpty(),
                state = report.yarnApplicationState?.name ?: "UNKNOWN",
                finalStatus = report.finalApplicationStatus?.name ?: "UNDEFINED",
                progress = report.progress,
                trackingUrl = report.trackingUrl?.takeIf(String::isNotBlank),
                originalTrackingUrl = report.originalTrackingUrl?.takeIf(String::isNotBlank),
                diagnostics = report.diagnostics?.takeIf(String::isNotBlank),
                submitTime = report.submitTime,
                startTime = report.startTime,
                launchTime = report.launchTime,
                finishTime = report.finishTime,
                logAggregationStatus = report.logAggregationStatus?.name,
                tags = report.applicationTags.orEmpty(),
            )
        }.subscribeOn(hadoopScheduler)

    fun attempts(applicationId: String): Flux<YarnApplicationAttemptInfo> =
        Mono.fromCallable {
            yarnClient.getApplicationAttempts(ApplicationId.fromString(applicationId)).map { attempt ->
                YarnApplicationAttemptInfo(
                    attemptId = attempt.applicationAttemptId.toString(),
                    state = attempt.yarnApplicationAttemptState?.name ?: "UNKNOWN",
                    amContainerId = attempt.amContainerId?.toString(),
                    host = attempt.host?.takeIf(String::isNotBlank),
                    rpcPort = attempt.rpcPort,
                    trackingUrl = attempt.trackingUrl?.takeIf(String::isNotBlank),
                    originalTrackingUrl = attempt.originalTrackingUrl?.takeIf(String::isNotBlank),
                    diagnostics = attempt.diagnostics?.takeIf(String::isNotBlank),
                    startTime = attempt.startTime,
                    finishTime = attempt.finishTime,
                )
            }
        }.subscribeOn(hadoopScheduler).flatMapMany { Flux.fromIterable(it) }

    fun containers(applicationId: String): Flux<YarnContainerInfo> =
        Mono.fromCallable {
            val appId = ApplicationId.fromString(applicationId)
            yarnClient.getApplicationAttempts(appId)
                .flatMap { attempt -> yarnClient.getContainers(attempt.applicationAttemptId) }
                .distinctBy { it.containerId }
                .map { container ->
                    YarnContainerInfo(
                        containerId = container.containerId.toString(),
                        attemptId = container.containerId.applicationAttemptId.toString(),
                        state = container.containerState?.name ?: "UNKNOWN",
                        nodeId = container.assignedNode?.toString(),
                        nodeHttpAddress = container.nodeHttpAddress?.takeIf(String::isNotBlank),
                        logUrl = container.logUrl?.takeIf(String::isNotBlank),
                        diagnostics = container.diagnosticsInfo?.takeIf(String::isNotBlank),
                        exitStatus = container.containerExitStatus,
                        memoryMb = container.allocatedResource?.memorySize,
                        virtualCores = container.allocatedResource?.virtualCores,
                        creationTime = container.creationTime,
                        finishTime = container.finishTime,
                    )
                }
        }.subscribeOn(hadoopScheduler).flatMapMany { Flux.fromIterable(it) }

    fun snapshot(applicationId: String): Mono<YarnApplicationSnapshot> =
        Mono.fromCallable {
            val appId = ApplicationId.fromString(applicationId)
            val report = yarnClient.getApplicationReport(appId)
            val containers = if (report.yarnApplicationState == null || isFinal(report.yarnApplicationState.name)) {
                emptyList()
            } else {
                yarnClient.getApplicationAttempts(appId)
                    .flatMap { yarnClient.getContainers(it.applicationAttemptId) }
                    .distinctBy { it.containerId }
                    .mapNotNull { container ->
                        val nodeId = container.assignedNode?.toString() ?: return@mapNotNull null
                        val httpAddress = container.nodeHttpAddress?.takeIf { it.isNotBlank() }
                            ?: return@mapNotNull null
                        YarnContainerSnapshot(
                            containerId = container.containerId.toString(),
                            nodeId = nodeId,
                            nodeHttpAddress = httpAddress,
                            state = container.containerState?.name ?: "UNKNOWN",
                        )
                    }
            }
            YarnApplicationSnapshot(
                applicationId = applicationId,
                owner = report.user,
                state = report.yarnApplicationState,
                containers = containers,
            )
        }.subscribeOn(hadoopScheduler)
            .doOnSubscribe { logger.debug("Fetching YARN application snapshot: applicationId={}", applicationId) }
            .doOnNext { snapshot ->
                logger.debug(
                    "Fetched YARN application snapshot: applicationId={}, state={}, containers={}",
                    snapshot.applicationId,
                    snapshot.state.name,
                    snapshot.containers.size,
                )
            }

    private fun isFinal(state: String): Boolean =
        state == "FINISHED" || state == "FAILED" || state == "KILLED"

    private companion object {
        val logger = LoggerFactory.getLogger(HadoopYarnGateway::class.java)
    }
}
