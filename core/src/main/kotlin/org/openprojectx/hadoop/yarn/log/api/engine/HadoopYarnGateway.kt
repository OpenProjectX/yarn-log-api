package org.openprojectx.hadoop.yarn.log.api.engine

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.client.api.YarnClient
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

class HadoopYarnGateway(
    private val yarnClient: YarnClient,
    private val hadoopScheduler: Scheduler,
) {
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

    private fun isFinal(state: String): Boolean =
        state == "FINISHED" || state == "FAILED" || state == "KILLED"
}
