package org.openprojectx.hadoop.yarn.log.api

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface YarnApplicationQueryService {
    fun application(requester: String?, applicationId: String): Mono<YarnApplicationInfo>

    fun attempts(requester: String?, applicationId: String): Flux<YarnApplicationAttemptInfo>

    fun containers(requester: String?, applicationId: String): Flux<YarnContainerInfo>
}

data class YarnApplicationInfo(
    val applicationId: String,
    val currentAttemptId: String?,
    val name: String,
    val applicationType: String,
    val user: String,
    val queue: String,
    val state: String,
    val finalStatus: String,
    val progress: Float,
    val trackingUrl: String?,
    val originalTrackingUrl: String?,
    val diagnostics: String?,
    val submitTime: Long,
    val startTime: Long,
    val launchTime: Long,
    val finishTime: Long,
    val logAggregationStatus: String?,
    val tags: Set<String>,
)

data class YarnApplicationAttemptInfo(
    val attemptId: String,
    val state: String,
    val amContainerId: String?,
    val host: String?,
    val rpcPort: Int,
    val trackingUrl: String?,
    val originalTrackingUrl: String?,
    val diagnostics: String?,
    val startTime: Long,
    val finishTime: Long,
)

data class YarnContainerInfo(
    val containerId: String,
    val attemptId: String,
    val state: String,
    val nodeId: String?,
    val nodeHttpAddress: String?,
    val logUrl: String?,
    val diagnostics: String?,
    val exitStatus: Int,
    val memoryMb: Long?,
    val virtualCores: Int?,
    val creationTime: Long,
    val finishTime: Long,
)

class YarnApplicationAccessDeniedException(applicationId: String) :
    SecurityException("Not authorized to read $applicationId")
