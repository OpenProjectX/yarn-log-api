package org.openprojectx.hadoop.yarn.log.api.engine

import java.time.Duration

data class YarnLogEngineSettings(
    val maxConcurrentNodeManagerRequests: Int = 16,
    val maxTailWindowBytes: Long = 8 * 1024 * 1024,
    val heartbeatInterval: Duration = Duration.ofSeconds(15),
    val aggregationWaitTimeout: Duration = Duration.ofMinutes(2),
    val aggregationRetryInterval: Duration = Duration.ofSeconds(2),
)
