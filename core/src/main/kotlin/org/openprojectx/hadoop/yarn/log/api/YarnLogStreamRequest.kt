package org.openprojectx.hadoop.yarn.log.api

import java.time.Duration

data class YarnLogStreamRequest(
    val applicationId: String,
    val requester: String? = null,
    val follow: Boolean = true,
    val logFiles: Set<String> = setOf("stdout", "stderr"),
    val containerIds: Set<String> = emptySet(),
    val tailBytes: Long = 64 * 1024,
    val pollInterval: Duration = Duration.ofSeconds(1),
)
