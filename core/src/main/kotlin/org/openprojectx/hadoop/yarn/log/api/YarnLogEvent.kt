package org.openprojectx.hadoop.yarn.log.api

import java.time.Instant

enum class YarnLogEventType {
    OPEN,
    APPLICATION_STATE,
    CONTAINER_DISCOVERED,
    LOG,
    RESET,
    WARNING,
    HEARTBEAT,
    COMPLETE,
    ERROR,
}

enum class YarnLogSource {
    NODE_MANAGER,
    AGGREGATED,
}

/**
 * Transport-neutral event used by both SSE and WebSocket endpoints.
 * [data] is Base64 when [encoding] is `BASE64`. [text] is a lossy UTF-8 convenience
 * view with line breaks and control characters escaped onto one line.
 */
data class YarnLogEvent(
    val type: YarnLogEventType,
    val sequence: Long,
    val timestamp: Instant = Instant.now(),
    val applicationId: String,
    val containerId: String? = null,
    val nodeId: String? = null,
    val logType: String? = null,
    val source: YarnLogSource? = null,
    val offset: Long? = null,
    val generation: Long? = null,
    val encoding: String? = null,
    val data: String? = null,
    val text: String? = null,
    val state: String? = null,
    val message: String? = null,
)
