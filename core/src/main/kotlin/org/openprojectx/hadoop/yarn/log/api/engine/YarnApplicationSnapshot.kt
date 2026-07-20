package org.openprojectx.hadoop.yarn.log.api.engine

import org.apache.hadoop.yarn.api.records.YarnApplicationState

data class YarnContainerSnapshot(
    val containerId: String,
    val nodeId: String,
    val nodeHttpAddress: String,
    val state: String,
)

data class YarnApplicationSnapshot(
    val applicationId: String,
    val owner: String,
    val state: YarnApplicationState,
    val containers: List<YarnContainerSnapshot>,
) {
    val isFinal: Boolean
        get() = state == YarnApplicationState.FINISHED ||
            state == YarnApplicationState.FAILED ||
            state == YarnApplicationState.KILLED
}
