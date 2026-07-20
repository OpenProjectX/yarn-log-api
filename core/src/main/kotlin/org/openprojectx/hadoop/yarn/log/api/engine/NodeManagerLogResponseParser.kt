package org.openprojectx.hadoop.yarn.log.api.engine

import java.nio.charset.StandardCharsets

data class NodeManagerLogResponse(
    val fileLength: Long,
    val responseStartOffset: Long,
    val bytes: ByteArray,
)

/** Extracts only the local log payload from Hadoop's header/body/trailer response. */
class NodeManagerLogResponseParser {
    fun parse(response: ByteArray, requestedTailBytes: Long): NodeManagerLogResponse {
        require(requestedTailBytes > 0) { "requestedTailBytes must be positive" }
        val marker = "LogContents:\n".toByteArray(StandardCharsets.UTF_8)
        val contentOffset = response.indexOf(marker)
        require(contentOffset >= 0) { "NodeManager response has no LogContents header" }

        val headerEnd = contentOffset + marker.size
        val header = String(response, 0, contentOffset, StandardCharsets.UTF_8)
        val fileLength = LOG_LENGTH.find(header)?.groupValues?.get(1)?.toLong()
            ?: error("NodeManager response has no LogLength header")
        val returnedLength = minOf(requestedTailBytes, fileLength).toInt()
        require(response.size >= headerEnd + returnedLength) {
            "Truncated NodeManager response: expected $returnedLength log bytes"
        }
        return NodeManagerLogResponse(
            fileLength = fileLength,
            responseStartOffset = fileLength - returnedLength,
            bytes = response.copyOfRange(headerEnd, headerEnd + returnedLength),
        )
    }

    private fun ByteArray.indexOf(needle: ByteArray): Int {
        if (needle.isEmpty()) return 0
        for (start in 0..size - needle.size) {
            var matches = true
            for (index in needle.indices) {
                if (this[start + index] != needle[index]) {
                    matches = false
                    break
                }
            }
            if (matches) return start
        }
        return -1
    }

    private companion object {
        val LOG_LENGTH = Regex("(?:^|\\n)LogLength:(\\d+)(?:\\n|$)")
    }
}
