package org.openprojectx.hadoop.yarn.log.api.engine

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class NodeManagerLogResponseParserTest {
    private val parser = NodeManagerLogResponseParser()

    @Test
    fun `parses a suffix response using byte lengths`() {
        val content = byteArrayOf(0x00, 0x01, 0x7f, 0xff.toByte())
        val response = response(fileLength = 10, content = content)

        val parsed = parser.parse(response, requestedTailBytes = 4)

        assertEquals(10, parsed.fileLength)
        assertEquals(6, parsed.responseStartOffset)
        assertContentEquals(content, parsed.bytes)
    }

    @Test
    fun `does not treat trailer text inside the log as framing`() {
        val content = "before End of LogType:stdout after".encodeToByteArray()
        val response = response(fileLength = content.size.toLong(), content = content)

        val parsed = parser.parse(response, requestedTailBytes = 1024)

        assertContentEquals(content, parsed.bytes)
    }

    @Test
    fun `rejects a truncated response`() {
        val response = response(fileLength = 100, content = "short".encodeToByteArray())

        assertFailsWith<IllegalArgumentException> {
            parser.parse(response, requestedTailBytes = 50)
        }
    }

    private fun response(fileLength: Long, content: ByteArray): ByteArray =
        buildString {
            appendLine("Container: container_1 on node:8041")
            appendLine("LogAggregationType: LOCAL")
            appendLine("====================================")
            appendLine("LogType:stdout")
            appendLine("LogLastModifiedTime:now")
            appendLine("LogLength:$fileLength")
            appendLine("LogContents:")
        }.encodeToByteArray() + content + "End of LogType:stdout\n".encodeToByteArray()
}
