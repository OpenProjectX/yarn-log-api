package org.openprojectx.hadoop.yarn.log.api.engine

import kotlin.test.Test
import kotlin.test.assertEquals

class SingleLineLogTextTest {
    @Test
    fun `escapes line breaks tabs backslashes and controls`() {
        val bytes = "first\r\nsecond\t\\literal\u0001\u007f".toByteArray()

        assertEquals("first\\r\\nsecond\\t\\\\literal\\u0001\\u007f", bytes.toSingleLineLogText())
    }

    @Test
    fun `keeps ordinary unicode readable`() {
        assertEquals("任务完成 ✓", "任务完成 ✓".toByteArray().toSingleLineLogText())
    }

    @Test
    fun `escapes unicode line separators`() {
        assertEquals("one\\u0085two\\u2028three\\u2029four", "one\u0085two\u2028three\u2029four".toByteArray().toSingleLineLogText())
    }
}
