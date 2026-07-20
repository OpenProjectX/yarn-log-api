package org.openprojectx.hadoop.yarn.log.api.engine

internal fun ByteArray.toSingleLineLogText(): String = toString(Charsets.UTF_8).toSingleLineLogText()

private fun String.toSingleLineLogText(): String = buildString(length) {
    for (character in this@toSingleLineLogText) {
        when (character) {
            '\\' -> append("\\\\")
            '\n' -> append("\\n")
            '\r' -> append("\\r")
            '\t' -> append("\\t")
            '\b' -> append("\\b")
            '\u000c' -> append("\\f")
            '\u0085' -> append("\\u0085")
            '\u2028' -> append("\\u2028")
            '\u2029' -> append("\\u2029")
            else -> if (character.code < 0x20 || character.code == 0x7f) {
                append("\\u")
                append(character.code.toString(16).padStart(4, '0'))
            } else {
                append(character)
            }
        }
    }
}
