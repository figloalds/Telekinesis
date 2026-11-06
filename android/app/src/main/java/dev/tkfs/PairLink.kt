package dev.tkfs

import java.net.URLDecoder

data class PairInfo(val endpoint: String?, val code: String)

/** The link a PC shows as a QR: `tkfs://pair?endpoint=<percent-encoded>&code=<64 hex>`. */
object PairLink {
    private const val PREFIX = "tkfs://pair?"
    private val code = Regex("[0-9a-fA-F]{64}")

    /** Null for anything that is not a link with a code of 64 hex characters. */
    fun parse(text: String): PairInfo? {
        val link = text.trim()
        if (!link.startsWith(PREFIX)) return null
        val fields = link.removePrefix(PREFIX).split('&').mapNotNull { part ->
            val at = part.indexOf('=')
            if (at < 0) null else part.substring(0, at) to decode(part.substring(at + 1))
        }.toMap()
        val found = fields["code"]?.takeIf { code.matches(it) } ?: return null
        return PairInfo(fields["endpoint"]?.takeIf { it.isNotBlank() }, found)
    }

    private fun decode(value: String): String = try {
        URLDecoder.decode(value, "UTF-8")
    } catch (_: IllegalArgumentException) {
        value
    }

    fun isCode(text: String): Boolean = code.matches(text)
}
