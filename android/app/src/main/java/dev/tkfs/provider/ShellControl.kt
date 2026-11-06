package dev.tkfs.provider

import android.os.Bundle
import dev.tkfs.Native
import java.io.IOException
import java.util.Base64

/**
 * How the end-to-end harness drives the app from outside:
 *
 *     adb shell content call --uri content://dev.tkfs.documents --method tkfs.manage \
 *         --arg ACTION --extra payload:s:BASE64_OF_JSON
 *
 * Only a debug build's manifest lets the shell reach the provider at all, and a provider's
 * `call` has no permission of its own to check, so this answers a shell or root caller and
 * nobody else.
 */
internal object ShellControl {
    private const val MANAGE = "tkfs.manage"
    private const val ROOT_UID = 0
    private const val SHELL_UID = 2000

    fun admits(debugBuild: Boolean, callingUid: Int): Boolean =
        debugBuild && (callingUid == SHELL_UID || callingUid == ROOT_UID)

    /** `{"result": json}`, `{"error": message}`, or null for a method that is not ours. */
    fun answer(method: String, arg: String?, extras: Bundle?): Bundle? = when (method) {
        MANAGE -> reply {
            val action = arg ?: throw IOException("ARGUMENT_REQUIRED: action")
            Native.manage(action, payload(extras?.getString("payload")))
        }
        else -> null
    }

    /** `content call` refuses an extra with a colon in its value, so JSON comes as base64. */
    fun payload(encoded: String?): String {
        if (encoded == null) return "{}"
        return try {
            String(Base64.getDecoder().decode(encoded), Charsets.UTF_8)
        } catch (error: IllegalArgumentException) {
            throw IOException("INVALID_REQUEST: the payload is not base64", error)
        }
    }

    private fun reply(block: () -> String): Bundle = Bundle().apply {
        try {
            putString("result", block())
        } catch (error: IOException) {
            putString("error", error.message ?: "request failed")
        }
    }
}
