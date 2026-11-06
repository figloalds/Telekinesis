package dev.tkfs.provider

import android.content.Context
import android.os.Bundle
import dev.tkfs.Native
import dev.tkfs.Settings
import org.json.JSONArray
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
    private const val FAULT = "tkfs.fault"
    private const val SET_LAST_SYNC = "tkfs.set_last_sync"
    private const val ROOT_UID = 0
    private const val SHELL_UID = 2000

    fun admits(debugBuild: Boolean, callingUid: Int): Boolean =
        debugBuild && (callingUid == SHELL_UID || callingUid == ROOT_UID)

    /** `{"result": json}`, `{"error": message}`, or null for a method that is not ours. */
    fun answer(context: Context, method: String, arg: String?, extras: Bundle?): Bundle? = when (method) {
        MANAGE -> reply {
            val action = arg ?: throw IOException("MISSING_ARGUMENT: action")
            Native.manage(action, payload(extras?.getString("payload")))
        }
        // Proves a panic in native code fails the call, not the process, under Android's JNI checks.
        FAULT -> reply {
            Native.faultForTest()
            "{}"
        }
        // Ages every folder's sync clock: `--arg MILLIS_SINCE_EPOCH`, so a listing can be made stale.
        SET_LAST_SYNC -> reply {
            val ms = arg?.toLongOrNull() ?: throw IOException("MISSING_ARGUMENT: milliseconds")
            val settings = Settings.of(context)
            val folders = JSONArray(Native.manage("list", "{}"))
            for (index in 0 until folders.length()) settings.setLastSyncMs(folders.getJSONObject(index).getString("id"), ms)
            "{}"
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
