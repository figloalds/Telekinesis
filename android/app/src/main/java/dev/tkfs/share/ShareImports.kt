package dev.tkfs.share

import android.content.ContentResolver
import android.content.Context
import android.net.Uri
import android.provider.OpenableColumns
import android.util.Log
import dev.tkfs.Native
import java.io.IOException

/** What is in flight in this process, and the clean-up of what a killed process left. */
internal object ShareImports {
    private const val TAG = "tkfs"
    private val live = HashSet<String>()
    private var sweeping = false

    @Synchronized fun begin(id: String) {
        live += id
    }

    @Synchronized fun end(id: String) {
        live -= id
    }

    @Synchronized private fun running(): Set<String> = live.toSet()

    /**
     * Reports what a killed process did not save, and removes its working files through the
     * engine, so that the removal syncs. A share that is running here is left alone. What cannot be
     * removed yet (the folder is not running) stays in the ledger for the next call.
     */
    fun recover(context: Context) {
        synchronized(this) {
            if (sweeping) return
            sweeping = true
        }
        try {
            val ledger = ShareLedger.of(context)
            ledger.recover(running())
            for (orphan in ledger.orphans()) {
                try {
                    Native.delete(orphan.folder, orphan.path)
                    ledger.swept(orphan)
                } catch (error: IOException) {
                    // Not there is as good as removed.
                    if (error.message?.startsWith("FILE_NOT_FOUND") == true) ledger.swept(orphan)
                    else Log.w(TAG, "share: sweep ${orphan.path}: ${error.message}")
                }
            }
        } finally {
            synchronized(this) { sweeping = false }
        }
    }

    /** At app start, in the background: the orchestrator may not be up yet, and the native calls wait for it. */
    fun recoverInBackground(context: Context) {
        val app = context.applicationContext
        Thread {
            try {
                Native.ensureStarted(app)
                recover(app)
            } catch (error: IOException) {
                Log.w(TAG, "share: recovery: ${error.message}")
            }
        }.apply { isDaemon = true }.start()
    }
}

/** What the sender calls a stream, made safe to be one entry of a directory. */
internal fun displayNameOf(resolver: ContentResolver, uri: Uri): String {
    val name = try {
        resolver.query(uri, arrayOf(OpenableColumns.DISPLAY_NAME), null, null, null)?.use { cursor ->
            if (cursor.moveToFirst() && !cursor.isNull(0)) cursor.getString(0) else null
        }
    } catch (error: RuntimeException) {
        null
    }
    return entryName(name?.takeIf { it.isNotEmpty() } ?: uri.lastPathSegment.orEmpty())
}
