package dev.tkfs.provider

import android.content.Context
import android.database.ContentObserver
import android.os.Binder
import android.os.Bundle
import android.os.ParcelFileDescriptor
import android.os.StatFs
import android.provider.DocumentsContract
import dev.tkfs.Native
import dev.tkfs.Settings
import dev.tkfs.sync.SyncCoordinator
import dev.tkfs.sync.floorBytes
import org.json.JSONArray
import org.json.JSONObject
import java.io.FileOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

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
    private const val DOCUMENT = "tkfs.document"
    private const val LIST_ALL = "tkfs.list_all"
    private const val METERED = "tkfs.metered"
    private const val WAITING = "tkfs.waiting"
    private const val CONFIRM = "tkfs.confirm"
    private const val SPACE = "tkfs.space"
    private const val FREE = "tkfs.free"
    private const val A_DAY_LATER = "tkfs.a_day_later"
    private const val ROOT_UID = 0
    private const val SHELL_UID = 2000
    private const val A_DAY_MS = 25 * 60 * 60 * 1000L

    /** Descriptors a `write` was told to leave open, by document, for a kill to find or the next `write` to use. */
    private val leftOpen = HashMap<String, ParcelFileDescriptor>()

    /** How many changes Android told of in each directory's listing, since `changes` first asked. */
    private val heard = HashMap<String, AtomicInteger>()

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
        // What `content` cannot do, the way another app would: `--arg OPERATION`, and a payload.
        DOCUMENT -> reply {
            val operation = arg ?: throw IOException("MISSING_ARGUMENT: operation")
            val request = JSONObject(payload(extras?.getString("payload")))
            // Android checks the caller of each of these, and it should see the app, not the shell.
            val shell = Binder.clearCallingIdentity()
            try {
                document(context, operation, request).toString()
            } finally {
                Binder.restoreCallingIdentity(shell)
            }
        }
        // Every name in a directory, including the working files the provider hides: `--arg FOLDER`, payload {path}.
        LIST_ALL -> reply {
            val folder = arg ?: throw IOException("MISSING_ARGUMENT: folder")
            Native.list(folder, JSONObject(payload(extras?.getString("payload"))).optString("path", "/"))
        }
        // Whether the app sees the network as metered, by the call its sync policy uses.
        METERED -> reply { JSONObject().put("metered", SyncCoordinator.metered(context)).toString() }
        // The files of a folder the PCs have not confirmed: `--arg FOLDER`.
        WAITING -> reply { Native.waiting(arg ?: throw IOException("MISSING_ARGUMENT: folder")) }
        // Asks the PCs now, as after a burst: `--arg FOLDER`, answering `{waiting, unsupported}`.
        CONFIRM -> reply { Native.confirm(arg ?: throw IOException("MISSING_ARGUMENT: folder")) }
        // What the app sees of the volume, and what its last check of the floor left: `{free, total, floor, blocked}`.
        SPACE -> reply {
            val volume = StatFs(context.filesDir.path)
            JSONObject()
                .put("free", volume.availableBytes)
                .put("total", volume.totalBytes)
                .put("floor", floorBytes(volume.totalBytes))
                .put("blocked", SyncCoordinator.spaceBlocked.value ?: JSONObject.NULL)
                .toString()
        }
        // What "Free up space now" does, with no screen to show it.
        FREE -> reply {
            val freed = SyncCoordinator.freeUp(context) ?: throw IOException("UNKNOWN_ERROR: it could not ask")
            JSONObject().put("bytes", freed.bytes).put("blocked", freed.blocked ?: JSONObject.NULL).toString()
        }
        // As if a day had passed for what goes by it: when files were last opened, and the last check of the floor.
        A_DAY_LATER -> reply {
            Native.ageOpenedForTest(A_DAY_MS)
            Settings.of(context).floorChecked = null
            "{}"
        }
        else -> null
    }

    private fun document(context: Context, operation: String, request: JSONObject): JSONObject {
        val resolver = context.contentResolver
        fun uri(key: String) = DocumentsContract.buildDocumentUri(TkfsProvider.AUTHORITY, request.getString(key))
        fun made(uri: android.net.Uri?) = JSONObject().put("id", uri?.let(DocumentsContract::getDocumentId))
        return when (operation) {
            "create" -> made(
                DocumentsContract.createDocument(resolver, uri("parent"), request.getString("mime"), request.getString("name")),
            )
            "rename" -> made(DocumentsContract.renameDocument(resolver, uri("id"), request.getString("name")))
            "move" -> made(DocumentsContract.moveDocument(resolver, uri("id"), uri("from"), uri("to")))
            "delete" -> {
                DocumentsContract.deleteDocument(resolver, uri("id"))
                JSONObject()
            }
            "write" -> write(context, request)
            "changes" -> JSONObject().put("count", heardIn(context, request.getString("id")).get())
            // What the Files app asks of a listing: a thumbnail for each id, or null where none is offered.
            "thumbnails" -> {
                val size = android.graphics.Point(request.getInt("width"), request.getInt("height"))
                val ids = request.getJSONArray("ids")
                val found = JSONObject()
                for (index in 0 until ids.length()) {
                    val id = ids.getString(index)
                    val bitmap = DocumentsContract.getDocumentThumbnail(resolver, DocumentsContract.buildDocumentUri(TkfsProvider.AUTHORITY, id), size, null)
                    found.put(id, bitmap?.let { JSONObject().put("width", it.width).put("height", it.height) } ?: JSONObject.NULL)
                    bitmap?.recycle()
                }
                JSONObject().put("thumbnails", found)
            }
            else -> throw IOException("UNSUPPORTED_OPERATION: $operation")
        }
    }

    private fun heardIn(context: Context, directory: String): AtomicInteger = synchronized(heard) {
        heard.getOrPut(directory) {
            val count = AtomicInteger()
            val listing = DocumentsContract.buildChildDocumentsUri(TkfsProvider.AUTHORITY, directory)
            context.contentResolver.registerContentObserver(listing, false, object : ContentObserver(null) {
                override fun onChange(selfChange: Boolean) {
                    count.incrementAndGet()
                }
            })
            count
        }
    }

    /**
     * Writes once at `offset` through a real descriptor, fsyncs if asked, and closes unless told
     * not to. The descriptor is the one left open for this document, or a new one in `mode`.
     */
    private fun write(context: Context, request: JSONObject): JSONObject {
        val id = request.getString("id")
        val uri = DocumentsContract.buildDocumentUri(TkfsProvider.AUTHORITY, id)
        val data = Base64.getDecoder().decode(request.getString("data"))
        val descriptor = synchronized(leftOpen) { leftOpen.remove(id) }
            ?: context.contentResolver.openFileDescriptor(uri, request.getString("mode"))
            ?: throw IOException("UNKNOWN_ERROR: no descriptor for $uri")
        var close = true
        try {
            val written = FileOutputStream(descriptor.fileDescriptor).channel
                .write(ByteBuffer.wrap(data), request.getLong("offset"))
            if (request.getBoolean("fsync")) descriptor.fileDescriptor.sync()
            close = request.getBoolean("close")
            return JSONObject().put("written", written)
        } finally {
            if (close) descriptor.close() else synchronized(leftOpen) { leftOpen[id] = descriptor }
        }
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
