package dev.tkfs.spike

import android.database.Cursor
import android.database.MatrixCursor
import android.os.CancellationSignal
import android.os.Handler
import android.os.HandlerThread
import android.os.ParcelFileDescriptor
import android.os.ProxyFileDescriptorCallback
import android.os.storage.StorageManager
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import android.system.ErrnoException
import android.system.OsConstants
import android.util.Log
import android.webkit.MimeTypeMap
import org.json.JSONArray
import org.json.JSONObject
import java.io.FileNotFoundException
import java.io.IOException

/**
 * Read-only view of every folder. A document id is "FOLDER_ID:/path/in/folder".
 * Android starts this on demand, so a request can be the thing that starts the
 * orchestrator; the native calls wait, bounded, for it and the folder's worker.
 */
class TkfsProvider : DocumentsProvider() {
    private val rootColumns = arrayOf(
        Root.COLUMN_ROOT_ID,
        Root.COLUMN_DOCUMENT_ID,
        Root.COLUMN_TITLE,
        Root.COLUMN_ICON,
        Root.COLUMN_FLAGS,
    )
    private val documentColumns = arrayOf(
        Document.COLUMN_DOCUMENT_ID,
        Document.COLUMN_DISPLAY_NAME,
        Document.COLUMN_MIME_TYPE,
        Document.COLUMN_SIZE,
        Document.COLUMN_LAST_MODIFIED,
        Document.COLUMN_FLAGS,
    )

    override fun onCreate(): Boolean {
        Native.ensureStarted(requireNotNull(context))
        return true
    }

    override fun queryRoots(projection: Array<out String>?): Cursor {
        val cursor = MatrixCursor(projection ?: rootColumns)
        val folders = JSONArray(call { Native.manage("list", "{}") })
        for (index in 0 until folders.length()) {
            val folder = folders.getJSONObject(index)
            val id = folder.getString("id")
            cursor.newRow()
                .add(Root.COLUMN_ROOT_ID, id)
                .add(Root.COLUMN_DOCUMENT_ID, "$id:/")
                .add(Root.COLUMN_TITLE, folder.getString("label"))
                .add(Root.COLUMN_ICON, R.drawable.ic_folder)
                .add(Root.COLUMN_FLAGS, Root.FLAG_SUPPORTS_IS_CHILD)
        }
        return cursor
    }

    override fun queryDocument(documentId: String, projection: Array<out String>?): Cursor {
        val cursor = MatrixCursor(projection ?: documentColumns)
        val (folder, path) = split(documentId)
        addRow(cursor, documentId, JSONObject(call { Native.stat(folder, path) }), path)
        return cursor
    }

    override fun queryChildDocuments(
        parentDocumentId: String,
        projection: Array<out String>?,
        sortOrder: String?,
    ): Cursor {
        val cursor = MatrixCursor(projection ?: documentColumns)
        val (folder, path) = split(parentDocumentId)
        val entries = JSONArray(call { Native.list(folder, path) })
        for (index in 0 until entries.length()) {
            val entry = entries.getJSONObject(index)
            val child = path.trimEnd('/') + "/" + entry.getString("name")
            addRow(cursor, "$folder:$child", entry, child)
        }
        return cursor
    }

    override fun isChildDocument(parentDocumentId: String, documentId: String): Boolean {
        val parent = parentDocumentId.trimEnd('/') + "/"
        return documentId.startsWith(parent) && documentId != parentDocumentId
    }

    override fun openDocument(
        documentId: String,
        mode: String,
        signal: CancellationSignal?,
    ): ParcelFileDescriptor {
        if (mode != "r") throw FileNotFoundException("READ_ONLY: this spike cannot open for $mode")
        val (folder, path) = split(documentId)
        val size = JSONObject(call { Native.stat(folder, path) }).getLong("size")
        var token = call { Native.open(folder, path) }
        // One thread per open: a read waiting on the network must not stall others.
        val thread = HandlerThread("tkfs-read").apply { start() }
        val storage = requireNotNull(context).getSystemService(StorageManager::class.java)
        return storage.openProxyFileDescriptor(
            ParcelFileDescriptor.MODE_READ_ONLY,
            object : ProxyFileDescriptorCallback() {
                override fun onGetSize(): Long = size

                override fun onRead(offset: Long, count: Int, data: ByteArray): Int = try {
                    try {
                        Native.read(folder, token, offset, data, count)
                    } catch (error: IOException) {
                        // Bytes unavailable: retrying would only wait again.
                        if (error.message?.contains("OBJECT_MISSING") == true) throw error
                        // The worker restarted (after a freeze) and forgot this handle.
                        Log.i(TAG, "reopen $documentId: ${error.message}")
                        token = Native.open(folder, path)
                        Native.read(folder, token, offset, data, count)
                    }
                } catch (error: IOException) {
                    Log.w(TAG, "read $documentId@$offset: ${error.message}")
                    throw ErrnoException("read", OsConstants.EIO, error)
                }

                override fun onRelease() {
                    try {
                        Native.close(folder, token)
                    } catch (error: IOException) {
                        Log.w(TAG, "close $documentId: ${error.message}")
                    }
                    thread.quitSafely()
                }
            },
            Handler(thread.looper),
        )
    }

    private fun addRow(cursor: MatrixCursor, id: String, entry: JSONObject, path: String) {
        val name = entry.getString("name").ifEmpty { path }
        cursor.newRow()
            .add(Document.COLUMN_DOCUMENT_ID, id)
            .add(Document.COLUMN_DISPLAY_NAME, name)
            .add(Document.COLUMN_MIME_TYPE, mime(name, entry.getBoolean("directory")))
            .add(Document.COLUMN_SIZE, entry.getLong("size"))
            .add(Document.COLUMN_LAST_MODIFIED, entry.getLong("modified_ms"))
            .add(Document.COLUMN_FLAGS, 0)
    }

    private fun mime(name: String, directory: Boolean): String {
        if (directory) return Document.MIME_TYPE_DIR
        val extension = name.substringAfterLast('.', "").lowercase()
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)
            ?: "application/octet-stream"
    }

    private fun split(documentId: String): Pair<String, String> {
        val at = documentId.indexOf(':')
        if (at < 0) throw FileNotFoundException("INVALID_DOCUMENT_ID: $documentId")
        return documentId.substring(0, at) to documentId.substring(at + 1)
    }

    /** The provider API only lets these calls throw FileNotFoundException. */
    private fun <T> call(block: () -> T): T = try {
        block()
    } catch (error: IOException) {
        Log.w(TAG, error.message ?: "request failed")
        throw FileNotFoundException(error.message)
    }

    private companion object {
        const val TAG = "tkfs"
    }
}
