package dev.tkfs.provider

import android.database.Cursor
import android.database.MatrixCursor
import android.os.Binder
import android.os.Bundle
import android.os.CancellationSignal
import android.os.ParcelFileDescriptor
import android.os.storage.StorageManager
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import android.util.Log
import android.webkit.MimeTypeMap
import dev.tkfs.BuildConfig
import dev.tkfs.Native
import dev.tkfs.R
import org.json.JSONArray
import org.json.JSONObject
import java.io.FileNotFoundException
import java.io.IOException

/**
 * Read-only view of every folder. Android starts this on demand, so a request can be the
 * thing that starts the orchestrator; the native calls wait, bounded, for it and the folder's
 * worker.
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
        val folders = JSONArray(orNotFound { Native.manage("list", "{}") })
        for (index in 0 until folders.length()) {
            val folder = folders.getJSONObject(index)
            val id = folder.getString("id")
            cursor.newRow()
                .add(Root.COLUMN_ROOT_ID, id)
                .add(Root.COLUMN_DOCUMENT_ID, DocumentIds.make(id, "/"))
                .add(Root.COLUMN_TITLE, folder.getString("label"))
                .add(Root.COLUMN_ICON, R.drawable.ic_folder)
                .add(Root.COLUMN_FLAGS, Root.FLAG_SUPPORTS_IS_CHILD)
        }
        return cursor
    }

    override fun queryDocument(documentId: String, projection: Array<out String>?): Cursor {
        val cursor = MatrixCursor(projection ?: documentColumns)
        val (folder, path) = DocumentIds.split(documentId)
        addRow(cursor, documentId, JSONObject(orNotFound { Native.stat(folder, path) }), path)
        return cursor
    }

    override fun queryChildDocuments(
        parentDocumentId: String,
        projection: Array<out String>?,
        sortOrder: String?,
    ): Cursor {
        val cursor = MatrixCursor(projection ?: documentColumns)
        val (folder, path) = DocumentIds.split(parentDocumentId)
        val entries = JSONArray(orNotFound { Native.list(folder, path) })
        for (index in 0 until entries.length()) {
            val entry = entries.getJSONObject(index)
            val child = DocumentIds.childPath(path, entry.getString("name"))
            addRow(cursor, DocumentIds.make(folder, child), entry, child)
        }
        return cursor
    }

    override fun isChildDocument(parentDocumentId: String, documentId: String): Boolean =
        DocumentIds.isChild(parentDocumentId, documentId)

    override fun openDocument(
        documentId: String,
        mode: String,
        signal: CancellationSignal?,
    ): ParcelFileDescriptor {
        if (mode != "r") throw FileNotFoundException("READ_ONLY: cannot open for $mode")
        val (folder, path) = DocumentIds.split(documentId)
        val storage = requireNotNull(context).getSystemService(StorageManager::class.java)
        return orNotFound {
            val size = JSONObject(Native.stat(folder, path)).getLong("size")
            FileProxy.openForRead(storage, documentId, size)
        }
    }

    /** `DocumentsProvider` handles the `android:` methods; `tkfs.` ones are the harness's. */
    override fun call(method: String, arg: String?, extras: Bundle?): Bundle? {
        if (method.startsWith("tkfs.") && ShellControl.admits(BuildConfig.DEBUG, Binder.getCallingUid())) {
            ShellControl.answer(method, arg, extras)?.let { return it }
        }
        return super.call(method, arg, extras)
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

    /** The provider API only lets these calls throw FileNotFoundException. */
    private fun <T> orNotFound(block: () -> T): T = try {
        block()
    } catch (error: IOException) {
        Log.w(TAG, error.message ?: "request failed")
        throw FileNotFoundException(error.message)
    }

    private companion object {
        const val TAG = "tkfs"
    }
}
