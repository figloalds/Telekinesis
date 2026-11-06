package dev.tkfs.provider

import android.database.Cursor
import android.database.MatrixCursor
import android.os.Binder
import android.os.Bundle
import android.os.CancellationSignal
import android.os.ParcelFileDescriptor
import android.os.storage.StorageManager
import android.provider.DocumentsContract
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import android.util.Log
import android.webkit.MimeTypeMap
import dev.tkfs.BuildConfig
import dev.tkfs.Native
import dev.tkfs.R
import dev.tkfs.sync.SyncCoordinator
import org.json.JSONArray
import org.json.JSONObject
import java.io.FileNotFoundException
import java.io.IOException

/**
 * Every folder as a tree of documents. Android starts this on demand, so a request can be the
 * thing that starts the orchestrator; the native calls wait, bounded, for it and the folder's
 * worker. Failures reach a caller as FileNotFoundException, the only one the provider API allows
 * these calls to throw, with the tkfs error code as its message.
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
        cursor.setNotificationUri(resolver(), DocumentsContract.buildRootsUri(AUTHORITY))
        val folders = JSONArray(orNotFound { Native.manage("list", "{}") })
        for (index in 0 until folders.length()) {
            val folder = folders.getJSONObject(index)
            val id = folder.getString("id")
            cursor.newRow()
                .add(Root.COLUMN_ROOT_ID, id)
                .add(Root.COLUMN_DOCUMENT_ID, DocumentIds.make(id, "/"))
                .add(Root.COLUMN_TITLE, folder.getString("label"))
                .add(Root.COLUMN_ICON, R.drawable.ic_folder)
                .add(Root.COLUMN_FLAGS, Root.FLAG_SUPPORTS_IS_CHILD or Root.FLAG_SUPPORTS_CREATE)
        }
        return cursor
    }

    override fun queryDocument(documentId: String, projection: Array<out String>?): Cursor {
        val cursor = MatrixCursor(projection ?: documentColumns)
        cursor.setNotificationUri(resolver(), DocumentsContract.buildDocumentUri(AUTHORITY, documentId))
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
        cursor.setNotificationUri(resolver(), DocumentsContract.buildChildDocumentsUri(AUTHORITY, parentDocumentId))
        val (folder, path) = DocumentIds.split(parentDocumentId)
        // Answered from what is known; a burst behind it brings what is new and notifies.
        SyncCoordinator.onListing(requireNotNull(context), folder)
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
        val open = parseOpenMode(mode)
        Log.i(TAG, "open $documentId $mode")
        val storage = requireNotNull(context).getSystemService(StorageManager::class.java)
        return orNotFound {
            if (open.write) {
                FileProxy.openForWrite(storage, documentId, open) { changed(DocumentIds.parent(documentId)) }
            } else {
                FileProxy.openForRead(storage, documentId)
            }
        }
    }

    override fun createDocument(parentDocumentId: String, mimeType: String, displayName: String): String {
        val name = DocumentIds.requireName(displayName)
        val (folder, parent) = DocumentIds.split(parentDocumentId)
        val path = orNotFound { createNumbered(folder, parent, name, mimeType == Document.MIME_TYPE_DIR) }
        changed(parentDocumentId)
        return DocumentIds.make(folder, path)
    }

    /** A burst can bring the same name in between a listing and the create. */
    private fun createNumbered(folder: String, parent: String, name: String, directory: Boolean): String {
        var attempts = 0
        while (true) {
            val path = DocumentIds.childPath(parent, numberedName(names(folder, parent), name))
            try {
                Native.create(folder, path, directory)
                return path
            } catch (error: IOException) {
                val clash = error.message?.substringBefore(':') == "ALREADY_EXISTS"
                if (!clash || ++attempts == CREATE_ATTEMPTS) throw error
            }
        }
    }

    private fun names(folder: String, path: String): Set<String> {
        val entries = JSONArray(Native.list(folder, path))
        return (0 until entries.length()).mapTo(HashSet()) { entries.getJSONObject(it).getString("name") }
    }

    override fun renameDocument(documentId: String, displayName: String): String {
        val name = DocumentIds.requireName(displayName)
        val (folder, path) = DocumentIds.split(documentId)
        val target = DocumentIds.childPath(DocumentIds.parentPath(path), name)
        orNotFound { Native.rename(folder, path, target) }
        changed(DocumentIds.parent(documentId))
        return DocumentIds.make(folder, target)
    }

    /** Across folders this refuses, and then Android copies and deletes. */
    override fun moveDocument(
        sourceDocumentId: String,
        sourceParentDocumentId: String,
        targetParentDocumentId: String,
    ): String {
        val (folder, path) = DocumentIds.split(sourceDocumentId)
        val (targetFolder, targetParent) = DocumentIds.split(targetParentDocumentId)
        if (targetFolder != folder) throw FileNotFoundException("UNSUPPORTED_OPERATION: a move across folders")
        val target = DocumentIds.childPath(targetParent, path.substringAfterLast('/'))
        orNotFound { Native.rename(folder, path, target) }
        changed(DocumentIds.parent(sourceDocumentId), targetParentDocumentId)
        return DocumentIds.make(folder, target)
    }

    override fun deleteDocument(documentId: String) {
        val (folder, path) = DocumentIds.split(documentId)
        orNotFound { Native.delete(folder, path) }
        changed(DocumentIds.parent(documentId))
    }

    /** Something here changed: a burst takes it to the PC, and an open Files window lists it again. */
    private fun changed(vararg parents: String) {
        SyncCoordinator.onLocalChange(requireNotNull(context))
        for (parent in parents) {
            resolver().notifyChange(DocumentsContract.buildChildDocumentsUri(AUTHORITY, parent), null)
        }
    }

    /** `DocumentsProvider` handles the `android:` methods; `tkfs.` ones are the harness's. */
    override fun call(method: String, arg: String?, extras: Bundle?): Bundle? {
        if (method.startsWith("tkfs.") && ShellControl.admits(BuildConfig.DEBUG, Binder.getCallingUid())) {
            ShellControl.answer(requireNotNull(context), method, arg, extras)?.let { return it }
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
            .add(Document.COLUMN_FLAGS, flags(entry.getBoolean("directory"), path == "/"))
    }

    /** A folder's root can only hold things: it has no parent to go from or name to change. */
    private fun flags(directory: Boolean, folderRoot: Boolean): Int {
        val holds = if (directory) Document.FLAG_DIR_SUPPORTS_CREATE else Document.FLAG_SUPPORTS_WRITE
        if (folderRoot) return holds
        return holds or Document.FLAG_SUPPORTS_DELETE or Document.FLAG_SUPPORTS_RENAME or Document.FLAG_SUPPORTS_MOVE
    }

    private fun mime(name: String, directory: Boolean): String {
        if (directory) return Document.MIME_TYPE_DIR
        val extension = name.substringAfterLast('.', "").lowercase()
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)
            ?: "application/octet-stream"
    }

    private fun <T> orNotFound(block: () -> T): T = try {
        block()
    } catch (error: IOException) {
        Log.w(TAG, error.message ?: "request failed")
        throw FileNotFoundException(error.message)
    }

    private fun resolver() = requireNotNull(context).contentResolver

    companion object {
        const val AUTHORITY = "dev.tkfs.documents"
        private const val TAG = "tkfs"
        private const val CREATE_ATTEMPTS = 3
    }
}
