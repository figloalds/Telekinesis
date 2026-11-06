package dev.tkfs.provider

import android.content.res.AssetFileDescriptor
import android.database.Cursor
import android.database.MatrixCursor
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Point
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
import dev.tkfs.BuildConfig
import dev.tkfs.Native
import dev.tkfs.Notifications
import dev.tkfs.R
import dev.tkfs.share.PART_PREFIX
import dev.tkfs.share.ShareImports
import dev.tkfs.sync.SyncCoordinator
import dev.tkfs.ui.isOnThisPhone
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

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
        Document.COLUMN_SUMMARY,
    )

    override fun onCreate(): Boolean {
        Native.ensureStarted(requireNotNull(context))
        // A process that was killed mid-copy left a report and working files to remove.
        ShareImports.recoverInBackground(requireNotNull(context))
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
            // A file being shared in has this name until it is whole.
            if (entry.getString("name").startsWith(PART_PREFIX)) continue
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
            // Bytes the phone lacks come from a PC, and with none there a read would wait for it.
            // A truncating open needs nothing of the old bytes, so it never asks.
            if (open.read && !open.truncate) failFastWithoutAPC(documentId, signal)
            if (open.write) {
                FileProxy.openForWrite(storage, documentId, open) { changed(DocumentIds.parent(documentId)) }
            } else {
                FileProxy.openForRead(storage, documentId)
            }
        }
    }

    /**
     * For a file that is not all here: waits, bounded, for a PC to be reachable, and if none is,
     * fails and tells the owner, since a viewer's own error message will not say why.
     */
    private fun failFastWithoutAPC(documentId: String, signal: CancellationSignal?) {
        val (folder, path) = DocumentIds.split(documentId)
        val entry = JSONObject(Native.stat(folder, path))
        if (isOnThisPhone(entry.getLong("size"), entry.getLong("local_bytes"))) return
        try {
            awaiting(signal) { Native.reachPeer(folder) }
        } catch (error: IOException) {
            if (error.message?.substringBefore(':') == "PC_UNREACHABLE") {
                val name = path.substringAfterLast('/')
                OpenNotices.of(requireNotNull(context)).add(name)
                Notifications.cannotOpen(requireNotNull(context), name)
            }
            throw error
        }
    }

    /**
     * Runs `block` on a thread of its own and waits for it, so that a cancelled caller is let go
     * at once. The native wait it started ends by itself within its own bound.
     */
    private fun <T> awaiting(signal: CancellationSignal?, block: () -> T): T {
        val job = waits.submit<T> { block() }
        try {
            while (true) {
                signal?.throwIfCanceled()
                try {
                    return job.get(CANCEL_POLL_MS, TimeUnit.MILLISECONDS)
                } catch (_: TimeoutException) {
                    continue
                } catch (failed: ExecutionException) {
                    throw failed.cause ?: failed
                }
            }
        } finally {
            if (!job.isDone) job.cancel(true)
        }
    }

    /** Only for an image that is all here: a thumbnail never fetches anything. */
    override fun openDocumentThumbnail(
        documentId: String,
        sizeHint: Point,
        signal: CancellationSignal?,
    ): AssetFileDescriptor {
        val (folder, path) = DocumentIds.split(documentId)
        val entry = orNotFound { JSONObject(Native.stat(folder, path)) }
        val mime = mimeOf(path.substringAfterLast('/'), entry.getBoolean("directory"))
        if (!offersThumbnail(mime, entry.getLong("size"), entry.getLong("local_bytes"))) {
            throw FileNotFoundException("NOT_FOUND: no thumbnail for $documentId")
        }
        signal?.throwIfCanceled()
        val bounds = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        orNotFound { NativeStream(folder, path).use { BitmapFactory.decodeStream(it, null, bounds) } }
        if (bounds.outWidth <= 0 || bounds.outHeight <= 0) {
            throw FileNotFoundException("NOT_FOUND: not an image: $documentId")
        }
        signal?.throwIfCanceled()
        val options = BitmapFactory.Options().apply {
            inSampleSize = sampleSize(bounds.outWidth, bounds.outHeight, sizeHint.x, sizeHint.y)
        }
        val bitmap = orNotFound { NativeStream(folder, path).use { BitmapFactory.decodeStream(it, null, options) } }
            ?: throw FileNotFoundException("NOT_FOUND: could not decode $documentId")
        try {
            signal?.throwIfCanceled()
            val cache = File(requireNotNull(context).cacheDir, "thumbnails").apply { mkdirs() }
            val file = File.createTempFile("thumb", ".img", cache)
            try {
                file.outputStream().use {
                    val format = if (bitmap.hasAlpha()) Bitmap.CompressFormat.PNG else Bitmap.CompressFormat.JPEG
                    bitmap.compress(format, THUMBNAIL_QUALITY, it)
                }
                val descriptor = ParcelFileDescriptor.open(file, ParcelFileDescriptor.MODE_READ_ONLY)
                return AssetFileDescriptor(descriptor, 0, AssetFileDescriptor.UNKNOWN_LENGTH)
            } finally {
                // The descriptor keeps what it opened.
                file.delete()
            }
        } finally {
            bitmap.recycle()
        }
    }

    /** A file all here, read through the engine, which asks no one for what is. */
    private class NativeStream(private val folder: String, path: String) : InputStream() {
        private var token = Native.open(folder, path, false)
        private var offset = 0L

        override fun read(): Int {
            val one = ByteArray(1)
            return if (read(one, 0, 1) < 0) -1 else one[0].toInt() and 0xFF
        }

        override fun read(buffer: ByteArray, at: Int, length: Int): Int {
            if (length == 0) return 0
            val chunk = ByteArray(minOf(length, STREAM_CHUNK))
            val count = Native.read(folder, token, offset, chunk, chunk.size)
            if (count <= 0) return -1
            System.arraycopy(chunk, 0, buffer, at, count)
            offset += count
            return count
        }

        override fun close() {
            if (token < 0) return
            val last = token
            token = -1
            Native.close(folder, last)
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
        val directory = entry.getBoolean("directory")
        val size = entry.getLong("size")
        val local = entry.getLong("local_bytes")
        val mime = mimeOf(name, directory)
        val thumbnail = if (offersThumbnail(mime, size, local)) Document.FLAG_SUPPORTS_THUMBNAIL else 0
        cursor.newRow()
            .add(Document.COLUMN_DOCUMENT_ID, id)
            .add(Document.COLUMN_DISPLAY_NAME, name)
            .add(Document.COLUMN_MIME_TYPE, mime)
            .add(Document.COLUMN_SIZE, size)
            .add(Document.COLUMN_LAST_MODIFIED, entry.getLong("modified_ms"))
            .add(Document.COLUMN_FLAGS, flags(directory, path == "/") or thumbnail)
            .add(Document.COLUMN_SUMMARY, summary(directory, size, local))
    }

    /** "On this phone" or "Not downloaded"; a directory holds nothing of its own. */
    private fun summary(directory: Boolean, size: Long, local: Long): String? = when {
        directory -> null
        isOnThisPhone(size, local) -> "On this phone"
        else -> "Not downloaded"
    }

    /** A folder's root can only hold things: it has no parent to go from or name to change. */
    private fun flags(directory: Boolean, folderRoot: Boolean): Int {
        val holds = if (directory) Document.FLAG_DIR_SUPPORTS_CREATE else Document.FLAG_SUPPORTS_WRITE
        if (folderRoot) return holds
        return holds or Document.FLAG_SUPPORTS_DELETE or Document.FLAG_SUPPORTS_RENAME or Document.FLAG_SUPPORTS_MOVE
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
        private const val CANCEL_POLL_MS = 50L
        private const val THUMBNAIL_QUALITY = 85
        private const val STREAM_CHUNK = 256 * 1024

        /** The waits for a PC, each on a thread that is not the caller's, so a cancel is heard. */
        private val waits = Executors.newCachedThreadPool()
    }
}
