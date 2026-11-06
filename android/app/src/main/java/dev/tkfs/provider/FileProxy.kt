package dev.tkfs.provider

import android.os.Handler
import android.os.HandlerThread
import android.os.ParcelFileDescriptor
import android.os.ProxyFileDescriptorCallback
import android.os.storage.StorageManager
import android.system.ErrnoException
import android.system.OsConstants
import android.util.Log
import dev.tkfs.Native
import java.io.FileNotFoundException
import java.io.IOException

/** How a document is opened, from the mode string `openDocument` gets. */
data class OpenMode(val read: Boolean, val write: Boolean, val truncate: Boolean, val append: Boolean)

/** @throws FileNotFoundException carrying its code, for any mode but these. */
fun parseOpenMode(mode: String): OpenMode = when (mode) {
    "r" -> OpenMode(read = true, write = false, truncate = false, append = false)
    "w", "wt" -> OpenMode(read = false, write = true, truncate = true, append = false)
    "rw" -> OpenMode(read = true, write = true, truncate = false, append = false)
    "rwt" -> OpenMode(read = true, write = true, truncate = true, append = false)
    "wa" -> OpenMode(read = false, write = true, truncate = false, append = true)
    else -> throw FileNotFoundException("INVALID_PARAMETER: open mode \"$mode\"")
}

/** What one open document needs from its folder's engine, which can change under it. */
internal interface OpenDocument {
    fun open(): Long
    fun read(token: Long, offset: Long, out: ByteArray, size: Int): Int
    fun length(token: Long): Long
    fun close(token: Long)
}

internal interface WritableDocument : OpenDocument {
    fun write(token: Long, offset: Long, data: ByteArray, size: Int, append: Boolean): Int
    fun truncate(token: Long)
    fun sync(token: Long)
}

/** What a descriptor's callbacks do, for either kind of open. */
internal interface ProxiedFile {
    fun length(): Long
    fun read(offset: Long, out: ByteArray, size: Int): Int
    fun write(offset: Long, data: ByteArray, size: Int): Int
    fun sync()

    /** The last call, when the descriptor is closed. */
    fun release()

    /** The last call instead, when no descriptor could be made. */
    fun abandon()
}

/**
 * Reads one open document across a worker restart. Android can kill a worker at any time, and
 * every call goes to whichever engine serves the folder by then. Tokens are unique in the
 * process, so an old token can only be refused there, never name another open file.
 */
internal class RecoveringReader(
    private val document: OpenDocument,
    private val log: (String) -> Unit = {},
) : ProxiedFile {
    private var token = document.open()

    override fun read(offset: Long, out: ByteArray, size: Int): Int =
        recovering { document.read(token, offset, out, size) }

    override fun length(): Long = recovering { document.length(token) }

    override fun write(offset: Long, data: ByteArray, size: Int): Int =
        throw IOException("ACCESS_DENIED: opened for reading")

    override fun sync() = Unit

    fun close() = document.close(token)

    override fun release() = abandon()

    override fun abandon() {
        try {
            close()
        } catch (error: IOException) {
            log("close: ${error.message}")
        }
    }

    private inline fun <T> recovering(call: () -> T): T = try {
        call()
    } catch (error: IOException) {
        if (error.message?.substringBefore(':') !in REOPEN_AFTER) throw error
        log("reopen: ${error.message}")
        closeOldToken()
        token = document.open()
        call()
    }

    /** Frees the handle if its engine is still there. A new engine just refuses the token. */
    private fun closeOldToken() {
        try {
            document.close(token)
        } catch (error: IOException) {
            log("close: ${error.message}")
        }
    }

    private companion object {
        /** Another engine serves the folder, or soon will. Anything else, retrying won't help. */
        val REOPEN_AFTER = setOf(
            "INVALID_HANDLE",
            "STOPPING",
            "ORCHESTRATOR_STOPPING",
            "WORKER_NOT_READY",
            "ORCHESTRATOR_NOT_READY",
        )
    }
}

/**
 * One document open for writing. Unlike a reader it never reopens: what it wrote is in the draft
 * behind its token, so going on from a new token would lose or misplace it. A stale token fails
 * every call, and the draft is sealed by its old engine's stop or at the next start.
 *
 * A truncating open changes nothing until [start], which comes once a descriptor exists: until
 * then the writer only says it is empty, so a failure to make one leaves the file as it was.
 */
internal class DocumentWriter(
    private val document: WritableDocument,
    private val append: Boolean,
    truncate: Boolean = false,
    private val log: (String) -> Unit = {},
    private val released: () -> Unit,
) : ProxiedFile {
    private val token = document.open()

    @Volatile private var truncatePending = truncate

    fun start() {
        if (truncatePending) document.truncate(token)
        truncatePending = false
    }

    override fun length(): Long = if (truncatePending) 0 else document.length(token)

    override fun read(offset: Long, out: ByteArray, size: Int): Int = document.read(token, offset, out, size)

    override fun write(offset: Long, data: ByteArray, size: Int): Int =
        document.write(token, offset, data, size, append)

    override fun sync() = document.sync(token)

    /**
     * Seals what was written before this returns, because the sealer thread may be frozen
     * moments later. No writer is left to tell of a failure: the draft stays on disk for the
     * sealer or the next start, so a burst and a new listing still follow.
     */
    override fun release() {
        attempt("sync") { document.sync(token) }
        attempt("close") { document.close(token) }
        released()
    }

    override fun abandon() = attempt("close") { document.close(token) }

    private inline fun attempt(what: String, call: () -> Unit) {
        try {
            call()
        } catch (error: IOException) {
            log("$what: ${error.message}")
        }
    }
}

/**
 * One open document as a file descriptor. Its callbacks all run on the thread it owns, one
 * per open, so a call waiting on the network does not stall any other. Writes go nowhere but
 * through here, never a raw descriptor, because that would bypass the dirty tracking.
 */
internal class FileProxy private constructor(
    private val documentId: String,
    private val file: ProxiedFile,
    private val thread: HandlerThread,
) : ProxyFileDescriptorCallback() {
    override fun onGetSize(): Long = orIoError("size") { file.length() }

    override fun onRead(offset: Long, count: Int, data: ByteArray): Int =
        orIoError("read@$offset") { file.read(offset, data, count) }

    override fun onWrite(offset: Long, count: Int, data: ByteArray): Int =
        orIoError("write@$offset") { file.write(offset, data, count) }

    override fun onFsync() = orIoError("fsync") { file.sync() }

    override fun onRelease() {
        file.release()
        Log.i(TAG, "released $documentId")
        thread.quitSafely()
    }

    private inline fun <T> orIoError(what: String, call: () -> T): T = try {
        call()
    } catch (error: IOException) {
        Log.w(TAG, "$what $documentId: ${error.message}")
        throw ErrnoException(what, OsConstants.EIO, error)
    }

    private class EngineDocument(
        private val folder: String,
        private val path: String,
        private val mode: OpenMode,
    ) : WritableDocument {
        override fun open(): Long =
            if (mode.truncate) Native.openToReplace(folder, path) else Native.open(folder, path, mode.write)

        override fun read(token: Long, offset: Long, out: ByteArray, size: Int): Int =
            Native.read(folder, token, offset, out, size)

        override fun length(token: Long): Long = Native.length(folder, token)

        override fun close(token: Long) = Native.close(folder, token)

        override fun write(token: Long, offset: Long, data: ByteArray, size: Int, append: Boolean): Int =
            Native.write(folder, token, offset, data, size, append)

        override fun truncate(token: Long) = Native.truncate(folder, token, 0)

        override fun sync(token: Long) = Native.sync(folder, token)
    }

    companion object {
        private const val TAG = "tkfs"

        /** @throws IOException carrying the tkfs error code. */
        fun openForRead(storage: StorageManager, documentId: String): ParcelFileDescriptor {
            val (folder, path) = DocumentIds.split(documentId)
            val mode = parseOpenMode("r")
            val reader = RecoveringReader(EngineDocument(folder, path, mode)) { Log.i(TAG, "$documentId: $it") }
            return descriptor(storage, documentId, reader, ParcelFileDescriptor.MODE_READ_ONLY, "tkfs-read")
        }

        /**
         * `released` runs after a release has sealed and closed the handle.
         *
         * @throws IOException carrying the tkfs error code.
         */
        fun openForWrite(
            storage: StorageManager,
            documentId: String,
            mode: OpenMode,
            released: () -> Unit,
        ): ParcelFileDescriptor {
            val (folder, path) = DocumentIds.split(documentId)
            val writer = DocumentWriter(
                EngineDocument(folder, path, mode),
                mode.append,
                mode.truncate,
                { Log.w(TAG, "$documentId: $it") },
                released,
            )
            val access = when {
                mode.read -> ParcelFileDescriptor.MODE_READ_WRITE
                mode.append -> ParcelFileDescriptor.MODE_WRITE_ONLY or ParcelFileDescriptor.MODE_APPEND
                else -> ParcelFileDescriptor.MODE_WRITE_ONLY
            }
            val descriptor = descriptor(storage, documentId, writer, access, "tkfs-write")
            try {
                writer.start()
            } catch (error: IOException) {
                descriptor.close()
                throw error
            }
            return descriptor
        }

        private fun descriptor(
            storage: StorageManager,
            documentId: String,
            file: ProxiedFile,
            access: Int,
            threadName: String,
        ): ParcelFileDescriptor {
            val proxy = FileProxy(documentId, file, HandlerThread(threadName))
            proxy.thread.start()
            try {
                return storage.openProxyFileDescriptor(access, proxy, Handler(proxy.thread.looper))
            } catch (error: IOException) {
                // No release will ever come for a descriptor that was not made.
                file.abandon()
                proxy.thread.quitSafely()
                throw error
            }
        }
    }
}
