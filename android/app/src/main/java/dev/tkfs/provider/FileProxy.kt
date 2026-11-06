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
import java.io.IOException

/** What one open document needs from its folder's engine, which can change under it. */
internal interface OpenDocument {
    fun open(): Long
    fun read(token: Long, offset: Long, out: ByteArray, size: Int): Int
    fun close(token: Long)
}

/**
 * Reads one open document across a worker restart. Android can kill a worker at any time, and
 * every call goes to whichever engine serves the folder by then. Tokens are unique in the
 * process, so an old token can only be refused there, never name another open file.
 */
internal class RecoveringReader(
    private val document: OpenDocument,
    private val log: (String) -> Unit = {},
) {
    private var token = document.open()

    fun read(offset: Long, out: ByteArray, size: Int): Int = try {
        document.read(token, offset, out, size)
    } catch (error: IOException) {
        if (error.message?.substringBefore(':') !in REOPEN_AFTER) throw error
        log("reopen: ${error.message}")
        closeOldToken()
        token = document.open()
        document.read(token, offset, out, size)
    }

    fun close() = document.close(token)

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
 * One open document as a file descriptor. Its callbacks all run on the thread it owns, one
 * per open, so a read waiting on the network does not stall any other.
 */
internal class FileProxy private constructor(
    private val documentId: String,
    private val reader: RecoveringReader,
    private val size: Long,
    private val thread: HandlerThread,
) : ProxyFileDescriptorCallback() {
    override fun onGetSize(): Long = size

    override fun onRead(offset: Long, count: Int, data: ByteArray): Int = try {
        reader.read(offset, data, count)
    } catch (error: IOException) {
        Log.w(TAG, "read $documentId@$offset: ${error.message}")
        throw ErrnoException("read", OsConstants.EIO, error)
    }

    override fun onRelease() {
        close()
        thread.quitSafely()
    }

    private fun close() {
        try {
            reader.close()
        } catch (error: IOException) {
            Log.w(TAG, "close $documentId: ${error.message}")
        }
    }

    private class EngineDocument(
        private val folder: String,
        private val path: String,
    ) : OpenDocument {
        override fun open(): Long = Native.open(folder, path, false)

        override fun read(token: Long, offset: Long, out: ByteArray, size: Int): Int =
            Native.read(folder, token, offset, out, size)

        override fun close(token: Long) = Native.close(folder, token)
    }

    companion object {
        private const val TAG = "tkfs"

        /** @throws IOException carrying the tkfs error code. */
        fun openForRead(storage: StorageManager, documentId: String, size: Long): ParcelFileDescriptor {
            val (folder, path) = DocumentIds.split(documentId)
            val reader = RecoveringReader(EngineDocument(folder, path)) { Log.i(TAG, "$documentId: $it") }
            val proxy = FileProxy(documentId, reader, size, HandlerThread("tkfs-read"))
            proxy.thread.start()
            try {
                return storage.openProxyFileDescriptor(
                    ParcelFileDescriptor.MODE_READ_ONLY,
                    proxy,
                    Handler(proxy.thread.looper),
                )
            } catch (error: IOException) {
                // No release will ever come for a descriptor that was not made.
                proxy.close()
                proxy.thread.quitSafely()
                throw error
            }
        }
    }
}
