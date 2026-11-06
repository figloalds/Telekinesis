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

/**
 * One open document as a file descriptor. Its callbacks all run on the thread it owns, one
 * per open, so a read waiting on the network does not stall any other.
 */
internal class FileProxy private constructor(
    private val documentId: String,
    private val folder: String,
    private val path: String,
    private val size: Long,
    private val thread: HandlerThread,
) : ProxyFileDescriptorCallback() {
    private var token: Long = Native.open(folder, path, false)

    override fun onGetSize(): Long = size

    override fun onRead(offset: Long, count: Int, data: ByteArray): Int = try {
        try {
            Native.read(folder, token, offset, data, count)
        } catch (error: IOException) {
            // Bytes unavailable: retrying would only wait again.
            if (error.message?.contains("OBJECT_MISSING") == true) throw error
            // Android can kill a worker at any time, and its successor knows no old handle.
            Log.i(TAG, "reopen $documentId: ${error.message}")
            token = Native.open(folder, path, false)
            Native.read(folder, token, offset, data, count)
        }
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
            Native.close(folder, token)
        } catch (error: IOException) {
            Log.w(TAG, "close $documentId: ${error.message}")
        }
    }

    companion object {
        private const val TAG = "tkfs"

        /** @throws IOException carrying the tkfs error code. */
        fun openForRead(storage: StorageManager, documentId: String, size: Long): ParcelFileDescriptor {
            val (folder, path) = DocumentIds.split(documentId)
            val proxy = FileProxy(documentId, folder, path, size, HandlerThread("tkfs-read"))
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
