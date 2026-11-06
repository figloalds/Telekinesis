package dev.tkfs.share

import dev.tkfs.Native
import dev.tkfs.provider.DocumentIds
import dev.tkfs.provider.numberedName
import org.json.JSONArray
import java.io.IOException
import java.io.InputStream
import java.util.UUID

/** What an import needs of one folder: the engine's calls with the folder bound. */
internal interface FolderFiles {
    fun names(directory: String): Set<String>
    fun create(path: String)
    fun open(path: String): Long
    fun write(token: Long, offset: Long, data: ByteArray, size: Int): Int
    fun sync(token: Long)
    fun close(token: Long)
    fun rename(from: String, to: String)
    fun delete(path: String)
}

internal class EngineFolder(private val folder: String) : FolderFiles {
    override fun names(directory: String): Set<String> {
        val entries = JSONArray(Native.list(folder, directory))
        return (0 until entries.length()).mapTo(HashSet()) { entries.getJSONObject(it).getString("name") }
    }

    override fun create(path: String) = Native.create(folder, path, false)
    override fun open(path: String): Long = Native.open(folder, path, true)
    override fun write(token: Long, offset: Long, data: ByteArray, size: Int): Int =
        Native.write(folder, token, offset, data, size, false)

    override fun sync(token: Long) = Native.sync(folder, token)
    override fun close(token: Long) = Native.close(folder, token)
    override fun rename(from: String, to: String) = Native.rename(folder, from, to)
    override fun delete(path: String) = Native.delete(folder, path)
}

/** Why one stream could not be saved, in words for the notification. */
class ImportFailed(message: String, cause: Throwable) : IOException(message, cause)

/**
 * Copies a stream into a folder so that a name in the folder always means a whole file. The
 * bytes go to a working name, are sealed there, and only then take their real name by a
 * rename, which is durable. A stream that fails partway, or a process killed partway, leaves a
 * working name and never a half file under the real one; a failure deletes the working file.
 */
internal class Importer(
    private val files: FolderFiles,
    private val bufferSize: Int = 256 * 1024,
    /** Told of a working file before it is created, so a kill cannot hide it. */
    private val working: (String) -> Unit = {},
    /** Told when a working file is gone or has its real name, and not when its removal failed. */
    private val cleared: (String) -> Unit = {},
) {
    /** The path it was saved as. `progress` gets the bytes copied so far. */
    fun import(directory: String, name: String, input: InputStream, progress: (Long) -> Unit): String {
        val part = DocumentIds.childPath(directory, PART_PREFIX + UUID.randomUUID())
        working(part)
        try {
            try {
                files.create(part)
            } catch (error: IOException) {
                // It never existed, so there is nothing to delete and nothing for a sweep to find.
                cleared(part)
                throw error
            }
            copy(part, input, progress)
            return rename(part, directory, name).also { cleared(part) }
        } catch (error: Exception) {
            try {
                files.delete(part)
                cleared(part)
            } catch (cleanup: IOException) {
                // Not there is as good as deleted; any other failure leaves it for the sweep.
                if (isNotFound(cleanup)) cleared(part) else error.addSuppressed(cleanup)
            }
            throw error as? ImportFailed ?: ImportFailed(error.message ?: error.javaClass.simpleName, error)
        }
    }

    private fun copy(path: String, input: InputStream, progress: (Long) -> Unit) {
        val token = files.open(path)
        try {
            val buffer = ByteArray(bufferSize)
            var offset = 0L
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                var done = 0
                while (done < read) {
                    val wrote = if (done == 0) {
                        files.write(token, offset, buffer, read)
                    } else {
                        files.write(token, offset + done, buffer.copyOfRange(done, read), read - done)
                    }
                    if (wrote <= 0) throw IOException("UNKNOWN_ERROR: a write took no bytes")
                    done += wrote
                }
                offset += read
                progress(offset)
            }
            files.sync(token)
        } finally {
            try {
                files.close(token)
            } catch (error: IOException) {
                // The copy failed or was sealed already; a close that fails here changes neither.
            }
        }
    }

    /** A rename fails if the name is taken, which a burst can do between the check and here. */
    private fun rename(working: String, directory: String, name: String): String {
        var attempts = 0
        while (true) {
            val path = DocumentIds.childPath(directory, numberedName(files.names(directory), name))
            try {
                files.rename(working, path)
                return path
            } catch (error: IOException) {
                val clash = error.message?.substringBefore(':') == "ALREADY_EXISTS"
                if (!clash || ++attempts == RENAME_ATTEMPTS) throw error
            }
        }
    }

    private companion object {
        const val RENAME_ATTEMPTS = 3
    }
}

/** The start of a working file's name. The provider does not list these, and a sweep removes what a kill left. */
internal const val PART_PREFIX = ".tkfs-part-"

/** The engine says a path is not there with this code. */
internal fun isNotFound(error: IOException): Boolean = error.message?.substringBefore(':') in setOf("NOT_FOUND", "FILE_NOT_FOUND")
