package dev.tkfs

import android.content.Context
import java.io.File
import java.io.IOException

/** The Rust side. Failures arrive as IOException carrying the tkfs error code. */
object Native {
    init {
        System.loadLibrary("tkfs_android")
    }

    @JvmStatic @Throws(IOException::class)
    external fun start(config: String)

    /** One orchestrator action, as `Daemon::execute` takes it. */
    @JvmStatic @Throws(IOException::class)
    external fun manage(action: String, payload: String): String

    /** The directory's entries: `[{name, directory, size, modified_ms}]`. */
    @JvmStatic @Throws(IOException::class)
    external fun list(folder: String, path: String): String

    @JvmStatic @Throws(IOException::class)
    external fun stat(folder: String, path: String): String

    @JvmStatic @Throws(IOException::class)
    external fun open(folder: String, path: String, writable: Boolean): Long

    @JvmStatic @Throws(IOException::class)
    external fun read(folder: String, token: Long, offset: Long, out: ByteArray, size: Int): Int

    /**
     * A handle for writing, whose next call is `truncate(folder, token, 0)`. With the old contents
     * going, none of them is asked for, so this works on a file the phone never downloaded and
     * with no PC to ask.
     */
    @JvmStatic @Throws(IOException::class)
    external fun openToReplace(folder: String, path: String): Long

    /** Writes `size` bytes of `data` at `offset`, or at the end if `append`; how many it wrote. */
    @JvmStatic @Throws(IOException::class)
    external fun write(folder: String, token: Long, offset: Long, data: ByteArray, size: Int, append: Boolean): Int

    /** What the handle sees as the file's length now. */
    @JvmStatic @Throws(IOException::class)
    external fun length(folder: String, token: Long): Long

    @JvmStatic @Throws(IOException::class)
    external fun truncate(folder: String, token: Long, size: Long)

    /** Makes everything written through the handle durable: `Engine::flush(Some(token))`. */
    @JvmStatic @Throws(IOException::class)
    external fun sync(folder: String, token: Long)

    @JvmStatic @Throws(IOException::class)
    external fun close(folder: String, token: Long)

    /** Each of these is durable when it returns: the app may be frozen or killed right after. */
    @JvmStatic @Throws(IOException::class)
    external fun create(folder: String, path: String, directory: Boolean)

    /** Renames or moves within the folder. Downloads nothing, and fails if `to` exists. */
    @JvmStatic @Throws(IOException::class)
    external fun rename(folder: String, from: String, to: String)

    /** Deletes a file or a directory with all it holds. Downloads nothing. */
    @JvmStatic @Throws(IOException::class)
    external fun delete(folder: String, path: String)

    /** Always throws: a panic with a Java exception pending. See `ShellControl`. */
    @JvmStatic @Throws(IOException::class)
    external fun faultForTest()

    /**
     * Every way into the process calls this. It writes the configuration on first run,
     * starts the orchestrator once and returns at once; native calls wait, bounded, for what
     * they need.
     */
    fun ensureStarted(context: Context) {
        val files = context.filesDir
        val config = File(files, "orchestrator.toml")
        if (!config.exists()) {
            // A kill halfway through writing must not leave a config that cannot start.
            val draft = File(files, "orchestrator.toml.tmp")
            draft.writeText(
                """
                format_version = 2
                data_directory = "${File(files, "data").absolutePath}"
                default_mount_directory = "${File(files, "mounts").absolutePath}"
                maximum_running = 8

                [network]
                listen = ""
                advertise = ""
                sync_profile = "passive"
                connections = 4
                worker_lanes = 2
                """.trimIndent() + "\n",
            )
            if (!draft.renameTo(config)) throw IOException("CONFIG_WRITE_FAILED: ${config.path}")
        }
        start(config.absolutePath)
    }
}
