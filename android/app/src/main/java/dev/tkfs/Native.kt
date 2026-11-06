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

    @JvmStatic @Throws(IOException::class)
    external fun close(folder: String, token: Long)

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
