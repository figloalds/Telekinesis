package dev.tkfs.spike

import android.content.Context
import java.io.File
import java.io.IOException

/** The Rust side. Failures arrive as IOException carrying the tkfs error code. */
object Native {
    init {
        System.loadLibrary("tkfs_android")
    }

    @JvmStatic external fun start(config: String)

    @JvmStatic @Throws(IOException::class)
    external fun manage(action: String, payload: String): String

    @JvmStatic @Throws(IOException::class)
    external fun list(folder: String, path: String): String

    @JvmStatic @Throws(IOException::class)
    external fun stat(folder: String, path: String): String

    @JvmStatic @Throws(IOException::class)
    external fun open(folder: String, path: String): Long

    @JvmStatic @Throws(IOException::class)
    external fun read(folder: String, token: Long, offset: Long, out: ByteArray, size: Int): Int

    @JvmStatic @Throws(IOException::class)
    external fun close(folder: String, token: Long)

    /** Writes the configuration on first run and starts the orchestrator once. */
    fun ensureStarted(context: Context) {
        val files = context.filesDir
        val config = File(files, "orchestrator.toml")
        if (!config.exists()) {
            // listen = "": the phone only dials its peers.
            config.writeText(
                """
                format_version = 2
                data_directory = "${File(files, "data").absolutePath}"
                default_mount_directory = "${File(files, "mounts").absolutePath}"
                maximum_running = 8

                [network]
                listen = ""
                sync_profile = "passive"
                advertise = ""
                connections = 4
                worker_lanes = 2
                sync_interval_ms = 500

                [freshness]
                open_wait_ms = 250
                """.trimIndent() + "\n",
            )
        }
        start(config.absolutePath)
    }
}
