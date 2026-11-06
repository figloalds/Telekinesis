package dev.tkfs.share

import android.app.Service
import android.content.ClipData
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.Uri
import android.os.IBinder
import android.provider.OpenableColumns
import android.util.Log
import androidx.core.app.ServiceCompat
import dev.tkfs.Native
import dev.tkfs.Notifications
import dev.tkfs.sync.SyncCoordinator
import dev.tkfs.ui.errorInWords
import java.io.IOException
import java.util.UUID
import java.util.concurrent.Executors

/**
 * Copies what was shared into a folder, then asks for the transfer job. This is a foreground
 * service, not WorkManager work, because the read grant on the shared URIs travels with the
 * intent that starts a service and lasts as long as it runs.
 *
 * Each file takes its name only once it is whole (see [Importer]). One that cannot be copied is
 * deleted and named in the notification that ends the share.
 */
class ShareImportService : Service() {
    private val worker = Executors.newSingleThreadExecutor()
    private val lock = Any()
    private var pending = 0
    @Volatile private var stopped = false

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // Within seconds of the start. A refusal (the `dataSync` budget is spent, or Android will not
        // have it from here) must not lose the share: the copy goes on without the service.
        try {
            ServiceCompat.startForeground(
                this, Notifications.SHARE_ID, Notifications.sharing(this, "Starting", 0, 0),
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC,
            )
        } catch (error: IllegalStateException) {
            Log.w(TAG, "share: no foreground: ${error.javaClass.simpleName}: ${error.message}")
        } catch (error: SecurityException) {
            Log.w(TAG, "share: no foreground: ${error.javaClass.simpleName}: ${error.message}")
        }
        val folder = intent?.getStringExtra(FOLDER)
        val directory = intent?.getStringExtra(DIRECTORY) ?: "/"
        val label = intent?.getStringExtra(LABEL) ?: "Telekinesis"
        val uris = intent?.clipData?.let(::uris).orEmpty()
        if (folder == null || uris.isEmpty()) {
            finishIfIdle(extra = 0)
            return START_NOT_STICKY
        }
        // Written down before anything else, so that a kill from here on is reported, not silent.
        val id = UUID.randomUUID().toString()
        val names = uris.map { displayNameOf(contentResolver, it) }
        ShareImports.begin(id)
        ShareLedger.of(this).begin(id, folder, names)
        synchronized(lock) { pending++ }
        worker.execute {
            try {
                copy(id, folder, directory, label, uris, names)
            } catch (error: Throwable) {
                // A copy that cannot even run (a dead worker, a bug) still says so.
                Log.w(TAG, "share: ${error.javaClass.simpleName}: ${error.message}")
                ShareLedger.of(this).finish(id, failed = names)
            } finally {
                ShareImports.end(id)
                finishIfIdle(extra = 1)
            }
        }
        return START_NOT_STICKY
    }

    /** Android 15 limits `dataSync` to 6 h in 24: the service must stop at once, not be killed. */
    override fun onTimeout(startId: Int, fgsType: Int) {
        Log.i(TAG, "share: dataSync timed out; stopping")
        stopped = true
        stopSelf()
    }

    override fun onDestroy() {
        stopped = true
        worker.shutdown()
        super.onDestroy()
    }

    private fun finishIfIdle(extra: Int) {
        val idle = synchronized(lock) {
            pending -= extra
            pending <= 0
        }
        if (idle) {
            ServiceCompat.stopForeground(this, ServiceCompat.STOP_FOREGROUND_REMOVE)
            stopSelf()
        }
    }

    private fun copy(id: String, folder: String, directory: String, label: String, uris: List<Uri>, names: List<String>) {
        val ledger = ShareLedger.of(this)
        // What an earlier, killed process left is reported and swept first.
        ShareImports.recover(this)
        val importer = Importer(
            EngineFolder(folder),
            working = { ledger.working(id, it) },
            cleared = { ledger.cleared(id, it) },
        )
        val sizes = uris.map { size(it) }
        val grand = if (sizes.all { it >= 0 }) sizes.sum() else 0L
        var finished = 0L
        var saved = 0
        val failed = mutableListOf<Pair<String, String>>()
        for ((index, uri) in uris.withIndex()) {
            val name = names[index]
            if (stopped) {
                failed += name to "Android stopped saving."
                ledger.done(id, name)
                continue
            }
            var last = 0L
            try {
                Native.ensureStarted(this)
                val input = contentResolver.openInputStream(uri) ?: throw IOException("could not open it")
                input.use {
                    importer.import(directory, name, it) { done ->
                        if (stopped) throw IOException("Android stopped saving.")
                        val now = System.currentTimeMillis()
                        if (now - last >= PROGRESS_EVERY_MS) {
                            last = now
                            progress("$name (${index + 1} of ${uris.size})", finished + done, grand)
                        }
                    }
                }
                saved++
            } catch (error: IOException) {
                Log.w(TAG, "share: $name: ${error.message}")
                failed += name to errorInWords(error.message ?: "the copy failed")
            } catch (error: SecurityException) {
                // The sender took its grant back, or never gave one.
                Log.w(TAG, "share: $name: ${error.message}")
                failed += name to "the sender no longer allows it."
            }
            ledger.done(id, name)
            finished += maxOf(0, sizes[index])
        }
        // The names go to the Folders screen, which does not depend on the notification permission.
        ledger.finish(id, failed = failed.map { it.first })
        if (saved > 0) SyncCoordinator.onLocalChange(this)
        val (title, text) = resultLines(saved, failed, label)
        Notifications.shared(this, title, text)
    }

    private fun progress(text: String, done: Long, total: Long) {
        getSystemService(android.app.NotificationManager::class.java)
            .notify(Notifications.SHARE_ID, Notifications.sharing(this, text, done, total))
    }

    private fun size(uri: Uri): Long = query(uri, OpenableColumns.SIZE)?.toLongOrNull() ?: -1

    private fun query(uri: Uri, column: String): String? = try {
        contentResolver.query(uri, arrayOf(column), null, null, null)?.use { cursor ->
            if (cursor.moveToFirst() && !cursor.isNull(0)) cursor.getString(0) else null
        }
    } catch (error: RuntimeException) {
        null
    }

    companion object {
        private const val TAG = "tkfs"
        private const val FOLDER = "folder"
        private const val DIRECTORY = "directory"
        private const val LABEL = "label"
        private const val PROGRESS_EVERY_MS = 500L

        /**
         * Starts the copy. The URIs go in `ClipData` with the read grant flag, so the grant lasts as
         * long as the service does.
         */
        fun start(context: Context, folder: String, label: String, directory: String, uris: List<Uri>) {
            val clip = ClipData.newRawUri("shared", uris.first())
            for (uri in uris.drop(1)) clip.addItem(ClipData.Item(uri))
            val intent = Intent(context, ShareImportService::class.java)
                .putExtra(FOLDER, folder)
                .putExtra(LABEL, label)
                .putExtra(DIRECTORY, directory)
            intent.clipData = clip
            intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            context.startForegroundService(intent)
        }

        private fun uris(clip: ClipData): List<Uri> = (0 until clip.itemCount).mapNotNull { clip.getItemAt(it).uri }
    }
}
