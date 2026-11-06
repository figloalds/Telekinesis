package dev.tkfs.sync

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import dev.tkfs.Native
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException

/** The periodic job: one burst, bounded by the native `SYNC_NOW_BOUND`. */
class BurstWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    /** Success whatever the burst reports: the next trigger retries, and a backoff would only repeat it. */
    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        // WorkManager can start the process, so the orchestrator may not be up yet.
        try {
            Native.ensureStarted(applicationContext)
            SyncCoordinator.runTrigger(applicationContext, Trigger.Periodic)
        } catch (error: IOException) {
            // The next trigger starts it again.
        }
        Result.success()
    }
}
