package dev.tkfs.sync

import android.content.Context
import android.util.Log
import androidx.work.CoroutineWorker
import androidx.work.ForegroundInfo
import androidx.work.WorkerParameters
import dev.tkfs.Native
import dev.tkfs.Notifications
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.Dispatchers
import java.io.IOException

/**
 * Sends what the owner just made. It repeats full bursts until nothing remains, it is stopped,
 * or a burst makes no progress. What is left waits for the next `onLocalChange`, foreground or
 * periodic trigger, so every end of this job is a success.
 *
 * The foreground notification is what matters most: a cached app is frozen and its network is
 * cut, and a job without it would not reach the PC. Android may refuse to start the service
 * (the app is in the background and not exempt from battery optimization, or `dataSync` has timed
 * out). The job then runs without it for as long as it is let, and loses nothing.
 */
class TransferWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun getForegroundInfo(): ForegroundInfo = Notifications.transfer(applicationContext, "Starting")

    override suspend fun doWork(): Result {
        val gate = SyncCoordinator.gate
        gate.started()
        try {
            becomeForeground("Starting")
            withContext(Dispatchers.IO) { send(gate) }
        } catch (stop: CancellationException) {
            // A stop (constraints, quota, `onTimeout`). The rest is for the next trigger.
            Log.i(TAG, "transfer: stopped (${stopReason})")
        } finally {
            gate.stopped()
        }
        return Result.success()
    }

    private suspend fun send(gate: TransferGate) {
        try {
            Native.ensureStarted(applicationContext)
        } catch (error: IOException) {
            Log.i(TAG, "transfer: the orchestrator did not start: ${error.message}")
            return
        }
        // A burst the owner's change asked for, 5 s after the last of them.
        while (true) {
            val wait = gate.quietFor(System.currentTimeMillis())
            if (wait == 0L) break
            delay(wait)
        }
        val progress = Progress()
        var dropped = 0
        while (true) {
            val claimed = gate.claim()
            val result = SyncCoordinator.transferBurst(applicationContext).await()
            if (result.dropped) {
                // Another burst is on its way to the PC; ask again once it is done.
                if (++dropped > MAX_DROPPED) return
                delay(DROPPED_WAIT_MS)
                continue
            }
            val next = progress.after(result)
            Log.i(TAG, "transfer: burst pushed=${result.folders.sumOf { it.pushed }} " +
                "remaining=${result.folders.map { it.remaining }} -> $next")
            when {
                next is Next.Stop && next.why != Why.Done -> return
                next is Next.Again -> becomeForeground("Still sending")
                // Nothing remains, but only a burst that began after the last change says so.
                gate.finish(claimed) -> return
            }
        }
    }

    /** Tries each time, so a notification the system dropped comes back. A refusal is not an error. */
    private suspend fun becomeForeground(text: String) {
        try {
            setForeground(Notifications.transfer(applicationContext, text))
            changed(true, "foreground")
        } catch (error: IllegalStateException) {
            // `ForegroundServiceStartNotAllowedException` is one, from Android 12.
            changed(false, "no foreground: ${error.javaClass.simpleName}: ${error.message}")
        } catch (error: SecurityException) {
            changed(false, "no foreground: ${error.javaClass.simpleName}: ${error.message}")
        }
    }

    private var foreground: Boolean? = null

    /** Logs only when it changes, since the notification is refreshed after every burst. */
    private fun changed(now: Boolean, line: String) {
        if (foreground == now) return
        foreground = now
        Log.i(TAG, "transfer: $line")
    }

    private companion object {
        const val TAG = "tkfs"
        const val MAX_DROPPED = 30
        const val DROPPED_WAIT_MS = 2_000L
    }
}
