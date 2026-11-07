package dev.tkfs.sync

import android.content.Context
import android.util.Log
import androidx.work.CoroutineWorker
import androidx.work.ForegroundInfo
import androidx.work.WorkerParameters
import dev.tkfs.Native
import dev.tkfs.Notifications
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.Dispatchers
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Sends what the owner just made. It repeats full bursts until nothing remains, it is stopped,
 * or a burst makes no progress. What is left waits for the next `onLocalChange`, foreground or
 * periodic trigger, so every end of this job is a success. With nothing left to send, it downloads
 * what the owner marked to keep, under the same network.
 *
 * The foreground notification is what matters most: a cached app is frozen and its network is
 * cut, and a job without it would not reach the PC. Android may refuse to start the service
 * (the app is in the background and not exempt from battery optimization, or `dataSync` has timed
 * out). The job then runs without it for as long as it is let, and loses nothing.
 */
class TransferWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun getForegroundInfo(): ForegroundInfo = Notifications.transfer(applicationContext, "Starting")

    /** The changes the last burst began after: what it covers. */
    @Volatile private var claimed = 0L

    override suspend fun doWork(): Result {
        val gate = SyncCoordinator.gate
        gate.started()
        claimed = gate.claim()
        var stopped = false
        try {
            becomeForeground("Starting")
            withContext(Dispatchers.IO) { if (send(gate)) download() }
        } catch (stop: CancellationException) {
            // A stop (constraints, quota, `onTimeout`). The rest is for the next trigger.
            stopped = true
            Log.i(TAG, "transfer: stopped (${stopReason})")
        } finally {
            gate.stopped()
        }
        // Ended by itself, with a change that came while it ran: nothing else would send it.
        if (!stopped) SyncCoordinator.resumeTransfer(applicationContext, claimed)
        return Result.success()
    }

    /** What is kept and not all here. A stop ends it between steps, and what came stays. */
    private suspend fun download() {
        val missing = try {
            Native.keptMissing()
        } catch (error: IOException) {
            Log.i(TAG, "transfer: ${error.message}")
            false
        }
        if (!missing) return
        becomeForeground("Downloading kept files")
        val stop = AtomicBoolean(false)
        val hydrating = SyncCoordinator.hydrateKept(applicationContext, stop)
        try {
            hydrating.await()
        } catch (cancelled: CancellationException) {
            stop.set(true)
            throw cancelled
        }
    }

    /** Whether it ended with nothing left to send. */
    private suspend fun send(gate: TransferGate): Boolean {
        try {
            Native.ensureStarted(applicationContext)
        } catch (error: IOException) {
            Log.i(TAG, "transfer: the orchestrator did not start: ${error.message}")
            return false
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
            claimed = gate.claim()
            val burst = SyncCoordinator.transferBurst(applicationContext)
            val result = try {
                burst.await()
            } catch (stop: CancellationException) {
                // The burst is not interruptible. Ending it now keeps it from moving bytes, perhaps on
                // a metered network, until its own bound.
                withContext(NonCancellable) { SyncCoordinator.endBursts() }
                throw stop
            }
            if (result.dropped) {
                // Another burst is on its way to the PC; ask again once it is done.
                if (++dropped > MAX_DROPPED) return false
                delay(DROPPED_WAIT_MS)
                continue
            }
            val next = progress.after(result)
            Log.i(TAG, "transfer: burst pushed=${result.folders.sumOf { it.pushed }} " +
                "remaining=${result.folders.map { it.remaining }} -> $next")
            when {
                next is Next.Stop && next.why != Why.Done -> return false
                next is Next.Again -> becomeForeground("Still sending")
                // Nothing remains, but only a burst that began after the last change says so.
                gate.finish(claimed) -> return true
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
