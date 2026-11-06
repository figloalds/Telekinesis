package dev.tkfs.sync

import android.content.Context
import android.net.ConnectivityManager
import android.net.Uri
import android.provider.DocumentsContract
import android.util.Log
import androidx.work.Constraints
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.OutOfQuotaPolicy
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import dev.tkfs.Native
import dev.tkfs.Settings
import dev.tkfs.provider.TkfsProvider
import dev.tkfs.ui.Confirmed
import dev.tkfs.ui.parseConfirmed
import dev.tkfs.ui.parseExchanges
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.json.JSONException
import org.json.JSONObject
import java.io.IOException
import java.util.concurrent.TimeUnit

/** A burst that ran for `trigger`; `serial` tells two equal results apart. */
data class Finished(val serial: Long, val trigger: Trigger, val result: BurstResult)

/**
 * Every burst the app starts goes through here; nothing polls while idle. A trigger asks
 * `SyncPolicy` whether to run, and `Bursts` keeps it to one at a time.
 */
object SyncCoordinator {
    private val policy = SyncPolicy()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val bursts = Bursts(::runNative)
    private val done = MutableStateFlow<Finished?>(null)
    /** What `onLocalChange` and the transfer job agree on. */
    internal val gate = TransferGate()
    @Volatile private var lastListingBurstMs = 0L

    private val confirmed = MutableStateFlow<Map<String, Confirmed>>(emptyMap())

    val running: StateFlow<Boolean> get() = bursts.isRunning
    val finished: StateFlow<Finished?> get() = done

    /** What the PCs last said of each folder's waiting files, by folder id. */
    val confirmations: StateFlow<Map<String, Confirmed>> get() = confirmed

    fun onForeground(ctx: Context) = launch(ctx, Trigger.Foreground)

    fun syncNow(ctx: Context) = launch(ctx, Trigger.Manual)

    /**
     * A provider listing. It returns at once, so the listing is answered from what is known and
     * the burst runs behind it. A listing does not start another burst for 2 min, whatever the
     * first one found, so an unreachable PC is not asked at every listing.
     */
    fun onListing(ctx: Context, folder: String? = null) {
        val now = System.currentTimeMillis()
        val settings = Settings.of(ctx)
        val synced = if (folder == null) settings.lastSyncMs() else settings.lastSyncMs(folder)
        val last = maxOf(synced, lastListingBurstMs)
        val mode = policy.decide(Trigger.Listing, now, last, metered(ctx), settings.syncOnMobile)
        if (mode == Mode.Skip) return
        lastListingBurstMs = now
        val app = ctx.applicationContext
        scope.launch { runTrigger(app, Trigger.Listing, mode) }
    }

    /**
     * A file was written, created, renamed, deleted or shared in: the transfer job sends it. The
     * job is WorkManager's, not a timer here, because a cached app is frozen and a timer in it
     * would not run. The job waits out the 5 s debounce itself.
     */
    fun onLocalChange(ctx: Context) {
        val app = ctx.applicationContext
        if (!gate.changed(System.currentTimeMillis())) return
        try {
            enqueueTransfer(app, ExistingWorkPolicy.APPEND_OR_REPLACE)
        } catch (error: RuntimeException) {
            gate.notQueued()
            throw error
        }
    }

    /** "Sync on mobile data" changed: a job still waiting for a network takes the new requirement. */
    fun onMobileSettingChanged(ctx: Context) {
        if (gate.waiting()) enqueueTransfer(ctx.applicationContext, ExistingWorkPolicy.REPLACE)
    }

    private fun enqueueTransfer(app: Context, policy: ExistingWorkPolicy) {
        val network = if (Settings.of(app).syncOnMobile) NetworkType.CONNECTED else NetworkType.UNMETERED
        val request = OneTimeWorkRequestBuilder<TransferWorker>()
            .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
            .setConstraints(Constraints.Builder().setRequiredNetworkType(network).build())
            .build()
        WorkManager.getInstance(app).enqueueUniqueWork(TRANSFER, policy, request)
    }

    /** A full burst for the transfer job, which can stop waiting for it: the burst itself cannot be interrupted. */
    internal fun transferBurst(ctx: Context): Deferred<BurstResult> {
        val app = ctx.applicationContext
        return scope.async { runTrigger(app, Trigger.LocalChange, Mode.Full) }
    }

    /** One burst in `mode`, now, on this thread. */
    fun burst(ctx: Context, mode: Mode): BurstResult {
        val app = ctx.applicationContext
        val result = try {
            Native.ensureStarted(app)
            bursts.request(mode)
        } catch (error: IOException) {
            BurstResult(failure = error.message ?: "The burst failed.")
        }
        if (result.dropped) return result
        val settings = Settings.of(app)
        val now = System.currentTimeMillis()
        for (folder in result.folders) if (folder.error == null) settings.setLastSyncMs(folder.id, now)
        if (result.exchanged > 0) notifyChange(app)
        // After every burst, so a file leaves "waiting" as soon as the PC has it. Not after one a stop
        // ended: that network may be gone.
        if (!result.ended) confirm(result.folders.map { it.id })
        return result
    }

    /**
     * Asks the PCs which of each folder's waiting files they hold, on this thread. A folder that
     * fails keeps its files waiting: only a PC's answer clears one. True when every folder answered.
     */
    fun confirm(folders: Collection<String>): Boolean {
        var all = true
        for (id in folders) {
            try {
                val answer = parseConfirmed(Native.confirm(id))
                confirmed.update { it + (id to answer) }
                Log.i(TAG, "confirm $id: $answer")
            } catch (error: IOException) {
                all = false
                Log.i(TAG, "confirm $id: ${error.message}")
            } catch (error: JSONException) {
                all = false
                Log.i(TAG, "confirm $id: unreadable answer: ${error.message}")
            }
        }
        return all
    }

    /** The burst `trigger` calls for, on this thread. `decided` is a mode the caller already chose. */
    fun runTrigger(ctx: Context, trigger: Trigger, decided: Mode? = null): BurstResult {
        val app = ctx.applicationContext
        val settings = Settings.of(app)
        val mode = decided ?: policy.decide(
            trigger, System.currentTimeMillis(), settings.lastSyncMs(), metered(app), settings.syncOnMobile,
        )
        val result = burst(app, mode)
        if (!result.dropped) done.value = Finished((done.value?.serial ?: 0) + 1, trigger, result)
        if (mode == Mode.Full && (trigger == Trigger.Foreground || trigger == Trigger.Periodic)) sendWhatIsLeft(app, result)
        return result
    }

    /**
     * A burst that left work and moved something goes on as the transfer job, which a burst bounded
     * to the 185 s of `sync-now` cannot do. One that moved nothing is stuck, and the job would only
     * find that out again.
     */
    private fun sendWhatIsLeft(app: Context, result: BurstResult) {
        if (result.dropped || Progress().after(result) != Next.Again) return
        if (!gate.wanted()) return
        try {
            enqueueTransfer(app, ExistingWorkPolicy.APPEND_OR_REPLACE)
        } catch (error: RuntimeException) {
            gate.notQueued()
            throw error
        }
    }

    /** The transfer job ended with changes it did not cover. */
    internal fun resumeTransfer(ctx: Context, claimed: Long) {
        if (!gate.resume(claimed)) return
        try {
            enqueueTransfer(ctx.applicationContext, ExistingWorkPolicy.APPEND_OR_REPLACE)
        } catch (error: RuntimeException) {
            gate.notQueued()
            throw error
        }
    }

    /**
     * Ends every open burst now, so a stopped job leaves no bytes moving on a network it may no longer
     * use. At most the call in flight finishes.
     */
    internal fun endBursts() {
        try {
            Native.manage("end-bursts", "{}")
        } catch (error: IOException) {
            // The orchestrator is not there: no burst is either.
        }
    }

    /** Every 30 min by default and only with a network; `replace` after the interval changes. */
    fun schedulePeriodic(ctx: Context, replace: Boolean = false) {
        val minutes = Settings.of(ctx).periodMinutes.toLong()
        val request = PeriodicWorkRequestBuilder<BurstWorker>(minutes, TimeUnit.MINUTES)
            .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
            .build()
        WorkManager.getInstance(ctx.applicationContext).enqueueUniquePeriodicWork(
            PERIODIC,
            if (replace) ExistingPeriodicWorkPolicy.UPDATE else ExistingPeriodicWorkPolicy.KEEP,
            request,
        )
    }

    private fun launch(ctx: Context, trigger: Trigger) {
        val app = ctx.applicationContext
        scope.launch { runTrigger(app, trigger) }
    }

    private fun runNative(mode: Mode): BurstResult {
        val payload = JSONObject().put("metadata_only", mode == Mode.MetadataOnly)
        return try {
            val reply = Native.manage("sync-now", payload.toString())
            val first = BurstResult.parse(reply, emptyMap())
            if (first.errors.isEmpty()) first else {
                // Read after the burst, so an error goes only when a later exchange is newer.
                BurstResult.parse(reply, parseExchanges(Native.manage("status", "{}")))
            }
        } catch (error: IOException) {
            BurstResult(failure = error.message ?: "The burst failed.")
        }
    }

    /** What the policy sees: the same call that holds back files on a metered network. */
    internal fun metered(ctx: Context): Boolean =
        ctx.getSystemService(ConnectivityManager::class.java)?.isActiveNetworkMetered ?: false

    /** The roots and every directory listing are stale once a burst brought anything. */
    private fun notifyChange(ctx: Context) {
        val resolver = ctx.contentResolver
        resolver.notifyChange(DocumentsContract.buildRootsUri(TkfsProvider.AUTHORITY), null)
        // Observers of every URI below the authority hear it too: the documents and their children.
        resolver.notifyChange(Uri.parse("content://${TkfsProvider.AUTHORITY}"), null)
    }

    private const val TAG = "tkfs"
    private const val PERIODIC = "periodic-burst"
    const val TRANSFER = "transfer"
}
