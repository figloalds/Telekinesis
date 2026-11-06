package dev.tkfs.sync

import android.content.Context
import android.net.ConnectivityManager
import android.net.Uri
import android.provider.DocumentsContract
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
import dev.tkfs.ui.parseExchanges
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
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

    val running: StateFlow<Boolean> get() = bursts.isRunning
    val finished: StateFlow<Finished?> get() = done

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
        return result
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
        return result
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

    private fun metered(ctx: Context): Boolean =
        ctx.getSystemService(ConnectivityManager::class.java)?.isActiveNetworkMetered ?: false

    /** The roots and every directory listing are stale once a burst brought anything. */
    private fun notifyChange(ctx: Context) {
        val resolver = ctx.contentResolver
        resolver.notifyChange(DocumentsContract.buildRootsUri(TkfsProvider.AUTHORITY), null)
        // Observers of every URI below the authority hear it too: the documents and their children.
        resolver.notifyChange(Uri.parse("content://${TkfsProvider.AUTHORITY}"), null)
    }

    private const val PERIODIC = "periodic-burst"
    const val TRANSFER = "transfer"
}
