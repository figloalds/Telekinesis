package dev.tkfs.sync

import android.content.Context
import android.net.ConnectivityManager
import android.net.Uri
import android.os.StatFs
import android.provider.DocumentsContract
import android.util.Log
import androidx.work.Constraints
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.OutOfQuotaPolicy
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkInfo
import androidx.work.WorkManager
import dev.tkfs.Native
import dev.tkfs.Settings
import dev.tkfs.provider.TkfsProvider
import dev.tkfs.ui.Confirmed
import dev.tkfs.ui.parseConfirmed
import dev.tkfs.ui.parseExchanges
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.json.JSONException
import org.json.JSONObject
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BooleanSupplier

/** A burst that ran for `trigger`; `serial` tells two equal results apart. */
data class Finished(val serial: Long, val trigger: Trigger, val result: BurstResult)

/**
 * Every burst the app starts goes through here; nothing polls while idle. A trigger asks
 * `SyncPolicy` whether to run, and `Bursts` keeps it to one at a time.
 */
object SyncCoordinator {
    private val policy = SyncPolicy()
    private val foregroundAttempts = SyncPolicy.Attempts(policy)
    private val scope = backgroundScope { Log.e(TAG, "sync: ${it.javaClass.simpleName}: ${it.message}", it) }
    private val bursts = Bursts(::runNative)
    private val done = MutableStateFlow<Finished?>(null)
    /** What `onLocalChange` and the transfer job agree on. */
    internal val gate = TransferGate()
    @Volatile private var lastListingBurstMs = 0L

    private val confirmed = MutableStateFlow<Map<String, Confirmed>>(emptyMap())

    private val lowSpace = MutableStateFlow<String?>(null)
    private val belowFloor = MutableStateFlow(false)
    private val keptShort = MutableStateFlow(false)
    private val freeing = Any()

    private val network = NetworkWatch { app, metered ->
        if (holdsFilesBack(metered, Settings.of(app).syncOnMobile)) scope.launch { stopMovingFiles(app) }
    }
    private val appStarted = AtomicBoolean(false)

    val running: StateFlow<Boolean> get() = bursts.isRunning
    val finished: StateFlow<Finished?> get() = done

    /** What the PCs last said of each folder's waiting files, by folder id. */
    val confirmations: StateFlow<Map<String, Confirmed>> get() = confirmed

    /** Why free space stays below the floor, when only a PC could change that: `PC_UNREACHABLE`, `PC_TOO_OLD`, or null. */
    val spaceBlocked: StateFlow<String?> get() = lowSpace

    /** Free space stayed below the floor after freeing, for whatever reason. */
    val stillBelowFloor: StateFlow<Boolean> get() = belowFloor

    /** Something marked to keep is not all here, and there is no room for it above the floor. */
    val keptDoesNotFit: StateFlow<Boolean> get() = keptShort

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

    /**
     * A mark changed, or a burst brought files into a marked folder: the transfer job downloads what
     * is kept. Below the floor it would stop as soon as it began, so no job is asked for until a burst
     * has found room, and the Folders screen says why.
     */
    fun onKeptChanged(ctx: Context) {
        val app = ctx.applicationContext
        if (!keptMissing()) return
        val short = spaceNeededNow(app) > 0
        keptShort.value = short
        if (!short) wantTransfer(app)
    }

    /** Whether anything kept is not all here. If nothing is, nothing is short of room either. */
    internal fun keptMissing(): Boolean {
        val missing = try {
            Native.keptMissing()
        } catch (error: IOException) {
            Log.i(TAG, "kept: ${error.message}")
            false
        }
        if (!missing) keptShort.value = false
        return missing
    }

    /** Asks for the transfer job with no change to send, unless it is on its way. */
    private fun wantTransfer(app: Context) {
        if (!gate.wanted()) return
        try {
            enqueueTransfer(app, ExistingWorkPolicy.APPEND_OR_REPLACE)
        } catch (error: RuntimeException) {
            gate.notQueued()
            throw error
        }
    }

    /**
     * Downloads what is kept for the transfer job, under its network, until `stop` says so. It is not
     * interruptible, so the job can stop waiting for it, and `stop` ends it between steps.
     */
    internal fun hydrateKept(ctx: Context, stop: () -> Boolean): Deferred<Unit> {
        val app = ctx.applicationContext
        return scope.async {
            val ended = downloadKept(
                hydrate = ::hydrateOnce,
                missing = ::keptMissing,
                belowFloor = { spaceNeededNow(app) > 0 },
                freeUp = { free(app, unasked = true) },
                stop = stop,
            )
            Log.i(TAG, "kept: $ended")
            if (ended != Kept.Stopped) keptShort.value = ended == Kept.DoesNotFit
        }
    }

    /** What came stays whatever comes after it, and the next round asks again for the rest. */
    private fun hydrateOnce(cancel: () -> Boolean): Long = try {
        Native.hydrateKept(BooleanSupplier { cancel() }).also { Log.i(TAG, "kept: brought $it bytes") }
    } catch (error: IOException) {
        Log.i(TAG, "kept: ${error.message}")
        0
    }

    /**
     * Frees space if the volume is below the floor, leaving what was opened lately. It is a look at the
     * volume and nothing else above it, so it can follow every burst. Below the floor it does more than
     * look only when [floorCheckDue] says so.
     */
    internal fun checkFloor(ctx: Context) {
        val app = ctx.applicationContext
        val settings = Settings.of(app)
        if (spaceNeededNow(app) == 0L) {
            lowSpace.value = null
            belowFloor.value = false
            settings.floorChecked = null
            return
        }
        val now = System.currentTimeMillis()
        if (floorCheckDue(settings.floorChecked, now, StatFs(app.filesDir.path).availableBytes)) {
            free(app, unasked = true)
            val after = StatFs(app.filesDir.path)
            val stillLow = spaceNeeded(after.availableBytes, after.totalBytes) > 0
            settings.floorChecked = if (stillLow) FloorChecked(now, after.availableBytes) else null
        } else {
            belowFloor.value = true
        }
    }

    /** "Free up space now", on this thread: up to the floor, or if it is met only what nothing needs. Null if it could not ask. */
    fun freeUp(ctx: Context): Freed? = free(ctx.applicationContext, unasked = false)

    private fun free(app: Context, unasked: Boolean): Freed? =
        synchronized(freeing) {
            val freed = try {
                Native.ensureStarted(app)
                parseFreed(Native.freeSpace(spaceNeededNow(app), unasked))
            } catch (error: IOException) {
                Log.i(TAG, "free: ${error.message}")
                return@synchronized null
            } catch (error: JSONException) {
                Log.i(TAG, "free: unreadable answer: ${error.message}")
                return@synchronized null
            }
            val stillLow = spaceNeededNow(app) > 0
            lowSpace.value = if (stillLow) freed.blocked else null
            belowFloor.value = stillLow
            Log.i(TAG, "free: ${freed.bytes} bytes, blocked ${freed.blocked}, still low: $stillLow, unasked: $unasked")
            freed
        }

    /** How far the volume is below the floor. */
    fun spaceNeededNow(ctx: Context): Long {
        val volume = StatFs(ctx.filesDir.path)
        return spaceNeeded(volume.availableBytes, volume.totalBytes)
    }

    /**
     * "Sync on mobile data" changed: a job there is, running or waiting for a network, takes the new
     * requirement. Turned off on a metered network, nothing goes on moving files either.
     */
    fun onMobileSettingChanged(ctx: Context) {
        val app = ctx.applicationContext
        scope.launch {
            if (holdsFilesBack(metered(app), Settings.of(app).syncOnMobile)) stopMovingFiles(app)
            requeueTransfer(app) { !it.isFinished }
        }
    }

    /**
     * Once a process, when the app starts. A job waiting for a network keeps the requirement it was
     * queued with, and the setting may have changed since: nothing in memory says so after a restart.
     */
    fun onAppStart(ctx: Context) {
        if (!appStarted.compareAndSet(false, true)) return
        val app = ctx.applicationContext
        scope.launch { requeueTransfer(app) { it == WorkInfo.State.ENQUEUED || it == WorkInfo.State.BLOCKED } }
    }

    /** Queues the job again with the requirement it should have now, if it has a state that `needs` that. Blocking. */
    private fun requeueTransfer(app: Context, needs: (WorkInfo.State) -> Boolean) {
        val jobs = WorkManager.getInstance(app).getWorkInfosForUniqueWork(TRANSFER).get()
        if (jobs.none { needs(it.state) }) return
        gate.wanted()
        enqueueTransfer(app, ExistingWorkPolicy.REPLACE)
    }

    /**
     * This network must not carry files, with no one having asked for them to move. What waits does
     * not start, what runs ends, and a running job goes back to waiting for a network it may use.
     */
    private fun stopMovingFiles(app: Context) {
        Log.i(TAG, "network: held back, so files stop moving")
        if (bursts.holdBack()) endBursts()
        if (gate.working()) requeueTransfer(app) { !it.isFinished }
    }

    /** While it runs, the transfer job has the network watched, with or without a burst. */
    internal fun watchNetwork(app: Context) = network.hold(app.applicationContext)

    internal fun stopWatchingNetwork(app: Context) = network.release(app.applicationContext)

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

    /**
     * One burst in `mode`, now, on this thread. `asked` is the owner asking, which goes on whatever
     * the network: otherwise a network that will not carry files gets no full burst, and one it stops
     * being able to carry is ended.
     */
    fun burst(ctx: Context, mode: Mode, asked: Boolean = false): BurstResult {
        val app = ctx.applicationContext
        val settings = Settings.of(app)
        if (mode == Mode.Full && !asked && holdsFilesBack(metered(app), settings.syncOnMobile)) return BurstResult.DROPPED
        network.hold(app)
        val result = try {
            Native.ensureStarted(app)
            bursts.request(mode, asked)
        } catch (error: IOException) {
            BurstResult(failure = error.message ?: "The burst failed.")
        } finally {
            network.release(app)
        }
        if (result.dropped) return result
        val now = System.currentTimeMillis()
        for (folder in result.folders) if (folder.error == null) settings.setLastSyncMs(folder.id, now)
        if (result.exchanged > 0) notifyChange(app)
        for (folder in result.folders) if (folder.pushed > 0) notePushed(folder.id)
        // After every burst, so a file leaves "waiting" as soon as the PC has it. Not after one a stop
        // ended: that network may be gone.
        if (!result.ended) confirm(app, result.folders.map { it.id })
        return result
    }

    /**
     * Asks the PCs which of each folder's waiting files they hold, on this thread. A folder that
     * fails keeps its files waiting: only a PC's answer clears one. True when every folder answered.
     */
    fun confirm(ctx: Context, folders: Collection<String>): Boolean {
        val metered = metered(ctx.applicationContext)
        var all = true
        for (id in folders) {
            try {
                val answer = parseConfirmed(Native.confirm(id, metered))
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

    private fun notePushed(folder: String) {
        try {
            Native.notePushed(folder)
        } catch (error: IOException) {
            Log.i(TAG, "pushed $folder: ${error.message}")
        }
    }

    /** The burst `trigger` calls for, on this thread. `decided` is a mode the caller already chose. */
    fun runTrigger(ctx: Context, trigger: Trigger, decided: Mode? = null): BurstResult {
        val app = ctx.applicationContext
        val settings = Settings.of(app)
        val now = System.currentTimeMillis()
        val mode = decided ?: if (trigger == Trigger.Foreground) {
            foregroundAttempts.decide(trigger, now, settings.lastSyncMs(), metered(app), settings.syncOnMobile)
        } else {
            policy.decide(trigger, now, settings.lastSyncMs(), metered(app), settings.syncOnMobile)
        }
        val result = burst(app, mode, asked = trigger == Trigger.Manual)
        if (!result.dropped) done.value = Finished((done.value?.serial ?: 0) + 1, trigger, result)
        if (mode == Mode.Full && (trigger == Trigger.Foreground || trigger == Trigger.Periodic)) sendWhatIsLeft(app, result)
        // Not after a burst a stop ended: that network may be gone, and the job that stopped is going too.
        if (!result.dropped && !result.ended) {
            checkFloor(app)
            // The job's own burst goes on to this by itself.
            if (trigger != Trigger.LocalChange) onKeptChanged(app)
        }
        return result
    }

    /**
     * A burst that left work and moved something goes on as the transfer job, which a burst bounded
     * to the 185 s of `sync-now` cannot do. One that moved nothing is stuck, and the job would only
     * find that out again.
     */
    private fun sendWhatIsLeft(app: Context, result: BurstResult) {
        if (result.dropped || Progress().after(result) != Next.Again) return
        wantTransfer(app)
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

    /** What `tkfs.set_last_sync` is for: the next foreground goes by the clock it set. */
    internal fun forgetForegroundAttempts() = foregroundAttempts.forget()

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
        return reportOf {
            val reply = Native.manage("sync-now", payload.toString())
            val first = BurstResult.parse(reply, emptyMap())
            if (first.errors.isEmpty()) first else {
                // Read after the burst, so an error goes only when a later exchange is newer.
                BurstResult.parse(reply, parseExchanges(Native.manage("status", "{}")))
            }
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
