package dev.tkfs.ui

import android.app.Application
import android.os.PowerManager
import android.os.StatFs
import android.text.format.Formatter
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import dev.tkfs.Native
import dev.tkfs.Settings
import dev.tkfs.provider.OpenNotices
import dev.tkfs.share.ShareLedger
import dev.tkfs.sync.BurstError
import dev.tkfs.sync.Finished
import dev.tkfs.sync.SyncCoordinator
import dev.tkfs.sync.Trigger
import dev.tkfs.sync.floorBytes
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONException
import org.json.JSONObject
import java.io.File
import java.io.IOException

sealed class Screen {
    data object Folders : Screen()
    data object AddPc : Screen()
    data object Settings : Screen()

    /** A folder's files, by the folder's id. */
    data class Browse(val folder: String, val label: String) : Screen()

    /** A folder's conflicts, by the folder's id. */
    data class Conflicts(val folder: String, val label: String) : Screen()
}

/** An orchestrator refusal: its code and a sentence a person can read. */
data class Failure(val code: String?, val sentence: String) {
    companion object {
        fun of(error: Exception): Failure {
            val text = error.message ?: "Something went wrong."
            return Failure(codeOf(text), errorInWords(text))
        }
    }
}

/** Where Add a PC is: the fields, or the PC's folders to pick from. */
sealed class Pairing {
    data object Fields : Pairing()
    data object Working : Pairing()
    data class Choose(val endpoint: String, val published: List<Published>, val chosen: Set<String>) : Pairing()
}

data class AppUi(
    val screen: Screen = Screen.Folders,
    /** Null until the first answer from the orchestrator. */
    val snapshot: Snapshot? = null,
    val syncing: Boolean = false,
    /** What the last action was refused with; cleared by the next action or screen change. */
    val failure: Failure? = null,
    /** Why the last status poll failed; cleared by the next poll that works. Only Folders shows it. */
    val pollFailure: Failure? = null,
    val pairing: Pairing = Pairing.Fields,
    val storage: String = "",
    /** How much the app keeps free on the volume; 0 until it has looked. */
    val floorBytes: Long = 0,
    /** Why space stays below that, when only a PC could change it. */
    val spaceBlocked: String? = null,
    /** What the last "Free up space now" did, or that it is at it. */
    val spaceNote: String? = null,
    val freeingSpace: Boolean = false,
    val syncOnMobile: Boolean = false,
    val periodMinutes: Int = 30,
    /** Whether Android lets the app run while the phone is idle (the battery-optimization exemption). */
    val batteryAllowed: Boolean = true,
    /** Names of files shared in that were not saved, until the owner dismisses them. */
    val shareFailures: List<String> = emptyList(),
    /** Names of files that could not open because no PC answered, until the owner dismisses them. */
    val openFailures: List<String> = emptyList(),
    val now: Long = System.currentTimeMillis(),
)

/** The state after a `status` poll that worked: its answer in, its failure gone. */
fun AppUi.polled(snapshot: Snapshot, storage: String, now: Long) =
    copy(snapshot = snapshot, storage = storage, now = now, pollFailure = null)

/** The state after a poll that failed. It is kept apart from an action's `failure`. */
fun AppUi.pollFailed(failure: Failure) = copy(pollFailure = failure)

private const val POLL_MS = 2_000L
private const val STORAGE_EVERY = 5
private const val CONFIRM_ATTEMPTS = 3
private const val CONFIRM_RETRY_MS = 5_000L

/**
 * What the screens show, refreshed from `status` every 2 s while one is showing and after
 * each action. Every native call runs off the main thread.
 *
 * `status` already carries `Network::peers` as its `peers`, so one call gives folders and
 * peers from the same moment.
 */
class AppModel(application: Application) : AndroidViewModel(application) {
    private val mutable = MutableStateFlow(AppUi())
    val state: StateFlow<AppUi> = mutable

    private var polling: Job? = null
    private val settings = Settings.of(application)
    private var bursts: Map<String, BurstError> = emptyMap()
    private var polls = 0
    private var seen = SyncCoordinator.finished.value?.serial ?: 0L

    init {
        mutable.update { it.copy(syncOnMobile = settings.syncOnMobile, periodMinutes = settings.periodMinutes) }
        viewModelScope.launch { SyncCoordinator.running.collect { running -> mutable.update { it.copy(syncing = running) } } }
        viewModelScope.launch { SyncCoordinator.finished.collect { it?.let { finished -> burstEnded(finished) } } }
        viewModelScope.launch { SyncCoordinator.spaceBlocked.collect { blocked -> mutable.update { it.copy(spaceBlocked = blocked) } } }
    }

    fun startPolling() {
        if (polling?.isActive == true) return
        polling = viewModelScope.launch {
            var shown = true
            while (isActive) {
                refresh()
                // Once each time the app shows: what the PC took since the last burst clears here.
                if (shown) {
                    shown = false
                    launch { confirmWhatIsShown() }
                }
                delay(POLL_MS)
            }
        }
    }

    /** A cold app may not have answered `status` yet, or its worker may not be up: try again a few times. */
    private suspend fun confirmWhatIsShown() {
        repeat(CONFIRM_ATTEMPTS) {
            val folders = mutable.value.snapshot?.folders.orEmpty().map { it.id }
            val answered = folders.isNotEmpty() && withContext(Dispatchers.IO) { SyncCoordinator.confirm(folders) }
            refresh()
            if (answered) return
            delay(CONFIRM_RETRY_MS)
        }
    }

    fun stopPolling() {
        polling?.cancel()
        polling = null
    }

    fun show(screen: Screen) = mutable.update {
        it.copy(screen = screen, failure = null, pairing = Pairing.Fields, spaceNote = null)
    }

    fun dismissFailure() = mutable.update { it.copy(failure = null) }

    fun dismissShareFailures() {
        ShareLedger.of(getApplication()).dismiss()
        mutable.update { it.copy(shareFailures = emptyList()) }
    }

    fun dismissOpenFailures() {
        OpenNotices.of(getApplication()).dismiss()
        mutable.update { it.copy(openFailures = emptyList()) }
    }

    private suspend fun refresh() {
        val snapshot = try {
            parseSnapshot(call("status"), bursts)
        } catch (error: IOException) {
            mutable.update { it.pollFailed(Failure.of(error)) }
            return
        } catch (error: JSONException) {
            mutable.update { it.pollFailed(Failure(null, "The app could not read its own status.")) }
            return
        }
        val shown = withWaiting(snapshot)
        val allowed = getApplication<Application>().getSystemService(PowerManager::class.java)
            .isIgnoringBatteryOptimizations(getApplication<Application>().packageName)
        val storage = if (polls++ % STORAGE_EVERY == 0) storageLine() else mutable.value.storage
        val floor = floorBytes(withContext(Dispatchers.IO) { StatFs(getApplication<Application>().filesDir.path).totalBytes })
        mutable.update { it.polled(shown, storage, System.currentTimeMillis()).copy(floorBytes = floor, batteryAllowed = allowed, shareFailures = ShareLedger.of(getApplication()).notices(), openFailures = OpenNotices.of(getApplication()).names()) }
    }

    /** The folders' waiting files, read from the table on disk: no PC is asked. */
    private suspend fun withWaiting(snapshot: Snapshot): Snapshot {
        val counted = HashMap<String, Int>()
        val failed = HashMap<String, String>()
        withContext(Dispatchers.IO) {
            for (folder in snapshot.folders) {
                try {
                    counted[folder.id] = parseWaiting(Native.waiting(folder.id))
                } catch (error: IOException) {
                    Log.w("tkfs", "waiting ${folder.id}: ${error.message}")
                    failed[folder.id] = error.message ?: "UNKNOWN_ERROR: the waiting files could not be read"
                } catch (error: JSONException) {
                    Log.w("tkfs", "waiting ${folder.id}: unreadable: ${error.message}")
                    failed[folder.id] = "The app could not read what is waiting to send."
                }
            }
        }
        return snapshot.withWaiting(counted, failed, mutable.value.snapshot, SyncCoordinator.confirmations.value)
    }

    /** One burst for every folder. Its errors stay on their folders until a newer exchange. */
    fun syncNow() {
        mutable.update { it.copy(failure = null) }
        SyncCoordinator.syncNow(getApplication())
    }

    /** Any burst, whoever started it, replaces the folders' errors; only the owner's own shows a failure. */
    private suspend fun burstEnded(finished: Finished) {
        if (finished.serial <= seen) return
        seen = finished.serial
        bursts = finished.result.errors
        val failure = finished.result.failure
        if (failure != null && finished.trigger == Trigger.Manual) {
            mutable.update { it.copy(failure = Failure.of(IOException(failure))) }
        }
        refresh()
    }

    /** "Free up space now": up to the floor, whatever the last burst left. */
    fun freeSpace() {
        if (mutable.value.freeingSpace) return
        mutable.update { it.copy(freeingSpace = true, spaceNote = "Freeing space...") }
        viewModelScope.launch {
            val note = withContext(Dispatchers.IO) {
                val app = getApplication<Application>()
                val freed = SyncCoordinator.freeUp(app)
                if (freed == null) "Could not free space. Try again." else freedSentence(freed, SyncCoordinator.spaceNeededNow(app) > 0)
            }
            mutable.update { it.copy(freeingSpace = false, spaceNote = note) }
            refresh()
        }
    }

    fun setSyncOnMobile(on: Boolean) {
        settings.syncOnMobile = on
        mutable.update { it.copy(syncOnMobile = on) }
        SyncCoordinator.onMobileSettingChanged(getApplication())
    }

    fun setPeriodMinutes(minutes: Int) {
        settings.periodMinutes = minutes
        mutable.update { it.copy(periodMinutes = minutes) }
        SyncCoordinator.schedulePeriodic(getApplication(), replace = true)
    }

    fun pair(endpoint: String, code: String) {
        mutable.update { it.copy(pairing = Pairing.Working, failure = null) }
        viewModelScope.launch {
            try {
                call("pair", JSONObject().put("endpoint", endpoint).put("code", code))
                val held = mutable.value.snapshot?.folders.orEmpty().map { it.repo }.toSet()
                val fresh = parsePublished(call("remote-list", JSONObject().put("endpoint", endpoint)))
                    .filter { it.repo !in held }
                if (fresh.isEmpty()) {
                    // The same PC again: its folders are already here, and a burst resumes them.
                    mutable.update { it.copy(screen = Screen.Folders, pairing = Pairing.Fields) }
                    SyncCoordinator.syncNow(getApplication())
                } else {
                    mutable.update {
                        it.copy(pairing = Pairing.Choose(endpoint, fresh, fresh.map { f -> f.repo }.toSet()))
                    }
                }
            } catch (error: IOException) {
                mutable.update { it.copy(pairing = Pairing.Fields, failure = Failure.of(error)) }
            }
            refresh()
        }
    }

    fun toggleChosen(repo: String) = mutable.update {
        val pairing = it.pairing as? Pairing.Choose ?: return@update it
        val chosen = if (repo in pairing.chosen) pairing.chosen - repo else pairing.chosen + repo
        it.copy(pairing = pairing.copy(chosen = chosen))
    }

    /** Receives the ticked folders, goes to the Folders screen and runs a burst. */
    fun receiveChosen() {
        val pairing = mutable.value.pairing as? Pairing.Choose ?: return
        val picked = pairing.published.filter { it.repo in pairing.chosen }
        mutable.update { it.copy(pairing = Pairing.Working) }
        viewModelScope.launch {
            try {
                for (folder in picked) {
                    val payload = JSONObject().put("endpoint", pairing.endpoint).put("repo", folder.repo)
                        .put("label", folder.label)
                    call("receive", payload)
                }
                mutable.update { it.copy(screen = Screen.Folders, pairing = Pairing.Fields) }
                SyncCoordinator.syncNow(getApplication())
            } catch (error: IOException) {
                mutable.update { it.copy(pairing = pairing, failure = Failure.of(error)) }
            }
            refresh()
        }
    }

    fun unpair(installation: String) = act { call("unpair", JSONObject().put("installation", installation)) }

    fun changeAddress(installation: String, endpoint: String) = act {
        call("set-endpoint", JSONObject().put("installation", installation).put("endpoint", endpoint))
    }

    private fun act(block: suspend () -> Unit) {
        viewModelScope.launch {
            try {
                block()
                bursts = emptyMap()
            } catch (error: IOException) {
                mutable.update { it.copy(failure = Failure.of(error)) }
            }
            refresh()
        }
    }

    private suspend fun call(action: String, payload: JSONObject = JSONObject()): String =
        withContext(Dispatchers.IO) { Native.manage(action, payload.toString()) }

    /** "4.8 GB on this phone, 61 GB free": what the app's data directory holds. */
    private suspend fun storageLine(): String = withContext(Dispatchers.IO) {
        val context = getApplication<Application>()
        val files = context.filesDir
        val used = files.walkTopDown().filter(File::isFile).sumOf(File::length)
        val free = StatFs(files.path).availableBytes
        "${Formatter.formatShortFileSize(context, used)} on this phone, " +
            "${Formatter.formatShortFileSize(context, free)} free"
    }
}
