package dev.tkfs.ui

import android.app.Application
import android.os.StatFs
import android.text.format.Formatter
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import dev.tkfs.Native
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
    val failure: Failure? = null,
    val pairing: Pairing = Pairing.Fields,
    val storage: String = "",
    val now: Long = System.currentTimeMillis(),
)

private const val POLL_MS = 2_000L
private const val STORAGE_EVERY = 5

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
    private var bursts: Map<String, String> = emptyMap()
    private var polls = 0

    fun startPolling() {
        if (polling?.isActive == true) return
        polling = viewModelScope.launch {
            while (isActive) {
                refresh()
                delay(POLL_MS)
            }
        }
    }

    fun stopPolling() {
        polling?.cancel()
        polling = null
    }

    fun show(screen: Screen) = mutable.update {
        it.copy(screen = screen, failure = null, pairing = Pairing.Fields)
    }

    fun dismissFailure() = mutable.update { it.copy(failure = null) }

    private suspend fun refresh() {
        val snapshot = try {
            parseSnapshot(call("status"), bursts)
        } catch (error: IOException) {
            mutable.update { it.copy(failure = it.failure ?: Failure.of(error)) }
            return
        } catch (error: JSONException) {
            mutable.update { it.copy(failure = Failure(null, "The app could not read its own status.")) }
            return
        }
        val storage = if (polls++ % STORAGE_EVERY == 0) storageLine() else mutable.value.storage
        mutable.update { it.copy(snapshot = snapshot, storage = storage, now = System.currentTimeMillis()) }
    }

    /** One burst for every folder; the errors it reports stay on their folders until the next. */
    fun syncNow() {
        viewModelScope.launch {
            mutable.update { it.copy(failure = null) }
            runBurst()
            refresh()
        }
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
                    runBurst()
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
                runBurst()
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

    private suspend fun runBurst() {
        mutable.update { it.copy(syncing = true) }
        try {
            bursts = parseBurstErrors(call("sync-now"))
        } catch (error: IOException) {
            mutable.update { it.copy(failure = Failure.of(error)) }
        }
        mutable.update { it.copy(syncing = false) }
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
