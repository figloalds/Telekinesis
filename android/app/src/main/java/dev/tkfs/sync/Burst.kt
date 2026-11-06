package dev.tkfs.sync

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.json.JSONObject

/**
 * The error a burst gave one folder. It stays on the folder until the folder reports an
 * exchange newer than `since`, its `last_exchange_ms` when the burst ended.
 */
data class BurstError(val message: String, val since: Long?) {
    fun stillApplies(lastExchangeMs: Long?): Boolean =
        lastExchangeMs == null || (since != null && lastExchangeMs <= since)
}

data class FolderBurst(
    val id: String,
    val exchanged: Long,
    val remaining: Int?,
    val error: BurstError?,
    /** How many local changes this burst sent to its peers. */
    val pushed: Long = 0,
)

/** What one burst reported: `sync-now`'s reply, or why the call failed, or that it was dropped. */
data class BurstResult(
    val folders: List<FolderBurst> = emptyList(),
    /** The call itself failed (for instance, the orchestrator was not running). */
    val failure: String? = null,
    /** Another burst was running and this wanted nothing more than it. */
    val dropped: Boolean = false,
) {
    val exchanged: Long get() = folders.sumOf { it.exchanged }
    val errors: Map<String, BurstError> get() = folders.mapNotNull { f -> f.error?.let { f.id to it } }.toMap()

    companion object {
        val DROPPED = BurstResult(dropped = true)

        /** `reply` is `sync-now`'s; `exchangeMs` is each folder's `last_exchange_ms` just after it. */
        fun parse(reply: String, exchangeMs: Map<String, Long>): BurstResult {
            val list = JSONObject(reply).optJSONArray("folders")
            val folders = (0 until (list?.length() ?: 0)).map { index ->
                val folder = list!!.getJSONObject(index)
                val id = folder.getString("id")
                val message = if (folder.isNull("error")) null else folder.optString("error").ifEmpty { null }
                FolderBurst(
                    id = id,
                    exchanged = folder.optLong("exchanged", 0),
                    pushed = folder.optLong("pushed", 0),
                    remaining = if (folder.isNull("remaining")) null else folder.optInt("remaining"),
                    error = message?.let { BurstError(it, exchangeMs[id]) },
                )
            }
            return BurstResult(folders)
        }
    }
}

/**
 * At most one app-driven burst at a time. A request that wants nothing more than the running
 * burst is dropped; a full one that arrives during a metadata-only burst waits and runs after it.
 */
class Bursts(private val run: (Mode) -> BurstResult) {
    private val lock = Object()
    private var running: Mode? = null
    private val busy = MutableStateFlow(false)
    val isRunning: StateFlow<Boolean> get() = busy

    fun request(mode: Mode): BurstResult {
        if (mode == Mode.Skip) return BurstResult.DROPPED
        synchronized(lock) {
            while (true) {
                val current = running ?: break
                if (rank(current) >= rank(mode)) return BurstResult.DROPPED
                lock.wait()
            }
            running = mode
            busy.value = true
        }
        try {
            return run(mode)
        } finally {
            synchronized(lock) {
                running = null
                busy.value = false
                lock.notifyAll()
            }
        }
    }

    private fun rank(mode: Mode) = if (mode == Mode.Full) 2 else 1
}
