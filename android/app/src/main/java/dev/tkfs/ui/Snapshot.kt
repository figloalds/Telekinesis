package dev.tkfs.ui

import dev.tkfs.sync.BurstError
import org.json.JSONArray
import org.json.JSONObject

/** One PC this phone dials: `Network::peers` rows that have an endpoint. */
data class PeerStatus(
    val installation: String,
    val endpoint: String,
    /** Whether its control lane is Ready. */
    val ready: Boolean,
    /** The control lane's failure, while it is down. */
    val error: String?,
)

data class Snapshot(val folders: List<FolderStatus>, val peers: List<PeerStatus>) {
    val pcName: String get() = pcName(peers.map { it.endpoint })
    val lastSyncMs: Long? get() = folders.mapNotNull { it.lastSyncMs }.maxOrNull()

    /** Set when no PC dialed from here answers, and not for a PC that refuses this phone. */
    val unreachable: Boolean get() = peers.isNotEmpty() && peers.none { it.ready } && !revoked
    val revoked: Boolean get() = peers.any { needsPairing(it.error) }
}

private val DOWN = setOf("Backoff", "Unauthorized", "Incompatible")

/**
 * The reply of `status`: its `folders` (`folder_entry` in the hub) and its `peers`
 * (`Network::peers`). `bursts` holds the error of each folder's last burst, by folder id; one
 * goes once the folder reports an exchange newer than the burst's. `status` knows of exchanges only
 * since this process started, so a folder with none there goes by `storedSyncMs`, 0 being none.
 */
fun parseSnapshot(
    reply: String,
    bursts: Map<String, BurstError> = emptyMap(),
    storedSyncMs: (folder: String) -> Long = { 0 },
): Snapshot {
    val status = JSONObject(reply)
    val peers = status.optJSONArray("peers").objects().mapNotNull { peer ->
        val endpoint = peer.optStringOrNull("endpoint") ?: return@mapNotNull null
        val control = peer.optJSONArray("lanes").objects().firstOrNull()
        val state = control?.optStringOrNull("state")
        PeerStatus(
            installation = peer.getString("installation"),
            endpoint = endpoint,
            ready = state == "Ready",
            error = if (state in DOWN) control?.optStringOrNull("last_error") else null,
        )
    }
    val reachable = peers.isEmpty() || peers.any { it.ready }
    val revoked = peers.firstNotNullOfOrNull { it.error?.takeIf(::needsPairing) }
    val folders = status.optJSONArray("folders").objects().map { entry ->
        val record = entry.getJSONObject("record")
        val id = record.getString("id")
        val exchanged = lastExchange(entry)
        FolderStatus(
            id = id,
            label = record.getString("label"),
            error = entry.optStringOrNull("error") ?: revoked
                ?: bursts[id]?.takeIf { it.stillApplies(exchanged) }?.message,
            conflicts = entry.optInt("conflicts", 0),
            pcReachable = reachable,
            lastSyncMs = exchanged ?: storedSyncMs(id).takeIf { it > 0 },
            repo = record.optString("repo"),
        )
    }
    return Snapshot(folders, peers)
}

/** The folders a PC publishes, from `remote-list`: `[{repo, label}]`. */
data class Published(val repo: String, val label: String)

fun parsePublished(reply: String): List<Published> =
    JSONArray(reply).objects().map { Published(it.getString("repo"), it.getString("label")) }

/**
 * The snapshot with each folder's waiting files. `counted` has the folders whose list could be read
 * now. One that could not (`failed`, with why) keeps what the last poll said and shows that error,
 * so a failed read is never "Synced". A folder whose table is still catching up with its journal is
 * `waitingUnknown`: nothing waiting yet says nothing. Of `confirmations`, only what still waits is shown.
 */
fun Snapshot.withWaiting(
    counted: Map<String, Int>,
    failed: Map<String, String>,
    last: Snapshot?,
    confirmations: Map<String, Confirmed>,
): Snapshot = copy(folders = folders.map { folder ->
    val waiting = counted[folder.id] ?: last?.folders?.firstOrNull { it.id == folder.id }?.waiting ?: 0
    val confirmed = confirmations[folder.id]
    folder.copy(
        waiting = waiting,
        unsupported = minOf(waiting, confirmed?.unsupported ?: 0),
        waitingUnknown = confirmed?.scanning == true,
        error = folder.error ?: failed[folder.id],
    )
})

/** How many files `Native.waiting` lists. */
fun parseWaiting(reply: String): Int = JSONArray(reply).length()

/** What `Native.confirm` answered: files still waiting, and those a too-old PC could not be asked about. */
data class Confirmed(val waiting: Int, val unsupported: Int, val scanning: Boolean = false)

fun parseConfirmed(reply: String): Confirmed =
    JSONObject(reply).let { Confirmed(it.getInt("waiting"), it.getInt("unsupported"), it.optBoolean("scanning", false)) }

/** Each folder's newest `last_exchange_ms` in a `status` reply, by folder id; none for a folder with none. */
fun parseExchanges(reply: String): Map<String, Long> =
    JSONObject(reply).optJSONArray("folders").objects()
        .mapNotNull { entry -> lastExchange(entry)?.let { entry.getJSONObject("record").getString("id") to it } }
        .toMap()

/** A slot has 0 until its first exchange finishes, which is no time at all. */
private fun lastExchange(entry: JSONObject): Long? = entry.optJSONObject("sync")?.let { sync ->
    sync.keys().asSequence().mapNotNull { sync.getJSONObject(it).optLongOrNull("last_exchange_ms") }.maxOrNull()
}?.takeIf { it > 0 }

private fun JSONArray?.objects(): List<JSONObject> =
    if (this == null) emptyList() else (0 until length()).map { getJSONObject(it) }

private fun JSONObject.optStringOrNull(name: String): String? =
    if (isNull(name)) null else optString(name).ifEmpty { null }

private fun JSONObject.optLongOrNull(name: String): Long? = if (isNull(name)) null else optLong(name)
