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
 * goes once the folder reports an exchange newer than the burst's.
 */
fun parseSnapshot(reply: String, bursts: Map<String, BurstError> = emptyMap()): Snapshot {
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
        val lastSync = lastExchange(entry)
        FolderStatus(
            id = id,
            label = record.getString("label"),
            error = entry.optStringOrNull("error") ?: revoked
                ?: bursts[id]?.takeIf { it.stillApplies(lastSync) }?.message,
            conflicts = entry.optInt("conflicts", 0),
            pcReachable = reachable,
            lastSyncMs = lastSync,
            repo = record.optString("repo"),
        )
    }
    return Snapshot(folders, peers)
}

/** The folders a PC publishes, from `remote-list`: `[{repo, label}]`. */
data class Published(val repo: String, val label: String)

fun parsePublished(reply: String): List<Published> =
    JSONArray(reply).objects().map { Published(it.getString("repo"), it.getString("label")) }

/** Each folder's newest `last_exchange_ms` in a `status` reply, by folder id; none for a folder with none. */
fun parseExchanges(reply: String): Map<String, Long> =
    JSONObject(reply).optJSONArray("folders").objects()
        .mapNotNull { entry -> lastExchange(entry)?.let { entry.getJSONObject("record").getString("id") to it } }
        .toMap()

private fun lastExchange(entry: JSONObject): Long? = entry.optJSONObject("sync")?.let { sync ->
    sync.keys().asSequence().mapNotNull { sync.getJSONObject(it).optLongOrNull("last_exchange_ms") }.maxOrNull()
}

private fun JSONArray?.objects(): List<JSONObject> =
    if (this == null) emptyList() else (0 until length()).map { getJSONObject(it) }

private fun JSONObject.optStringOrNull(name: String): String? =
    if (isNull(name)) null else optString(name).ifEmpty { null }

private fun JSONObject.optLongOrNull(name: String): Long? = if (isNull(name)) null else optLong(name)
