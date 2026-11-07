package dev.tkfs.ui

/** A run of 64 hex digits is shaped like a pairing code or a key; none of it leaves in a report. */
private val SECRET_SHAPE = Regex("[0-9a-fA-F]{64}")

/**
 * What "Copy" in Settings puts on the clipboard: each PC with its state, each folder with what it
 * last said, and the errors still showing. It is built only from `status`, which carries endpoints
 * and error texts but no pairing code or credential; a 64-hex run is hidden anyway, in case an error
 * text ever quotes one.
 */
fun diagnosticsText(snapshot: Snapshot?, storage: String, now: Long): String {
    val lines = mutableListOf<String>()
    if (storage.isNotEmpty()) lines += "Storage: $storage"
    if (snapshot == null) {
        lines += "The app has not read its status yet."
        return hidden(lines)
    }
    lines += "PCs: ${snapshot.peers.size}"
    for (peer in snapshot.peers) {
        lines += "- ${peer.endpoint}: ${if (peer.ready) "connected" else "not connected"}" +
            (peer.error?.let { " ($it)" } ?: "")
    }
    lines += "Folders: ${snapshot.folders.size}"
    for (folder in snapshot.folders) {
        val synced = folder.lastSyncMs?.let { "last synced ${folderAge(now - it)} ago" } ?: "never synced"
        lines += "- ${folder.label}: $synced, ${folder.waiting} waiting to send, ${folder.conflicts} conflicts"
    }
    val errors = snapshot.peers.mapNotNull { it.error } + snapshot.folders.mapNotNull { f -> f.error?.let { "${f.label}: $it" } }
    lines += "Recent errors:"
    if (errors.isEmpty()) lines += "- none" else errors.distinct().forEach { lines += "- $it" }
    return hidden(lines)
}

private fun hidden(lines: List<String>) = lines.joinToString("\n").replace(SECRET_SHAPE, "[hidden]")

private fun folderAge(ms: Long): String {
    val minutes = (ms.coerceAtLeast(0) / 60_000)
    return when {
        minutes < 1 -> "under a minute"
        minutes < 60 -> "$minutes min"
        minutes < 24 * 60 -> "${minutes / 60} h"
        else -> "${minutes / (24 * 60)} days"
    }
}
