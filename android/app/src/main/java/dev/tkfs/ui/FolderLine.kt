package dev.tkfs.ui

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

enum class Tone { Ok, Waiting, Bad, Quiet }

data class Line(val text: String, val tone: Tone)

/**
 * What the Folders screen knows of one folder. `waiting` is the files no PC has confirmed holding,
 * and `unsupported` those of them that a PC could not be asked about because it is too old.
 */
data class FolderStatus(
    val id: String,
    val label: String,
    val error: String? = null,
    val conflicts: Int = 0,
    val pcReachable: Boolean = true,
    val lastSyncMs: Long? = null,
    val waiting: Int = 0,
    val repo: String = "",
    val unsupported: Int = 0,
    /** What waits is not known yet: the table is still being filled from the journal. */
    val waitingUnknown: Boolean = false,
)

private const val REVOKED = "PEER_REVOKED"
private const val MINUTE = 60_000L
private const val HOUR = 60 * MINUTE
private const val DAY = 24 * HOUR

/** The sentence a folder shows, the first that applies: error, conflicts, waiting, then sync. */
fun folderLine(s: FolderStatus, now: Long, zone: ZoneId = ZoneId.systemDefault()): Line = when {
    s.error != null -> Line(errorInWords(s.error), Tone.Bad)
    s.conflicts > 0 -> Line(plural(s.conflicts, "conflict", "conflicts") + " to settle", Tone.Bad)
    s.waiting > 0 -> Line(
        plural(s.waiting, "file", "files") + " waiting to send" + if (s.unsupported > 0) ". Update the PC." else "",
        Tone.Waiting,
    )
    s.waitingUnknown -> Line("Checking what is waiting to send", Tone.Waiting)
    s.lastSyncMs == null -> Line("Not synced yet", Tone.Quiet)
    !s.pcReachable -> Line("Synced at " + clock(s.lastSyncMs, zone), Tone.Quiet)
    else -> Line("Synced " + ago(now - s.lastSyncMs), Tone.Ok)
}

fun unreachableStrip(pc: String, lastSyncMs: Long?, zone: ZoneId = ZoneId.systemDefault()): String =
    "$pc isn't reachable." + if (lastSyncMs == null) "" else " Last synced at ${clock(lastSyncMs, zone)}."

/** "My PC" while it is the only one; with several, the one at `endpoint`. */
fun pcName(endpoints: List<String>, endpoint: String? = endpoints.firstOrNull()): String =
    if (endpoints.size <= 1 || endpoint == null) "My PC" else endpoint

/** Whether the PC no longer knows this phone, so only pairing again helps. */
fun needsPairing(error: String?): Boolean = error != null && codeOf(error) == REVOKED

/** An orchestrator error as a person reads it: a sentence for the codes that need one. */
fun errorInWords(error: String): String = when (val code = codeOf(error)) {
    REVOKED -> "My PC no longer recognizes this phone. Pair again."
    "WS_CONNECT_FAILED" -> "Could not reach My PC."
    "SYNC_NOW_TIMEOUT" -> "Syncing took too long. Try again."
    "WORKER_NOT_RUNNING" -> "This folder is not running."
    "PHONE_DB_UNAVAILABLE" -> "Could not check what is waiting to send."
    // Nothing of this phone's can go to it, and it cannot say what it holds either.
    "PEER_PUSH_UNAVAILABLE" -> "My PC is too old to take files from this phone. Update the PC."
    else -> if (code == null) error else error.substringAfter(':').trim().ifEmpty { error }
}

/** The leading CODE of `CODE: detail`, or null. */
fun codeOf(error: String): String? {
    val code = error.substringBefore(':', "")
    return code.takeIf { it.length >= 4 && it.all { c -> c.isUpperCase() || c.isDigit() || c == '_' } }
}

private fun plural(count: Int, one: String, many: String) = "$count ${if (count == 1) one else many}"

private fun clock(ms: Long, zone: ZoneId): String =
    DateTimeFormatter.ofPattern("HH:mm").withZone(zone).format(Instant.ofEpochMilli(ms))

private fun ago(elapsed: Long): String = when {
    elapsed < MINUTE -> "just now"
    elapsed < HOUR -> "${elapsed / MINUTE} min ago"
    elapsed < DAY -> "${elapsed / HOUR} h ago"
    else -> "${elapsed / DAY} d ago"
}
