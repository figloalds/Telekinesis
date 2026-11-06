package dev.tkfs.ui

import org.json.JSONArray
import org.json.JSONObject
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

/** One competing version of a file: a head of a conflict. `deleted` is a head that deletes it. */
data class VersionEvent(
    val id: String,
    val author: String,
    val size: Long,
    val modifiedMs: Long,
    val deleted: Boolean = false,
)

/** A file with two or more heads that nobody has chosen between. */
data class FileConflict(val entity: String, val name: String, val versions: List<VersionEvent>)

private const val FILETIME_UNIX_EPOCH = 116_444_736_000_000_000L

/** The reply of the `conflicts` op: `Conflict` rows. A resolved one (history) is left out. */
fun parseConflicts(reply: String): List<FileConflict> {
    val rows = JSONArray(reply)
    return (0 until rows.length()).map { rows.getJSONObject(it) }.filterNot { it.optBoolean("resolved") }.map { row ->
        val heads = row.getJSONArray("heads")
        val versions = (0 until heads.length()).map { heads.getJSONObject(it) }
        FileConflict(
            entity = row.getString("entity"),
            name = versions.firstOrNull()?.getJSONObject("state")?.getString("name").orEmpty(),
            versions = versions.map(::parseVersion),
        )
    }
}

private fun parseVersion(event: JSONObject): VersionEvent {
    val state = event.getJSONObject("state")
    val content = state.optJSONObject("content")
    val modified = state.getJSONObject("times").getLong("modified")
    return VersionEvent(
        id = event.getString("id"),
        author = event.getString("author"),
        size = content?.optLong("total_len", 0L) ?: 0L,
        modifiedMs = (modified - FILETIME_UNIX_EPOCH).coerceAtLeast(0L) / 10_000,
        deleted = state.optBoolean("deleted", false),
    )
}

/** This device's id from a folder's `status` reply. */
fun parseDevice(reply: String): String = JSONObject(reply).getString("device")

/** What `preview-version` read, from its reply. */
fun parsePreview(reply: String): String = JSONObject(reply).getString("preview")

/**
 * Who made a version: this phone, the PC when only one is paired, otherwise the short id of the
 * author, as a PC's author id does not map to a name the phone knows.
 */
fun versionAuthor(author: String, device: String, pcs: Int): String = when {
    author == device -> "This phone"
    pcs <= 1 -> "My PC"
    else -> author.take(8)
}

/**
 * "This phone, Today at 11:42, 2.3 MB", or for a head that deletes the file "Deleted on this phone,
 * Today at 11:42". `pc` is what a peer's version is called.
 */
fun versionLine(
    e: VersionEvent,
    device: String,
    now: Long,
    pc: String = "My PC",
    zone: ZoneId = ZoneId.systemDefault(),
): String {
    val mine = e.author == device
    val who = if (mine) "This phone" else pc
    val when_ = whenText(e.modifiedMs, now, zone)
    return if (e.deleted) "Deleted on ${if (mine) "this phone" else pc}, $when_" else "$who, $when_, ${sizeText(e.size)}"
}

private fun whenText(ms: Long, now: Long, zone: ZoneId): String {
    val moment = Instant.ofEpochMilli(ms).atZone(zone)
    val days = LocalDate.ofInstant(Instant.ofEpochMilli(now), zone).toEpochDay() - moment.toLocalDate().toEpochDay()
    val clock = DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT).format(moment)
    return when (days) {
        0L -> "Today at $clock"
        1L -> "Yesterday at $clock"
        else -> DateTimeFormatter.ofPattern("d MMM", Locale.ENGLISH).format(moment) + " at $clock"
    }
}
