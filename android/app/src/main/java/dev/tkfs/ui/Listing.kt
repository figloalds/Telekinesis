package dev.tkfs.ui

import dev.tkfs.share.PART_PREFIX
import org.json.JSONArray

/** One entry of a folder as `Native.list` gives it. `id` is the entity, which a rename keeps. */
data class FileEntry(
    val id: String,
    val name: String,
    val directory: Boolean,
    val size: Long,
    val localBytes: Long,
    val modifiedMs: Long,
)

/**
 * The entries to show: directories first, then by name, with the working files of a share in
 * progress left out, as the provider's listings leave them.
 */
fun parseEntries(json: String): List<FileEntry> {
    val array = JSONArray(json)
    return (0 until array.length())
        .map { array.getJSONObject(it) }
        .filterNot { it.getString("name").startsWith(PART_PREFIX) }
        .map {
            FileEntry(
                id = it.getString("id"),
                name = it.getString("name"),
                directory = it.getBoolean("directory"),
                size = it.getLong("size"),
                localBytes = it.getLong("local_bytes"),
                modifiedMs = it.getLong("modified_ms"),
            )
        }
        .sortedWith(compareByDescending<FileEntry> { it.directory }.thenBy { it.name.lowercase() }.thenBy { it.name })
}

/** The entities of the files waiting to send, from `Native.waiting`. */
fun parseWaitingEntities(json: String): Set<String> {
    val array = JSONArray(json)
    return (0 until array.length()).mapTo(HashSet()) { array.getJSONObject(it).getString("entity") }
}
