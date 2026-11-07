package dev.tkfs.sync

import org.json.JSONObject

private const val GIB = 1024L * 1024 * 1024

/** How much the app keeps free on the volume: 2 GiB, or a tenth of it if that is more. */
fun floorBytes(volumeBytes: Long): Long = maxOf(2 * GIB, volumeBytes / 10)

/** How far free space is below the floor; 0 at or above it. */
fun spaceNeeded(freeBytes: Long, volumeBytes: Long): Long = maxOf(0, floorBytes(volumeBytes) - freeBytes)

/** What `Native.freeSpace` and `Native.removeDownload` answer: what left, and why no more could. */
data class Freed(val bytes: Long, val blocked: String?)

fun parseFreed(json: String): Freed = JSONObject(json).let {
    Freed(it.getLong("bytes"), if (it.isNull("blocked")) null else it.getString("blocked"))
}
