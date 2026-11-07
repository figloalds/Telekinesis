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

/** How a download of what is kept ended. */
enum class Kept { Done, Stopped, DoesNotFit }

/**
 * Downloads what is kept without going past the floor. `hydrate` asks its `cancel` between windows,
 * so it stops within a window of the floor, far from a full disk, where nothing could be freed at
 * all. Then space is freed, and it goes on if that made room. It also goes round for what was marked
 * while it ran. One round that brings nothing ends it: the PC cannot give that now, and the next job
 * asks again.
 */
internal fun downloadKept(
    hydrate: (cancel: () -> Boolean) -> Long,
    missing: () -> Boolean,
    belowFloor: () -> Boolean,
    freeUp: () -> Unit,
    stop: () -> Boolean,
): Kept {
    while (true) {
        if (stop()) return Kept.Stopped
        if (!missing()) return Kept.Done
        if (belowFloor()) {
            freeUp()
            if (belowFloor()) return Kept.DoesNotFit
        }
        val brought = hydrate { stop() || belowFloor() }
        if (brought == 0L && !stop() && !belowFloor()) return Kept.Done
    }
}
