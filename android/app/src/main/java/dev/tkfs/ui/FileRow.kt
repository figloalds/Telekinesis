package dev.tkfs.ui

import java.util.Locale

/**
 * What a file row says of one file, the first that applies: waiting to send, kept, on this
 * phone, or not downloaded. `kept` is true only for a file the owner asked to keep; until then
 * it is always false. `name` is the file's, for a caller that words the line for it.
 */
@Suppress("unused")
fun rowLine(name: String, size: Long, localBytes: Long, waiting: Boolean, kept: Boolean): Line {
    val sized = sizeText(size)
    return when {
        waiting -> Line("Waiting to send, $sized", Tone.Waiting)
        isOnThisPhone(size, localBytes) && kept -> Line("Kept on this phone, $sized", Tone.Ok)
        isOnThisPhone(size, localBytes) -> Line("On this phone, $sized", Tone.Ok)
        else -> Line("Not downloaded, $sized", Tone.Quiet)
    }
}

/** Every byte is here. A file that is only partly here has to be fetched to open. */
fun isOnThisPhone(size: Long, localBytes: Long): Boolean = localBytes >= size

/** "3.4 MB", "812 MB": powers of 1000, one decimal below 10, as Android's own sizes read. */
fun sizeText(bytes: Long): String {
    val units = listOf("B", "kB", "MB", "GB", "TB")
    var value = bytes.toDouble()
    var unit = 0
    while (unit < units.lastIndex && Math.round(value) >= 1000) {
        value /= 1000
        unit++
    }
    val digits = if (unit == 0) 0 else if (value < 10) 1 else 0
    return String.format(Locale.ROOT, "%.${digits}f %s", value, units[unit])
}

/** The sentence for a file that could not open because no PC answered. */
fun unreachableOpen(name: String): String = "My PC isn't reachable, so $name can't open yet."

/** The strip on the Folders screen for files that could not open, or null when there are none. */
fun openStripText(names: List<String>): String? = when (names.size) {
    0 -> null
    1 -> unreachableOpen(names[0])
    2 -> "My PC isn't reachable, so ${names[0]} and ${names[1]} can't open yet."
    else -> "My PC isn't reachable, so ${names[0]}, ${names[1]} and ${names.size - 2} more can't open yet."
}
