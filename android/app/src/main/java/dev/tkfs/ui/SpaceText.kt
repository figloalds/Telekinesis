package dev.tkfs.ui

import dev.tkfs.sync.Freed

private const val NOT_ON_PC = "NOT_ON_PC"
private const val PC_UNREACHABLE = "PC_UNREACHABLE"
private const val PC_TOO_OLD = "PC_TOO_OLD"

/** The strip on the Folders screen when space is low and only a PC could change that; else null. */
fun lowSpaceStrip(blocked: String?): String? = when (blocked) {
    PC_UNREACHABLE -> "Low on space. Can't free anything until My PC is reachable."
    PC_TOO_OLD -> "Low on space. Can't free anything until My PC is updated."
    else -> null
}

/** What Browse says of "Remove download"; null when the download went, which the row itself shows. */
fun removeSentence(name: String, freed: Freed): String? = when (freed.blocked) {
    NOT_ON_PC -> "Not on My PC yet, so it can't be removed."
    PC_UNREACHABLE -> "My PC isn't reachable, so the download of $name can't be removed yet."
    PC_TOO_OLD -> "My PC needs an update before the download of $name can be removed."
    else -> if (freed.bytes > 0) null else "$name is in use or has a conflict to settle, so it stays."
}

/** What Settings says of "Free up space now". `stillLow` is whether free space is below the floor after it. */
fun freedSentence(freed: Freed, stillLow: Boolean): String {
    val gone = if (freed.bytes > 0) "Freed ${sizeText(freed.bytes)}." else ""
    val more = when {
        freed.blocked == PC_UNREACHABLE -> "Can't free more until My PC is reachable."
        freed.blocked == PC_TOO_OLD -> "Can't free more until My PC is updated."
        !stillLow -> if (freed.bytes > 0) "" else "Enough is free already."
        else -> "Nothing else can be freed: the rest is kept, in use, or not on My PC yet."
    }
    return listOf(gone, more).filter { it.isNotEmpty() }.joinToString(" ")
}

/** The Settings row's line. */
fun floorLine(floorBytes: Long): String = "${sizeText(floorBytes)}. Oldest downloads go first."

/** What the long press offers for keeping: a mark of its own, or its folder's, which only the folder can lift. */
fun keepAction(marked: Boolean, kept: Boolean): String = when {
    marked -> "Stop keeping on this phone"
    kept -> "Kept with its folder"
    else -> "Keep on this phone"
}
