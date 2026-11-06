package dev.tkfs.share

/** The words of the share screen and its notifications, as the mockup has them. */

/** "2 photos from Photos". */
internal fun leadLine(mimeTypes: List<String>, source: String?): String {
    val count = mimeTypes.size
    val kind = when {
        mimeTypes.all { it.startsWith("image/") } -> "photo" to "photos"
        mimeTypes.all { it.startsWith("video/") } -> "video" to "videos"
        else -> "file" to "files"
    }
    val what = "$count ${if (count == 1) kind.first else kind.second}"
    return if (source == null) what else "$what from $source"
}

internal fun saveLabel(count: Int): String = "Save $count ${if (count == 1) "file" else "files"}"

/** The "Inside this folder" field: `path` is "/" for the folder's top level. */
internal fun insideLine(folder: String, path: String): String =
    if (path == "/") "$folder (top level)" else (listOf(folder) + path.trim('/').split('/')).joinToString(" / ")

/** A sender's name for a file, as one entry of a directory. */
internal fun entryName(name: String): String {
    val cleaned = name.replace('/', '_').replace('\\', '_').replace('\u0000', '_')
    return if (cleaned.isEmpty() || cleaned == "." || cleaned == "..") "shared" else cleaned
}

/** The title and text of the notification that ends a share; `failed` holds each name and why. */
internal fun resultLines(saved: Int, failed: List<Pair<String, String>>, where: String): Pair<String, String> {
    val total = saved + failed.size
    val title = when {
        failed.isEmpty() -> "Saved $saved ${if (saved == 1) "file" else "files"}"
        saved == 0 -> "Nothing was saved"
        else -> "Saved $saved of $total files"
    }
    val failures = failed.joinToString(" ") { (name, why) -> "$name could not be saved: $why" }
    val text = when {
        failed.isEmpty() -> "to $where. They go to My PC when it's reachable."
        saved == 0 -> failures
        else -> "to $where. $failures"
    }
    return title to text
}
