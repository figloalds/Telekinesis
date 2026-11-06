package dev.tkfs.provider

import java.io.FileNotFoundException
import java.text.Normalizer

/**
 * `name`, or if a directory holding `existing` has it already, `name (1).ext` with the first free
 * number. Names clash as they do in the core, whatever their case or Unicode form.
 */
fun numberedName(existing: Set<String>, name: String): String {
    val taken = existing.map(::nameKey).toSet()
    if (nameKey(name) !in taken) return name
    // A dot that starts or ends the name starts no extension.
    val dot = name.lastIndexOf('.').takeIf { it > 0 && it < name.length - 1 }
    val stem = if (dot == null) name else name.substring(0, dot)
    val extension = if (dot == null) "" else name.substring(dot)
    return generateSequence(1) { it + 1 }
        .map { "$stem ($it)$extension" }
        .first { nameKey(it) !in taken }
}

private fun nameKey(name: String): String = Normalizer.normalize(name, Normalizer.Form.NFC).lowercase()

/**
 * A document id is `FOLDER_ID:/path/in/folder`. A folder id holds no colon and a path may,
 * so only the first colon separates them.
 */
object DocumentIds {
    fun split(id: String): Pair<String, String> {
        val at = id.indexOf(':')
        if (at < 0) throw FileNotFoundException("INVALID_DOCUMENT_ID: $id")
        return id.substring(0, at) to id.substring(at + 1)
    }

    fun make(folder: String, path: String): String = "$folder:$path"

    fun childPath(parentPath: String, name: String): String = parentPath.trimEnd('/') + "/" + name

    /** The id of the directory above `id`. A folder's root is its own. */
    fun parent(id: String): String {
        val (folder, path) = split(id)
        return make(folder, parentPath(path))
    }

    fun parentPath(path: String): String = path.substringBeforeLast('/', "").ifEmpty { "/" }

    /**
     * `name`, if it can be one entry of a directory. Anything that is not would land somewhere
     * else or nowhere, and the core treats a backslash as a separator too.
     */
    fun requireName(name: String): String {
        if (!isValidName(name)) throw FileNotFoundException("INVALID_NAME: \"$name\"")
        return name
    }

    /** What [requireName] accepts, for a screen that asks the owner for a name. */
    fun isValidName(name: String): Boolean =
        name.isNotEmpty() && name != "." && name != ".." && name.none { it in "/\\\u0000" }

    /**
     * Whether `id` is below `parent`. This guards tree grants, so a path that climbs out of
     * the parent is not below it. The core treats a backslash as a separator too.
     */
    fun isChild(parent: String, id: String): Boolean {
        val prefix = parent.trimEnd('/') + "/"
        if (!id.startsWith(prefix)) return false
        val below = id.substring(prefix.length)
        return below.isNotEmpty() && below.split('/', '\\').none { it == "." || it == ".." }
    }
}
