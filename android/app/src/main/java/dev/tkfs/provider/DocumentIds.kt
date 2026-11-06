package dev.tkfs.provider

import java.io.FileNotFoundException

/**
 * A document id is "FOLDER_ID:/path/in/folder". A folder id holds no colon and a path may,
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
