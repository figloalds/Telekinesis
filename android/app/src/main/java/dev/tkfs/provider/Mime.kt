package dev.tkfs.provider

import android.provider.DocumentsContract.Document
import android.webkit.MimeTypeMap

/** What the provider calls a document, and so what Browse asks another app to open it as. */
fun mimeOf(name: String, directory: Boolean): String {
    if (directory) return Document.MIME_TYPE_DIR
    val extension = name.substringAfterLast('.', "").lowercase()
    return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension) ?: "application/octet-stream"
}
