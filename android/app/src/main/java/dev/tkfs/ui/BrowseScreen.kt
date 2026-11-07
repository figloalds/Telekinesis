package dev.tkfs.ui

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.provider.DocumentsContract
import androidx.activity.compose.BackHandler
import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.unit.dp
import dev.tkfs.Native
import dev.tkfs.R
import dev.tkfs.provider.DocumentIds
import dev.tkfs.provider.TkfsProvider
import dev.tkfs.provider.mimeOf
import dev.tkfs.sync.SyncCoordinator
import dev.tkfs.sync.parseFreed
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONException
import java.io.IOException

/** What a folder's listing is now: not read yet, read, or why it could not be. */
private sealed class Listed {
    data object Loading : Listed()
    data class Files(val entries: List<FileEntry>, val waiting: Set<String>) : Listed()
    data class Failed(val failure: Failure) : Listed()
}

/**
 * A folder's files and what is on the phone. Tap opens a file in another app and a folder in
 * place; a long press holds the actions the Files app has no room for. The path survives a
 * rotation. Reading the listing asks no PC for anything.
 */
@OptIn(ExperimentalFoundationApi::class, ExperimentalMaterial3Api::class)
@Composable
fun BrowseScreen(folder: String, label: String, waitingFiles: Int, onBack: () -> Unit) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    var path by rememberSaveable { mutableStateOf("/") }
    var listed by remember { mutableStateOf<Listed>(Listed.Loading) }
    var changes by remember { mutableIntStateOf(0) }
    var note by remember { mutableStateOf<String?>(null) }
    var sheet by remember { mutableStateOf<FileEntry?>(null) }
    var renaming by remember { mutableStateOf<FileEntry?>(null) }
    var deleting by remember { mutableStateOf<FileEntry?>(null) }
    // A burst that brought or sent something lists the folder again.
    val burst by SyncCoordinator.finished.collectAsState()

    LaunchedEffect(folder, path, burst?.serial, waitingFiles, changes) {
        listed = withContext(Dispatchers.IO) { read(folder, path) }
    }
    BackHandler(enabled = path != "/") { path = DocumentIds.parentPath(path) }

    fun up() = if (path == "/") onBack() else path = DocumentIds.parentPath(path)
    fun changed(parent: String) {
        SyncCoordinator.onLocalChange(context)
        context.contentResolver.notifyChange(
            DocumentsContract.buildChildDocumentsUri(TkfsProvider.AUTHORITY, DocumentIds.make(folder, parent)),
            null,
        )
        changes++
    }

    Column(Modifier.fillMaxSize()) {
        ScreenBar(if (path == "/") label else path.substringAfterLast('/'), onBack = ::up)
        Column(Modifier.weight(1f).padding(horizontal = 16.dp)) {
            note?.let { Note(it) { note = null } }
            when (val now = listed) {
                Listed.Loading -> Text("Loading...", color = MaterialTheme.colorScheme.onSurfaceVariant)
                is Listed.Failed -> FailureNote(now.failure)
                is Listed.Files -> if (now.entries.isEmpty()) {
                    Text("This folder is empty.", color = MaterialTheme.colorScheme.onSurfaceVariant)
                } else {
                    LazyColumn(Modifier.fillMaxSize()) {
                        items(now.entries, key = { it.id }) { entry ->
                            FileRow(
                                entry,
                                waiting = entry.id in now.waiting,
                                onClick = {
                                    if (entry.directory) path = DocumentIds.childPath(path, entry.name)
                                    else note = open(context, folder, DocumentIds.childPath(path, entry.name), entry.name)
                                },
                                onLongClick = { sheet = entry },
                            )
                            HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
                        }
                    }
                }
            }
        }
    }

    sheet?.let { entry ->
        val child = DocumentIds.childPath(path, entry.name)
        ModalBottomSheet(onDismissRequest = { sheet = null }) {
            Column(Modifier.navigationBarsPadding().padding(bottom = 16.dp)) {
                Text(entry.name, style = MaterialTheme.typography.titleMedium, maxLines = 1, modifier = Modifier.padding(horizontal = 24.dp, vertical = 8.dp))
                // A folder's mark is lifted on the folder.
                Action(R.drawable.ic_pin, keepAction(entry.marked, entry.kept), enabled = entry.marked || !entry.kept) {
                    sheet = null
                    scope.launch {
                        val failure = withContext(Dispatchers.IO) {
                            try {
                                Native.mark(folder, entry.id, !entry.marked)
                                SyncCoordinator.onKeptChanged(context)
                                null
                            } catch (error: IOException) {
                                Failure.of(error)
                            }
                        }
                        note = failure?.sentence
                        changes++
                    }
                }
                if (!entry.directory) {
                    Action(R.drawable.ic_cloud, "Remove download", enabled = !entry.kept && entry.localBytes > 0) {
                        sheet = null
                        note = "Asking My PC..."
                        scope.launch {
                            note = withContext(Dispatchers.IO) { removeDownload(folder, entry) }
                            context.contentResolver.notifyChange(
                                DocumentsContract.buildChildDocumentsUri(TkfsProvider.AUTHORITY, DocumentIds.make(folder, path)),
                                null,
                            )
                            changes++
                        }
                    }
                    Action(R.drawable.ic_share, "Share") {
                        sheet = null
                        note = share(context, folder, child, entry.name)
                    }
                }
                Action(R.drawable.ic_edit, "Rename") {
                    sheet = null
                    renaming = entry
                }
                Action(R.drawable.ic_trash, "Delete everywhere", danger = true) {
                    sheet = null
                    deleting = entry
                }
            }
        }
    }

    renaming?.let { entry ->
        RenameDialog(
            entry.name,
            onCancel = { renaming = null },
            onRename = { name ->
                // Kept on screen with its reason if the name is refused.
                var reason: String? = null
                withContext(Dispatchers.IO) {
                    try {
                        Native.rename(folder, DocumentIds.childPath(path, entry.name), DocumentIds.childPath(path, name))
                    } catch (error: IOException) {
                        reason = if (codeOf(error.message.orEmpty()) == "ALREADY_EXISTS") "A file with that name is already here." else Failure.of(error).sentence
                    }
                }
                if (reason == null) {
                    renaming = null
                    changed(path)
                }
                reason
            },
        )
    }

    deleting?.let { entry ->
        AlertDialog(
            onDismissRequest = { deleting = null },
            title = { Text("Delete ${entry.name} everywhere?") },
            text = {
                Text(
                    (if (entry.directory) "It and everything in it are deleted here and, when My PC is reachable, there too."
                    else "It is deleted here and, when My PC is reachable, there too."),
                )
            },
            confirmButton = {
                TextButton(onClick = {
                    deleting = null
                    scope.launch {
                        val failure = withContext(Dispatchers.IO) {
                            try {
                                Native.delete(folder, DocumentIds.childPath(path, entry.name))
                                null
                            } catch (error: IOException) {
                                Failure.of(error)
                            }
                        }
                        note = failure?.sentence
                        changed(path)
                    }
                }) { Text("Delete everywhere", color = MaterialTheme.colorScheme.error) }
            },
            dismissButton = { TextButton(onClick = { deleting = null }) { Text("Cancel") } },
        )
    }
}

/** What Browse says after "Remove download": null when the download went. */
private fun removeDownload(folder: String, entry: FileEntry): String? = try {
    removeSentence(entry.name, parseFreed(Native.removeDownload(folder, entry.id)))
} catch (error: IOException) {
    Failure.of(error).sentence
} catch (error: JSONException) {
    "The app could not read what happened to ${entry.name}."
}

private fun read(folder: String, path: String): Listed = try {
    Listed.Files(parseEntries(Native.list(folder, path)), parseWaitingEntities(Native.waiting(folder)))
} catch (error: IOException) {
    Listed.Failed(Failure.of(error))
} catch (error: JSONException) {
    Listed.Failed(Failure(null, "The app could not read this folder."))
}

@OptIn(ExperimentalFoundationApi::class)
@Composable
private fun FileRow(entry: FileEntry, waiting: Boolean, onClick: () -> Unit, onLongClick: () -> Unit) {
    val mime = mimeOf(entry.name, entry.directory)
    val line = when {
        !entry.directory -> rowLine(entry.name, entry.size, entry.localBytes, waiting, entry.kept)
        entry.kept -> Line("Kept on this phone", Tone.Ok)
        else -> null
    }
    Row(
        Modifier.fillMaxWidth()
            .combinedClickable(role = Role.Button, onClick = onClick, onLongClick = onLongClick)
            .heightIn(min = 56.dp)
            .padding(vertical = 10.dp),
        horizontalArrangement = Arrangement.spacedBy(14.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Icon(
            painterResource(
                when {
                    entry.directory -> R.drawable.ic_folder
                    mime.startsWith("image/") -> R.drawable.ic_image
                    else -> R.drawable.ic_file
                },
            ),
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(22.dp),
        )
        Column(Modifier.weight(1f)) {
            Text(entry.name, style = MaterialTheme.typography.bodyLarge, maxLines = 1)
            if (line != null) Text(line.text, color = toneColor(line.tone), style = MaterialTheme.typography.bodyMedium)
        }
        if (line != null) {
            // The same word as the line, as a mark: sending, here, or not yet.
            val mark = when {
                waiting -> R.drawable.ic_up
                entry.kept && (entry.directory || isOnThisPhone(entry.size, entry.localBytes)) -> R.drawable.ic_pin
                isOnThisPhone(entry.size, entry.localBytes) -> R.drawable.ic_check
                else -> R.drawable.ic_cloud
            }
            Icon(painterResource(mark), contentDescription = null, tint = toneColor(line.tone), modifier = Modifier.size(20.dp))
        }
    }
}

@Composable
private fun Action(icon: Int, text: String, enabled: Boolean = true, danger: Boolean = false, onClick: () -> Unit) {
    val color = if (danger) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurface
    Row(
        Modifier.fillMaxWidth()
            .alpha(if (enabled) 1f else 0.4f)
            .combinedClickableIf(enabled, onClick)
            .heightIn(min = 52.dp)
            .padding(horizontal = 24.dp),
        horizontalArrangement = Arrangement.spacedBy(16.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Icon(painterResource(icon), contentDescription = null, tint = color, modifier = Modifier.size(22.dp))
        Text(text, color = color, style = MaterialTheme.typography.bodyLarge)
    }
}

@OptIn(ExperimentalFoundationApi::class)
private fun Modifier.combinedClickableIf(enabled: Boolean, onClick: () -> Unit): Modifier =
    combinedClickable(enabled = enabled, role = Role.Button, onClick = onClick)

/** `onRename` answers why the name was refused, or null once it is done. */
@Composable
private fun RenameDialog(name: String, onCancel: () -> Unit, onRename: suspend (String) -> String?) {
    var text by rememberSaveable { mutableStateOf(name) }
    var refused by remember { mutableStateOf<String?>(null) }
    var working by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()
    val valid = DocumentIds.isValidName(text)
    AlertDialog(
        onDismissRequest = onCancel,
        title = { Text("Rename") },
        text = {
            OutlinedTextField(
                value = text,
                onValueChange = { text = it; refused = null },
                singleLine = true,
                isError = !valid || refused != null,
                supportingText = {
                    val message = if (!valid) "A name cannot be empty, . or .., or hold / or \\." else refused
                    if (message != null) Text(message)
                },
            )
        },
        confirmButton = {
            TextButton(
                enabled = valid && text != name && !working,
                onClick = {
                    working = true
                    scope.launch {
                        refused = onRename(text)
                        working = false
                    }
                },
            ) { Text("Rename") }
        },
        dismissButton = { TextButton(onClick = onCancel) { Text("Cancel") } },
    )
}

/** A short sentence above the files, until dismissed. */
@Composable
private fun Note(text: String, onDismiss: () -> Unit) {
    Row(Modifier.fillMaxWidth().padding(vertical = 4.dp), verticalAlignment = Alignment.CenterVertically) {
        Text(text, Modifier.weight(1f), style = MaterialTheme.typography.bodyMedium)
        TextButton(onClick = onDismiss) { Text("Dismiss") }
    }
}

private fun documentUri(folder: String, path: String) =
    DocumentsContract.buildDocumentUri(TkfsProvider.AUTHORITY, DocumentIds.make(folder, path))

/** Opens the file in another app, or says there is none; null when an app took it. */
private fun open(context: Context, folder: String, path: String, name: String): String? {
    val intent = Intent(Intent.ACTION_VIEW)
        .setDataAndType(documentUri(folder, path), mimeOf(name, false))
        .addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
    return try {
        context.startActivity(intent)
        null
    } catch (error: ActivityNotFoundException) {
        "No app on this phone can open $name."
    }
}

private fun share(context: Context, folder: String, path: String, name: String): String? {
    val send = Intent(Intent.ACTION_SEND)
        .setType(mimeOf(name, false))
        .putExtra(Intent.EXTRA_STREAM, documentUri(folder, path))
        .addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
    return try {
        context.startActivity(Intent.createChooser(send, name).addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION))
        null
    } catch (error: ActivityNotFoundException) {
        "No app on this phone can take $name."
    }
}
