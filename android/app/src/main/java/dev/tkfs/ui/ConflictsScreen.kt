package dev.tkfs.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import dev.tkfs.Native
import dev.tkfs.sync.SyncCoordinator
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONException
import org.json.JSONObject
import java.io.IOException

private sealed class Loaded {
    data object Loading : Loaded()
    data class Rows(val device: String, val conflicts: List<FileConflict>) : Loaded()
    data class Failed(val failure: Failure) : Loaded()
}

/** A version's preview: being read (the first 64 KiB, fetched if the phone lacks them), read, or why not. */
private sealed class Previewing {
    /** Which version the dialog is for, and the line that names it: the owner picks by this text. */
    abstract val version: String
    abstract val title: String

    data class Reading(override val version: String, override val title: String) : Previewing()
    data class Text(override val version: String, override val title: String, val text: String) : Previewing()
    data class Failed(override val version: String, override val title: String, val failure: Failure) : Previewing()
}

private fun folderOp(folder: String, op: String, payload: JSONObject = JSONObject()): String =
    Native.manage("folder", JSONObject().put("state", folder).put("op", op).put("payload", payload).toString())

/**
 * The folder's conflicts, the same model as the desktop: every competing version stays until one is
 * chosen. Choosing runs a burst so the choice reaches the PC. Every native call is off the main thread.
 */
@Composable
fun ConflictsScreen(folder: String, label: String, pcs: Int, now: Long, onBack: () -> Unit) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    var loaded by remember { mutableStateOf<Loaded>(Loaded.Loading) }
    var previewing by remember { mutableStateOf<Previewing?>(null) }
    var failure by remember { mutableStateOf<Failure?>(null) }
    var working by remember { mutableStateOf(false) }
    var changes by remember { mutableIntStateOf(0) }
    val burst by SyncCoordinator.finished.collectAsState()

    LaunchedEffect(folder, burst?.serial, changes) {
        loaded = withContext(Dispatchers.IO) {
            try {
                Loaded.Rows(parseDevice(folderOp(folder, "status")), parseConflicts(folderOp(folder, "conflicts")))
            } catch (error: IOException) {
                Loaded.Failed(Failure.of(error))
            } catch (error: JSONException) {
                Loaded.Failed(Failure(null, "The app could not read the conflicts."))
            }
        }
    }

    fun preview(title: String, version: VersionEvent) {
        previewing = Previewing.Reading(version.id, title)
        scope.launch {
            val shown = withContext(Dispatchers.IO) {
                try {
                    val reply = folderOp(folder, "preview-version", JSONObject().put("version", version.id))
                    Previewing.Text(version.id, title, parsePreview(reply))
                } catch (error: IOException) {
                    Previewing.Failed(version.id, title, Failure.of(error))
                } catch (error: JSONException) {
                    Previewing.Failed(version.id, title, Failure(null, "The app could not read the preview."))
                }
            }
            // Closed, or another version opened, while this was being read: drop it.
            if (previewing?.version == version.id) previewing = shown
        }
    }

    fun keep(conflict: FileConflict, version: VersionEvent) {
        // A second tap before the first is done would resolve what is already settled.
        if (working) return
        working = true
        failure = null
        scope.launch {
            try {
                withContext(Dispatchers.IO) {
                    folderOp(folder, "resolve", JSONObject().put("entity", conflict.entity).put("version", version.id))
                }
                // The choice is an event here: a burst takes it to the PC.
                SyncCoordinator.syncNow(context)
            } catch (error: IOException) {
                failure = Failure.of(error)
            }
            working = false
            changes++
        }
    }

    Column(Modifier.fillMaxSize()) {
        ScreenBar("Conflicts", onBack = onBack)
        Column(Modifier.weight(1f).padding(horizontal = 16.dp)) {
            FailureNote(failure)
            when (val rows = loaded) {
                Loaded.Loading -> Text("Loading...", color = MaterialTheme.colorScheme.onSurfaceVariant)
                is Loaded.Failed -> FailureNote(rows.failure)
                is Loaded.Rows -> if (rows.conflicts.isEmpty()) {
                    Text("Nothing to settle in $label.", color = MaterialTheme.colorScheme.onSurfaceVariant)
                } else {
                    Text(
                        "Both versions stay until you choose.",
                        style = MaterialTheme.typography.bodyLarge,
                        modifier = Modifier.padding(bottom = 8.dp),
                    )
                    LazyColumn(Modifier.fillMaxSize()) {
                        items(rows.conflicts, key = { it.entity }) { conflict ->
                            Text(
                                conflict.name,
                                style = MaterialTheme.typography.titleMedium,
                                modifier = Modifier.padding(top = 12.dp, bottom = 4.dp),
                            )
                            for (version in conflict.versions) {
                                val line = versionLine(version, rows.device, now, versionAuthor(version.author, rows.device, pcs))
                                Column(Modifier.fillMaxWidth().padding(vertical = 6.dp)) {
                                    Text(line, style = MaterialTheme.typography.bodyMedium)
                                    if (version.deleted) {
                                        Text(
                                            "The file will be removed from this phone and " + if (pcs <= 1) "My PC." else "the other devices.",
                                            style = MaterialTheme.typography.bodySmall,
                                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                                        )
                                    }
                                    Row(Modifier.padding(top = 4.dp), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                                        if (!version.deleted) {
                                            OutlinedButton(
                                                onClick = { preview(line, version) },
                                                enabled = !working,
                                                modifier = Modifier.heightIn(min = 44.dp),
                                            ) { Text("Preview") }
                                        }
                                        Button(
                                            onClick = { keep(conflict, version) },
                                            enabled = !working,
                                            modifier = Modifier.heightIn(min = 44.dp),
                                        ) { Text(if (version.deleted) "Delete the file" else "Keep this one") }
                                    }
                                }
                            }
                            HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
                        }
                    }
                }
            }
        }
    }

    previewing?.let { shown ->
        AlertDialog(
            onDismissRequest = { previewing = null },
            title = { Column { Text("Preview"); Text(shown.title, style = MaterialTheme.typography.bodyMedium) } },
            text = {
                Column(Modifier.verticalScroll(rememberScrollState())) {
                    when (shown) {
                        is Previewing.Reading -> Text("Reading...")
                        is Previewing.Text -> Text(shown.text, style = MaterialTheme.typography.bodySmall)
                        is Previewing.Failed -> FailureNote(shown.failure)
                    }
                }
            },
            confirmButton = { TextButton(onClick = { previewing = null }) { Text("Close") } },
        )
    }
}
