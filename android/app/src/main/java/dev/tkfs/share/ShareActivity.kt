package dev.tkfs.share

import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedCard
import androidx.compose.material3.RadioButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.unit.dp
import dev.tkfs.Native
import dev.tkfs.Settings
import dev.tkfs.ui.MainActivity
import dev.tkfs.ui.ScreenBar
import dev.tkfs.ui.TkfsTheme
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONArray
import java.io.IOException

private data class Target(val id: String, val label: String)

/**
 * The share target: "Save to Telekinesis", the folders to pick from and where inside the folder.
 * It does not copy anything itself. Its job ends by starting [ShareImportService], which holds
 * the read grant for as long as it copies.
 */
class ShareActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val uris = sharedUris(intent)
        val mimeTypes = sharedMimeTypes(intent, uris)
        val source = referrerLabel()
        setContent {
            TkfsTheme {
                Surface(Modifier.fillMaxSize()) {
                    SaveScreen(
                        lead = leadLine(mimeTypes, source),
                        count = uris.size,
                        onSave = { target, directory ->
                            val settings = Settings.of(this)
                            settings.lastShareFolder = target.id
                            settings.lastSharePath = directory
                            ShareImportService.start(this, target.id, target.label, directory, uris)
                            Toast.makeText(this, "Saving ${uris.size} ${if (uris.size == 1) "file" else "files"}", Toast.LENGTH_SHORT).show()
                            finish()
                        },
                        onOpenApp = {
                            startActivity(Intent(this, MainActivity::class.java))
                            finish()
                        },
                    )
                }
            }
        }
    }

    /** The app that shared, by its name, when Android lets this app see it. */
    private fun referrerLabel(): String? {
        val name = referrer?.takeIf { it.scheme == "android-app" }?.host ?: return null
        return try {
            packageManager.getApplicationLabel(packageManager.getApplicationInfo(name, 0)).toString()
        } catch (error: Exception) {
            null
        }
    }

    private fun sharedUris(intent: Intent): List<Uri> {
        val streams: List<Uri> = when (intent.action) {
            Intent.ACTION_SEND_MULTIPLE -> uriList(intent)
            else -> listOfNotNull(uri(intent))
        }
        if (streams.isNotEmpty()) return streams.distinct()
        // Some senders give the grant on `ClipData` and no extra.
        val clip = intent.clipData ?: return emptyList()
        return (0 until clip.itemCount).mapNotNull { clip.getItemAt(it).uri }.distinct()
    }

    @Suppress("DEPRECATION")
    private fun uri(intent: Intent): Uri? =
        if (Build.VERSION.SDK_INT >= 33) intent.getParcelableExtra(Intent.EXTRA_STREAM, Uri::class.java)
        else intent.getParcelableExtra(Intent.EXTRA_STREAM)

    @Suppress("DEPRECATION")
    private fun uriList(intent: Intent): List<Uri> =
        (if (Build.VERSION.SDK_INT >= 33) intent.getParcelableArrayListExtra(Intent.EXTRA_STREAM, Uri::class.java)
        else intent.getParcelableArrayListExtra<Uri>(Intent.EXTRA_STREAM)).orEmpty()

    private fun sharedMimeTypes(intent: Intent, uris: List<Uri>): List<String> =
        uris.map { contentResolver.getType(it) ?: intent.type ?: "*/*" }
}

@Composable
private fun SaveScreen(lead: String, count: Int, onSave: (Target, String) -> Unit, onOpenApp: () -> Unit) {
    val context = androidx.compose.ui.platform.LocalContext.current
    var targets by remember { mutableStateOf<List<Target>?>(null) }
    var chosen by remember { mutableStateOf<Target?>(null) }
    var directory by remember { mutableStateOf("/") }
    var picking by remember { mutableStateOf(false) }

    LaunchedEffect(Unit) {
        val found = withContext(Dispatchers.IO) {
            try {
                Native.ensureStarted(context)
                val folders = JSONArray(Native.manage("list", "{}"))
                (0 until folders.length()).map { folders.getJSONObject(it) }.map { Target(it.getString("id"), it.getString("label")) }
            } catch (error: IOException) {
                emptyList()
            }
        }
        val settings = Settings.of(context)
        val last = found.firstOrNull { it.id == settings.lastShareFolder }
        targets = found
        chosen = last ?: found.firstOrNull()
        if (last != null) directory = settings.lastSharePath
    }

    Column(Modifier.fillMaxSize()) {
        ScreenBar("Save to Telekinesis")
        val shown = targets
        if (shown != null && shown.isEmpty()) {
            Column(Modifier.weight(1f).padding(horizontal = 16.dp)) {
                Text("No folder is here yet.", style = MaterialTheme.typography.bodyLarge)
                Text(
                    "Add a PC in Telekinesis and receive a folder, then share again.",
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            Row(Modifier.fillMaxWidth().padding(16.dp)) { Button(onClick = onOpenApp, modifier = Modifier.fillMaxWidth()) { Text("Open Telekinesis") } }
            return@Column
        }
        Column(
            Modifier.weight(1f).verticalScroll(rememberScrollState()).padding(horizontal = 16.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp),
        ) {
            Text(lead, style = MaterialTheme.typography.bodyLarge, modifier = Modifier.padding(bottom = 8.dp))
            for (target in shown.orEmpty()) {
                Row(
                    Modifier.fillMaxWidth().selectable(
                        selected = target == chosen,
                        role = Role.RadioButton,
                        onClick = { if (chosen != target) { chosen = target; directory = "/" } },
                    ).padding(vertical = 4.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    RadioButton(selected = target == chosen, onClick = null)
                    Text(target.label, modifier = Modifier.padding(start = 12.dp), style = MaterialTheme.typography.bodyLarge)
                }
            }
            chosen?.let { target ->
                OutlinedCard(Modifier.fillMaxWidth().padding(top = 12.dp).clickable { picking = true }) {
                    Column(Modifier.padding(12.dp)) {
                        Text("Inside this folder", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                        Text(insideLine(target.label, directory), style = MaterialTheme.typography.bodyLarge)
                    }
                }
            }
        }
        Column(Modifier.fillMaxWidth().padding(16.dp)) {
            Button(
                onClick = { chosen?.let { onSave(it, directory) } },
                enabled = chosen != null && count > 0,
                modifier = Modifier.fillMaxWidth(),
            ) { Text(saveLabel(count)) }
            Text(
                "They go to My PC when it's reachable.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(top = 8.dp),
            )
        }
    }

    val target = chosen
    if (picking && target != null) {
        DirectoryPicker(target, directory, onPick = { directory = it; picking = false }, onDismiss = { picking = false })
    }
}

/** Walks down a folder's directories; "Use this folder" picks the one it shows. */
@Composable
private fun DirectoryPicker(target: Target, start: String, onPick: (String) -> Unit, onDismiss: () -> Unit) {
    var path by remember { mutableStateOf(start) }
    var names by remember { mutableStateOf<List<String>?>(null) }
    LaunchedEffect(path) {
        names = null
        names = withContext(Dispatchers.IO) {
            try {
                val entries = JSONArray(Native.list(target.id, path))
                (0 until entries.length()).map { entries.getJSONObject(it) }
                    .filter { it.getBoolean("directory") }.map { it.getString("name") }
                    .filterNot { it.startsWith(".") }.sorted()
            } catch (error: IOException) {
                emptyList()
            }
        }
    }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(insideLine(target.label, path)) },
        text = {
            Column(Modifier.verticalScroll(rememberScrollState())) {
                if (path != "/") {
                    PickerRow("..") { path = path.substringBeforeLast('/').ifEmpty { "/" } }
                }
                for (name in names.orEmpty()) PickerRow(name) { path = (if (path == "/") "" else path) + "/" + name }
                if (names != null && names.orEmpty().isEmpty()) {
                    Text("No folders in here.", color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
            }
        },
        confirmButton = { TextButton(onClick = { onPick(path) }) { Text("Use this folder") } },
        dismissButton = { TextButton(onClick = onDismiss) { Text("Cancel") } },
    )
}

@Composable
private fun PickerRow(name: String, onClick: () -> Unit) {
    Text(name, modifier = Modifier.fillMaxWidth().clickable(onClick = onClick).padding(vertical = 12.dp))
    HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
}
