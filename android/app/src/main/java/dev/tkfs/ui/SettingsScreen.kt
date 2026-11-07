package dev.tkfs.ui

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp

@Composable
fun SettingsScreen(
    ui: AppUi,
    onBack: () -> Unit,
    onSyncOnMobile: (Boolean) -> Unit,
    onPeriod: (Int) -> Unit,
    onChangeAddress: (installation: String, endpoint: String) -> Unit,
    onUnpair: (installation: String) -> Unit,
    onAllowBattery: () -> Unit = {},
    onFreeSpace: () -> Unit = {},
) {
    val peers = ui.snapshot?.peers.orEmpty()
    Column(Modifier.fillMaxSize()) {
        ScreenBar("Settings", onBack = onBack)
        Column(Modifier.weight(1f).verticalScroll(rememberScrollState()).padding(horizontal = 16.dp)) {
            // The rows below the PCs are the places later tasks fill in.
            Setting("Sync on mobile data", "Off: only file lists update. Files you open still download.") {
                Switch(checked = ui.syncOnMobile, onCheckedChange = onSyncOnMobile)
            }
            Setting("Background check", "Every ${periodWords(ui.periodMinutes)}") {
                TextButton(onClick = { onPeriod(nextPeriod(ui.periodMinutes)) }) { Text("Change") }
            }
            KeepFree(ui, onFreeSpace)
            Setting("Battery", batteryLine(ui.batteryAllowed)) {
                if (!ui.batteryAllowed) TextButton(onClick = onAllowBattery) { Text("Allow") }
            }
            if (peers.isEmpty()) {
                Setting("My PC", "No PC is paired.")
            }
            for (peer in peers) {
                PcRow(pcName(peers.map { it.endpoint }, peer.endpoint), peer, ui.failure, onChangeAddress, onUnpair)
            }
            Setting("Diagnostics", "Copy and Resync from the start arrive in a later update.")
        }
    }
}

@Composable
private fun KeepFree(ui: AppUi, onFreeSpace: () -> Unit) {
    Column(Modifier.fillMaxWidth().padding(vertical = 10.dp)) {
        Text("Keep this much free", style = MaterialTheme.typography.bodyLarge)
        Text(floorLine(ui.floorBytes), style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.onSurfaceVariant)
        TextButton(onClick = onFreeSpace, enabled = !ui.freeingSpace) { Text("Free up space now") }
        ui.spaceNote?.let { Text(it, style = MaterialTheme.typography.bodyMedium) }
    }
    HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
}

@Composable
private fun Setting(name: String, detail: String, trailing: @Composable () -> Unit = {}) {
    Row(Modifier.fillMaxWidth().padding(vertical = 10.dp), verticalAlignment = Alignment.CenterVertically) {
        Column(Modifier.weight(1f)) {
            Text(name, style = MaterialTheme.typography.bodyLarge)
            Text(detail, style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
        trailing()
    }
    HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
}

@Composable
private fun PcRow(
    name: String,
    peer: PeerStatus,
    failure: Failure?,
    onChangeAddress: (String, String) -> Unit,
    onUnpair: (String) -> Unit,
) {
    var changing by remember { mutableStateOf(false) }
    var unpairing by remember { mutableStateOf(false) }
    Column(Modifier.fillMaxWidth().padding(vertical = 10.dp)) {
        Text(name, style = MaterialTheme.typography.bodyLarge)
        Text(peer.endpoint, style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.onSurfaceVariant)
        Row {
            TextButton(onClick = { changing = true }) { Text("Change address") }
            TextButton(onClick = { unpairing = true }) { Text("Unpair") }
        }
        FailureNote(failure)
    }
    HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)

    if (changing) {
        var address by remember { mutableStateOf(peer.endpoint) }
        AlertDialog(
            onDismissRequest = { changing = false },
            title = { Text("Change address") },
            text = {
                OutlinedTextField(value = address, onValueChange = { address = it }, label = { Text("PC address") }, singleLine = true)
            },
            confirmButton = {
                TextButton(
                    enabled = address.isNotBlank(),
                    onClick = { changing = false; onChangeAddress(peer.installation, address.trim()) },
                ) { Text("Change") }
            },
            dismissButton = { TextButton(onClick = { changing = false }) { Text("Cancel") } },
        )
    }
    if (unpairing) {
        AlertDialog(
            onDismissRequest = { unpairing = false },
            title = { Text("Unpair $name?") },
            text = { Text("This phone stops syncing with it. Folders already received stay here.") },
            confirmButton = {
                TextButton(onClick = { unpairing = false; onUnpair(peer.installation) }) { Text("Unpair") }
            },
            dismissButton = { TextButton(onClick = { unpairing = false }) { Text("Cancel") } },
        )
    }
}

/** The intervals "Background check" steps through; WorkManager runs a periodic job no more often than 15 min. */
private val PERIODS = listOf(15, 30, 60, 120)

private fun nextPeriod(current: Int): Int = PERIODS.firstOrNull { it > current } ?: PERIODS.first()

private fun periodWords(minutes: Int): String = if (minutes % 60 == 0 && minutes >= 60) "${minutes / 60} h" else "$minutes min"

/** What the Battery row says; when it is not allowed, Android may hold syncing back until the phone is awake. */
internal fun batteryLine(allowed: Boolean): String =
    if (allowed) "Allowed to run in the background" else "Not allowed. Sending files can wait until the phone is in use."
