package dev.tkfs.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.pulltorefresh.PullToRefreshBox
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import dev.tkfs.R

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun FoldersScreen(ui: AppUi, onSync: () -> Unit, onAddPc: () -> Unit, onSettings: () -> Unit) {
    val snapshot = ui.snapshot
    Column(Modifier.fillMaxSize()) {
        ScreenBar("Telekinesis") { TextButton(onClick = onSettings) { Text("Settings") } }
        // Pulling down is the same as "Sync now".
        PullToRefreshBox(isRefreshing = ui.syncing, onRefresh = onSync, modifier = Modifier.weight(1f)) {
            Column(Modifier.fillMaxSize().padding(horizontal = 16.dp)) {
                when {
                    snapshot == null -> Text("Starting...", color = MaterialTheme.colorScheme.onSurfaceVariant)
                    snapshot.folders.isEmpty() -> Empty(snapshot, onAddPc)
                    else -> FolderList(snapshot, ui.now, onAddPc)
                }
                FailureNote(ui.failure ?: ui.pollFailure)
            }
        }
        Column(Modifier.padding(start = 16.dp, end = 16.dp, top = 12.dp, bottom = 16.dp)) {
            Button(
                onClick = onSync,
                enabled = !ui.syncing && snapshot != null && snapshot.peers.isNotEmpty(),
                modifier = Modifier.fillMaxWidth().heightIn(min = 44.dp),
            ) { Text(if (ui.syncing) "Syncing..." else "Sync now") }
            if (ui.storage.isNotEmpty()) {
                Text(
                    ui.storage,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center,
                    modifier = Modifier.fillMaxWidth().padding(top = 10.dp),
                )
            }
        }
    }
}

@Composable
private fun Empty(snapshot: Snapshot, onAddPc: () -> Unit) {
    Column(verticalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.padding(top = 8.dp)) {
        Strips(snapshot, onAddPc)
        Text(
            if (snapshot.peers.isEmpty()) "No PC is paired yet." else "No folders have been received yet.",
            style = MaterialTheme.typography.bodyLarge,
        )
        Button(onClick = onAddPc) { Text("Add a PC") }
    }
}

@Composable
private fun FolderList(snapshot: Snapshot, now: Long, onAddPc: () -> Unit) {
    LazyColumn(Modifier.fillMaxWidth()) {
        item { Strips(snapshot, onAddPc) }
        items(snapshot.folders, key = { it.id }) { folder ->
            val line = folderLine(folder, now)
            Row(
                Modifier.fillMaxWidth().padding(vertical = 12.dp),
                horizontalArrangement = Arrangement.spacedBy(14.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Icon(
                    painterResource(R.drawable.ic_folder),
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(22.dp),
                )
                Column(Modifier.weight(1f)) {
                    Text(folder.label, style = MaterialTheme.typography.bodyLarge, maxLines = 1)
                    Text(line.text, color = toneColor(line.tone), style = MaterialTheme.typography.bodyMedium)
                }
            }
            HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
        }
    }
}

/** The one loud thing above the folders: the PC is away, or it no longer knows this phone. */
@Composable
private fun Strips(snapshot: Snapshot, onAddPc: () -> Unit) {
    when {
        snapshot.revoked -> Row(
            Modifier.fillMaxWidth().padding(bottom = 8.dp)
                .background(MaterialTheme.colorScheme.errorContainer, RoundedCornerShape(12.dp))
                .padding(start = 12.dp, top = 4.dp, bottom = 4.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text(
                "${snapshot.pcName} no longer recognizes this phone.",
                Modifier.weight(1f),
                style = MaterialTheme.typography.bodyMedium,
            )
            TextButton(onClick = onAddPc) { Text("Pair again") }
        }

        snapshot.unreachable -> Text(
            unreachableStrip(snapshot.pcName, snapshot.lastSyncMs),
            style = MaterialTheme.typography.bodyMedium,
            modifier = Modifier.fillMaxWidth().padding(bottom = 8.dp)
                .background(amber().soft, RoundedCornerShape(12.dp))
                .padding(horizontal = 12.dp, vertical = 10.dp),
        )
    }
}
