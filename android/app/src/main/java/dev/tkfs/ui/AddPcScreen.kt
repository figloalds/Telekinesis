package dev.tkfs.ui

import android.content.Context
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.imePadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Checkbox
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import com.google.mlkit.vision.barcode.common.Barcode
import com.google.mlkit.vision.codescanner.GmsBarcodeScannerOptions
import com.google.mlkit.vision.codescanner.GmsBarcodeScanning
import dev.tkfs.PairLink

@Composable
fun AddPcScreen(
    ui: AppUi,
    onBack: () -> Unit,
    onPair: (endpoint: String, code: String) -> Unit,
    onToggle: (repo: String) -> Unit,
    onReceive: () -> Unit,
) {
    Column(Modifier.fillMaxSize().imePadding()) {
        ScreenBar("Add a PC", onBack = onBack)
        when (val pairing = ui.pairing) {
            is Pairing.Choose -> Choose(pairing, ui.failure, onToggle, onReceive)
            else -> Fields(working = pairing == Pairing.Working, ui.failure, onPair)
        }
    }
}

@Composable
private fun ColumnScope.Fields(working: Boolean, failure: Failure?, onPair: (String, String) -> Unit) {
    val context = LocalContext.current
    var endpoint by remember { mutableStateOf("") }
    var code by remember { mutableStateOf("") }
    var scanNote by remember { mutableStateOf<String?>(null) }

    // A pasted link fills both fields, wherever it lands.
    fun fill(text: String): Boolean {
        val link = PairLink.parse(text) ?: return false
        link.endpoint?.let { endpoint = it }
        code = link.code
        return true
    }

    val ready = endpoint.isNotBlank() && PairLink.isCode(code.trim()) && !working
    Column(Modifier.weight(1f).verticalScroll(rememberScrollState()).padding(horizontal = 16.dp)) {
        Text(
            "On your PC, create a new pairing code in Telekinesis. It shows a QR code.",
            style = MaterialTheme.typography.bodyLarge,
            modifier = Modifier.padding(bottom = 14.dp),
        )
        Button(
            onClick = { scan(context, onLink = { if (!fill(it)) scanNote = "That QR code is not a Telekinesis pairing code." }, onFail = { scanNote = it }) },
            modifier = Modifier.fillMaxWidth().heightIn(min = 44.dp),
        ) { Text("Scan QR code") }
        scanNote?.let { Text(it, color = MaterialTheme.colorScheme.error, modifier = Modifier.padding(top = 8.dp)) }
        Text(
            "or enter it yourself",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = TextAlign.Center,
            modifier = Modifier.fillMaxWidth().padding(top = 14.dp, bottom = 2.dp),
        )
        OutlinedTextField(
            value = endpoint,
            onValueChange = { if (!fill(it)) endpoint = it },
            label = { Text("PC address") },
            singleLine = true,
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri),
            modifier = Modifier.fillMaxWidth().padding(top = 12.dp).semantics { contentDescription = "PC address" },
        )
        OutlinedTextField(
            value = code,
            onValueChange = { if (!fill(it)) code = it },
            label = { Text("Pairing code, 64 characters") },
            singleLine = true,
            visualTransformation = PasswordVisualTransformation(),
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password),
            modifier = Modifier.fillMaxWidth().padding(top = 12.dp).semantics { contentDescription = "Pairing code" },
        )
        FailureNote(failure)
    }
    Column(Modifier.padding(start = 16.dp, end = 16.dp, top = 12.dp, bottom = 16.dp)) {
        Button(
            onClick = { onPair(endpoint.trim(), code.trim()) },
            enabled = ready,
            colors = ButtonDefaults.buttonColors(
                containerColor = MaterialTheme.colorScheme.primaryContainer,
                contentColor = MaterialTheme.colorScheme.onPrimaryContainer,
            ),
            modifier = Modifier.fillMaxWidth().heightIn(min = 44.dp),
        ) { Text(if (working) "Pairing..." else "Pair") }
    }
}

@Composable
private fun ColumnScope.Choose(pairing: Pairing.Choose, failure: Failure?, onToggle: (String) -> Unit, onReceive: () -> Unit) {
    Column(Modifier.weight(1f).verticalScroll(rememberScrollState()).padding(horizontal = 16.dp)) {
        Text(
            "Paired. Pick the folders to receive from ${pairing.endpoint}.",
            style = MaterialTheme.typography.bodyLarge,
            modifier = Modifier.padding(bottom = 8.dp),
        )
        for (folder in pairing.published) {
            Row(
                Modifier.fillMaxWidth().clickable { onToggle(folder.repo) }.padding(vertical = 4.dp),
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Checkbox(checked = folder.repo in pairing.chosen, onCheckedChange = { onToggle(folder.repo) })
                Text(folder.label, style = MaterialTheme.typography.bodyLarge)
            }
            HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
        }
        FailureNote(failure)
    }
    Column(Modifier.padding(start = 16.dp, end = 16.dp, top = 12.dp, bottom = 16.dp)) {
        val count = pairing.chosen.size
        Button(
            onClick = onReceive,
            enabled = count > 0,
            modifier = Modifier.fillMaxWidth().heightIn(min = 44.dp),
        ) { Text(if (count == 1) "Receive 1 folder" else "Receive $count folders") }
    }
}

/** Google's code scanner, which needs no camera permission and returns the QR's text. */
private fun scan(context: Context, onLink: (String) -> Unit, onFail: (String) -> Unit) {
    val options = GmsBarcodeScannerOptions.Builder().setBarcodeFormats(Barcode.FORMAT_QR_CODE).build()
    GmsBarcodeScanning.getClient(context, options).startScan()
        .addOnSuccessListener { barcode -> onLink(barcode.rawValue.orEmpty()) }
        .addOnCanceledListener { }
        .addOnFailureListener {
            onFail("The scanner is not available on this phone. Paste the link or the code instead.")
        }
}
