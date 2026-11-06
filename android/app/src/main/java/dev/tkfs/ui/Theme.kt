package dev.tkfs.ui

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.unit.dp

/** The colours of the mockup: a pine primary, and no dynamic colour. */
private val light = lightColorScheme(
    primary = Color(0xFF0E6B5A),
    onPrimary = Color(0xFFFFFFFF),
    primaryContainer = Color(0xFFBFEBDD),
    onPrimaryContainer = Color(0xFF00382D),
    background = Color(0xFFF6F8F7),
    surface = Color(0xFFF6F8F7),
    onBackground = Color(0xFF17211F),
    onSurface = Color(0xFF17211F),
    surfaceVariant = Color(0xFFE6ECEA),
    onSurfaceVariant = Color(0xFF56635F),
    outlineVariant = Color(0xFFCFD8D5),
    error = Color(0xFFB3261E),
    errorContainer = Color(0xFFF9DEDC),
)

private val dark = darkColorScheme(
    primary = Color(0xFF7FD8C1),
    onPrimary = Color(0xFF00382D),
    primaryContainer = Color(0xFF0A4F42),
    onPrimaryContainer = Color(0xFFBFEBDD),
    background = Color(0xFF141A19),
    surface = Color(0xFF141A19),
    onBackground = Color(0xFFE3EBE8),
    onSurface = Color(0xFFE3EBE8),
    surfaceVariant = Color(0xFF222A28),
    onSurfaceVariant = Color(0xFFA9B6B2),
    outlineVariant = Color(0xFF38423F),
    error = Color(0xFFF2B8B5),
    errorContainer = Color(0xFF5C1A15),
)

/** The amber the mockup uses for "waiting", which Material has no role for. */
data class Amber(val text: Color, val soft: Color)

private val amberLight = Amber(Color(0xFF7D5200), Color(0xFFFFE3AE))
private val amberDark = Amber(Color(0xFFF2C063), Color(0xFF4D3600))

@Composable
fun amber(): Amber = if (isSystemInDarkTheme()) amberDark else amberLight

@Composable
fun toneColor(tone: Tone): Color = when (tone) {
    Tone.Ok -> MaterialTheme.colorScheme.primary
    Tone.Waiting -> amber().text
    Tone.Bad -> MaterialTheme.colorScheme.error
    Tone.Quiet -> MaterialTheme.colorScheme.onSurfaceVariant
}

@Composable
fun TkfsTheme(content: @Composable () -> Unit) {
    MaterialTheme(colorScheme = if (isSystemInDarkTheme()) dark else light, content = content)
}

/** The title row every screen starts with; `onBack` is null on the home screen. */
@Composable
fun ScreenBar(title: String, onBack: (() -> Unit)? = null, actions: @Composable RowScope.() -> Unit = {}) {
    Row(
        modifier = Modifier.fillMaxWidth().padding(start = 16.dp, end = 8.dp, top = 12.dp, bottom = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        if (onBack != null) {
            TextButton(onClick = onBack) {
                Text("←", modifier = Modifier.semantics { contentDescription = "Back" })
            }
        }
        Text(title, style = MaterialTheme.typography.titleLarge, modifier = Modifier.weight(1f))
        actions()
    }
}

/** A refusal in words, with its code small beneath. */
@Composable
fun FailureNote(failure: Failure?) {
    if (failure == null) return
    Column(Modifier.fillMaxWidth().padding(vertical = 8.dp)) {
        Text(failure.sentence, color = MaterialTheme.colorScheme.error)
        failure.code?.let {
            Text(it, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
    }
}
