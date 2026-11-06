package dev.tkfs.ui

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.BackHandler
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.material3.Surface
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import dev.tkfs.Native
import dev.tkfs.sync.SyncCoordinator

/** The app's one window: the screen is a value in the state, with no navigation library. */
class MainActivity : ComponentActivity() {
    private val model: AppModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Native.ensureStarted(this)
        SyncCoordinator.schedulePeriodic(this)
        setContent {
            TkfsTheme {
                Surface(Modifier.fillMaxSize()) {
                    val ui by model.state.collectAsState()
                    BackHandler(enabled = ui.screen != Screen.Folders) { model.show(Screen.Folders) }
                    when (ui.screen) {
                        Screen.Folders -> FoldersScreen(
                            ui,
                            onSync = model::syncNow,
                            onAddPc = { model.show(Screen.AddPc) },
                            onSettings = { model.show(Screen.Settings) },
                        )
                        Screen.AddPc -> AddPcScreen(
                            ui,
                            onBack = { model.show(Screen.Folders) },
                            onPair = model::pair,
                            onToggle = model::toggleChosen,
                            onReceive = model::receiveChosen,
                        )
                        Screen.Settings -> SettingsScreen(
                            ui,
                            onBack = { model.show(Screen.Folders) },
                            onSyncOnMobile = model::setSyncOnMobile,
                            onPeriod = model::setPeriodMinutes,
                            onChangeAddress = model::changeAddress,
                            onUnpair = model::unpair,
                        )
                    }
                }
            }
        }
    }

    override fun onStart() {
        super.onStart()
        SyncCoordinator.onForeground(this)
        model.startPolling()
    }

    override fun onStop() {
        model.stopPolling()
        super.onStop()
    }
}
