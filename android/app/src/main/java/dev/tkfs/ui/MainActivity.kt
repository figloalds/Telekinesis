package dev.tkfs.ui

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.Settings as AndroidSettings
import androidx.activity.ComponentActivity
import androidx.activity.compose.BackHandler
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.material3.Surface
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import dev.tkfs.Native
import dev.tkfs.Settings
import dev.tkfs.share.ShareImports
import dev.tkfs.sync.SyncCoordinator

/** The app's one window: the screen is a value in the state, with no navigation library. */
class MainActivity : ComponentActivity() {
    private val model: AppModel by viewModels()

    /** The answer changes nothing here: without it the jobs still run, only without a visible notification. */
    private val notifications = registerForActivityResult(ActivityResultContracts.RequestPermission()) {}

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Native.ensureStarted(this)
        SyncCoordinator.schedulePeriodic(this)
        // A kill mid-copy left a report and working files; the provider does the same when it starts first.
        ShareImports.recoverInBackground(this)
        setContent {
            TkfsTheme {
                Surface(Modifier.fillMaxSize()) {
                    val ui by model.state.collectAsState()
                    // The owner has just added a PC: the first moment a notification has a use.
                    val choosing = ui.pairing is Pairing.Choose
                    LaunchedEffect(choosing) { if (choosing) askForNotifications() }
                    BackHandler(enabled = ui.screen != Screen.Folders) { model.show(Screen.Folders) }
                    when (val screen = ui.screen) {
                        Screen.Folders -> FoldersScreen(
                            ui,
                            onSync = model::syncNow,
                            onAddPc = { model.show(Screen.AddPc) },
                            onSettings = { model.show(Screen.Settings) },
                            onDismissShareFailures = model::dismissShareFailures,
                            onDismissOpenFailures = model::dismissOpenFailures,
                            onOpenFolder = { model.show(Screen.Browse(it.id, it.label)) },
                        )
                        is Screen.Browse -> BrowseScreen(
                            screen.folder,
                            screen.label,
                            waitingFiles = ui.snapshot?.folders?.firstOrNull { it.id == screen.folder }?.waiting ?: 0,
                            onBack = { model.show(Screen.Folders) },
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
                            onAllowBattery = ::askForBatteryExemption,
                        )
                    }
                }
            }
        }
    }

    /** Once, from Android 13, where it is a runtime permission. */
    private fun askForNotifications() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) return
        val settings = Settings.of(this)
        if (settings.notificationsAsked) return
        if (checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED) return
        settings.notificationsAsked = true
        notifications.launch(Manifest.permission.POST_NOTIFICATIONS)
    }

    private fun askForBatteryExemption() {
        startActivity(
            Intent(AndroidSettings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS, Uri.parse("package:$packageName")),
        )
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
