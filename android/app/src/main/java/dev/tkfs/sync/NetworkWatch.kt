package dev.tkfs.sync

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.os.Build
import android.util.Log

/**
 * Listens to the default network for as long as someone holds this, which is while a burst or the
 * transfer job runs, so nothing listens while idle. `changed` hears whether the network is metered as
 * soon as this starts listening, and whenever that may have changed, on Android's callback thread.
 */
internal class NetworkWatch(private val changed: (app: Context, metered: Boolean) -> Unit) {
    private var holders = 0
    private var callback: ConnectivityManager.NetworkCallback? = null

    @Synchronized fun hold(app: Context) {
        if (holders++ > 0) return
        val manager = app.getSystemService(ConnectivityManager::class.java) ?: return
        val listening = object : ConnectivityManager.NetworkCallback() {
            override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) {
                changed(app, capabilities.isMetered())
            }
        }
        try {
            manager.registerDefaultNetworkCallback(listening)
            callback = listening
        } catch (error: RuntimeException) {
            // Too many callbacks in this process, which is no reason to fail what asked: its burst stays
            // bounded, and the job's own network requirement still stops it.
            Log.w(TAG, "network: cannot listen: ${error.message}")
        }
    }

    @Synchronized fun release(app: Context) {
        if (--holders > 0) return
        val listening = callback ?: return
        callback = null
        app.getSystemService(ConnectivityManager::class.java)?.unregisterNetworkCallback(listening)
    }

    /** As `isActiveNetworkMetered` judges it, from what this callback was told: asking in a callback can get an older answer. */
    private fun NetworkCapabilities.isMetered(): Boolean =
        !hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED) &&
            !(Build.VERSION.SDK_INT >= Build.VERSION_CODES.R &&
                hasCapability(NetworkCapabilities.NET_CAPABILITY_TEMPORARILY_NOT_METERED))

    private companion object {
        const val TAG = "tkfs"
    }
}
