package dev.tkfs

import android.content.Context
import android.content.SharedPreferences
import dev.tkfs.sync.SyncPolicy

/** What the owner chose, and when each folder last synced (the clock the sync policy reads). */
class Settings private constructor(private val prefs: SharedPreferences) {
    /** Off by default: on a metered network only file lists update. */
    var syncOnMobile: Boolean
        get() = prefs.getBoolean(MOBILE, false)
        set(value) = prefs.edit().putBoolean(MOBILE, value).apply()

    var periodMinutes: Int
        get() = prefs.getInt(PERIOD, SyncPolicy.DEFAULT_PERIOD_MINUTES)
        set(value) = prefs.edit().putInt(PERIOD, value).apply()

    /** When a burst last ended with this folder in good order; 0 if never. */
    fun lastSyncMs(folder: String): Long = prefs.getLong(LAST + folder, 0)

    /** The newest of every folder's, 0 if none. */
    fun lastSyncMs(): Long =
        prefs.all.filterKeys { it.startsWith(LAST) }.values.filterIsInstance<Long>().maxOrNull() ?: 0

    fun setLastSyncMs(folder: String, ms: Long) = prefs.edit().putLong(LAST + folder, ms).apply()

    companion object {
        private const val MOBILE = "sync_on_mobile"
        private const val PERIOD = "period_minutes"
        private const val LAST = "last_sync:"

        fun of(context: Context) =
            Settings(context.applicationContext.getSharedPreferences("settings", Context.MODE_PRIVATE))
    }
}
