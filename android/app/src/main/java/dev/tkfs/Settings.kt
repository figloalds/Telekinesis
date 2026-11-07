package dev.tkfs

import android.content.Context
import android.content.SharedPreferences
import dev.tkfs.sync.FloorChecked
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

    /** The folder and directory the owner last shared into, so the next share starts there. */
    var lastShareFolder: String?
        get() = prefs.getString(SHARE_FOLDER, null)
        set(value) = prefs.edit().putString(SHARE_FOLDER, value).apply()

    var lastSharePath: String
        get() = prefs.getString(SHARE_PATH, "/") ?: "/"
        set(value) = prefs.edit().putString(SHARE_PATH, value).apply()

    /** Whether the notification permission has been asked for, which is done once. */
    var notificationsAsked: Boolean
        get() = prefs.getBoolean(NOTIFICATIONS_ASKED, false)
        set(value) = prefs.edit().putBoolean(NOTIFICATIONS_ASKED, value).apply()

    /** What the last freeing below the floor left, across a restart: a cached app's process comes and goes. */
    var floorChecked: FloorChecked?
        get() = if (prefs.contains(FLOOR_AT)) FloorChecked(prefs.getLong(FLOOR_AT, 0), prefs.getLong(FLOOR_FREE, 0)) else null
        set(value) {
            val edit = prefs.edit()
            if (value == null) edit.remove(FLOOR_AT).remove(FLOOR_FREE) else edit.putLong(FLOOR_AT, value.atMs).putLong(FLOOR_FREE, value.freeBytes)
            edit.apply()
        }

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
        private const val SHARE_FOLDER = "share_folder"
        private const val SHARE_PATH = "share_path"
        private const val NOTIFICATIONS_ASKED = "notifications_asked"
        private const val FLOOR_AT = "floor_checked_ms"
        private const val FLOOR_FREE = "floor_checked_free"

        fun of(context: Context) =
            Settings(context.applicationContext.getSharedPreferences("settings", Context.MODE_PRIVATE))
    }
}
