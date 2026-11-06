package dev.tkfs.provider

import android.content.Context
import org.json.JSONArray
import org.json.JSONException

/** Where the notices live: app preferences, or memory in a test. */
internal interface NoticeStore {
    fun read(): String?
    fun write(text: String)
}

private class PrefsNotices(context: Context) : NoticeStore {
    private val prefs = context.applicationContext.getSharedPreferences("opens", Context.MODE_PRIVATE)
    override fun read(): String? = prefs.getString("unreachable", null)
    override fun write(text: String) {
        prefs.edit().putString("unreachable", text).apply()
    }
}

/**
 * The files that could not open because no PC answered, until the owner dismisses them. Like a
 * share's failures they stay on the Folders screen whether or not the notification permission
 * is granted, since a viewer's own error message says nothing of why.
 */
internal class OpenNotices(private val store: NoticeStore) {
    @Synchronized fun names(): List<String> = read()

    @Synchronized fun add(name: String) {
        val names = read()
        if (name !in names) store.write(JSONArray(names + name).toString())
    }

    @Synchronized fun dismiss() = store.write("[]")

    private fun read(): List<String> = try {
        val array = JSONArray(store.read() ?: "[]")
        (0 until array.length()).map { array.getString(it) }
    } catch (error: JSONException) {
        emptyList()
    }

    companion object {
        private val shared = HashMap<Context, OpenNotices>()

        /** One per process, so its lock is the only one. */
        @Synchronized fun of(context: Context): OpenNotices =
            shared.getOrPut(context.applicationContext) { OpenNotices(PrefsNotices(context)) }
    }
}
