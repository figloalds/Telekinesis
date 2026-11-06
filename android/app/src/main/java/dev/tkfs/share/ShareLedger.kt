package dev.tkfs.share

import android.content.Context
import org.json.JSONArray
import org.json.JSONObject

/** Where the ledger's one piece of text lives: app preferences, or memory in a test. */
internal interface LedgerStore {
    fun read(): String?
    fun write(text: String)
}

private class PrefsStore(context: Context) : LedgerStore {
    private val prefs = context.applicationContext.getSharedPreferences("share", Context.MODE_PRIVATE)
    override fun read(): String? = prefs.getString(KEY, null)

    // `commit`, not `apply`: this is written right before a copy that a kill can interrupt.
    override fun write(text: String) {
        prefs.edit().putString(KEY, text).commit()
    }

    private companion object {
        const val KEY = "ledger"
    }
}

/** A working file nobody owns: to be deleted through the engine, so that its removal syncs. */
internal data class Orphan(val folder: String, val path: String)

/**
 * What sharing in has promised and what it has failed at, in preferences, so that a process
 * that is killed mid-copy is told about by the next one. A grant does not outlive a kill, so
 * what is recorded is a report, never a resumption.
 *
 * - A share is `begin`ning, with the names to save and then each working file in turn, until it
 *   `finish`es. One that is still here when no live share owns it was killed.
 * - A name that was not saved is a notice until the owner dismisses it, whether or not the
 *   notification permission is granted.
 */
internal class ShareLedger(private val store: LedgerStore) {
    private class Share(val id: String, val folder: String, val names: MutableList<String>, val parts: MutableList<String>)

    private class State(
        val shares: MutableList<Share> = mutableListOf(),
        val orphans: MutableList<Orphan> = mutableListOf(),
        val notices: MutableList<String> = mutableListOf(),
    )

    @Synchronized fun begin(id: String, folder: String, names: List<String>) =
        change { it.shares += Share(id, folder, names.toMutableList(), mutableListOf()) }

    /** A working file is about to be created. It is written down first, so a kill cannot hide it. */
    @Synchronized fun working(id: String, path: String) = change { state -> share(state, id)?.parts?.add(path) }

    /** The working file is gone or has its real name. */
    @Synchronized fun cleared(id: String, path: String) = change { state -> share(state, id)?.parts?.remove(path) }

    /** This name was saved, or failed and is being reported by the caller. */
    @Synchronized fun done(id: String, name: String) = change { state -> share(state, id)?.names?.remove(name) }

    /** The share ended. A working file still here could not be removed: it is swept later. */
    @Synchronized fun finish(id: String, failed: List<String>) = change { state ->
        share(state, id)?.let { share ->
            state.orphans += share.parts.map { Orphan(share.folder, it) }
            state.shares.remove(share)
        }
        addTo(state, failed)
    }

    /** Shares no live process owns were killed: their unsaved names become notices, their working files orphans. */
    @Synchronized fun recover(live: Set<String>) = change { state ->
        val dead = state.shares.filter { it.id !in live }
        for (share in dead) {
            addTo(state, share.names)
            state.orphans += share.parts.map { Orphan(share.folder, it) }
        }
        state.shares.removeAll(dead.toSet())
    }

    @Synchronized fun orphans(): List<Orphan> = read().orphans.toList()

    @Synchronized fun swept(orphan: Orphan) = change { it.orphans.remove(orphan) }

    @Synchronized fun notices(): List<String> = read().notices.toList()

    @Synchronized fun addNotices(names: List<String>) = change { addTo(it, names) }

    @Synchronized fun dismiss() = change { it.notices.clear() }

    private fun share(state: State, id: String) = state.shares.firstOrNull { it.id == id }

    private fun addTo(state: State, names: List<String>) {
        for (name in names) if (name !in state.notices) state.notices += name
    }

    private inline fun change(edit: (State) -> Unit) {
        val state = read()
        edit(state)
        store.write(write(state))
    }

    private fun read(): State = try {
        val json = JSONObject(store.read() ?: "{}")
        State(
            shares = objects(json.optJSONArray("shares")).mapTo(mutableListOf()) {
                Share(it.getString("id"), it.getString("folder"), strings(it.optJSONArray("names")), strings(it.optJSONArray("parts")))
            },
            orphans = objects(json.optJSONArray("orphans")).mapTo(mutableListOf()) { Orphan(it.getString("folder"), it.getString("path")) },
            notices = strings(json.optJSONArray("notices")),
        )
    } catch (error: org.json.JSONException) {
        // Whatever it held is lost; the next write starts it again.
        State()
    }

    private fun write(state: State): String = JSONObject()
        .put("shares", JSONArray(state.shares.map {
            JSONObject().put("id", it.id).put("folder", it.folder).put("names", JSONArray(it.names)).put("parts", JSONArray(it.parts))
        }))
        .put("orphans", JSONArray(state.orphans.map { JSONObject().put("folder", it.folder).put("path", it.path) }))
        .put("notices", JSONArray(state.notices))
        .toString()

    private fun objects(array: JSONArray?): List<JSONObject> =
        (0 until (array?.length() ?: 0)).map { array!!.getJSONObject(it) }

    private fun strings(array: JSONArray?): MutableList<String> =
        (0 until (array?.length() ?: 0)).mapTo(mutableListOf()) { array!!.getString(it) }

    companion object {
        private val shared = HashMap<Context, ShareLedger>()

        /** One per process, so its lock is the only one. */
        @Synchronized fun of(context: Context): ShareLedger =
            shared.getOrPut(context.applicationContext) { ShareLedger(PrefsStore(context)) }
    }
}
