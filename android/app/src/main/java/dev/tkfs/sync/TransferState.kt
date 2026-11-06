package dev.tkfs.sync

enum class Why { Done, NoProgress, Failed }

sealed interface Next {
    data object Again : Next
    data class Stop(val why: Why) : Next
}

/**
 * Whether the transfer job runs another burst. A folder is stuck when its burst pushed nothing
 * and either failed or left the `remaining` the last burst left; the job ends when every folder
 * that still has work is stuck. Folders are compared one by one, since a total could hide one
 * folder standing still behind another that moved. A burst that ran out of its time reports an
 * error too, and it is not stuck if it pushed.
 */
class Progress {
    private var before: Map<String, Int> = emptyMap()

    fun after(burst: BurstResult): Next {
        if (burst.failure != null) return Next.Stop(Why.Failed)
        // `null` is an unknown: no peer answered.
        val open = burst.folders.filter { (it.remaining ?: 0) > 0 }
        val last = before
        before = burst.folders.mapNotNull { f -> f.remaining?.let { f.id to it } }.toMap()
        if (open.isEmpty()) return if (burst.errors.isEmpty()) Next.Stop(Why.Done) else Next.Stop(Why.NoProgress)
        val stuck = open.all { it.pushed == 0L && (it.error != null || last[it.id] == it.remaining) }
        return if (stuck) Next.Stop(Why.NoProgress) else Next.Again
    }
}

/**
 * What `onLocalChange` and the transfer job agree on, so a change is never left without a job
 * and the job is not asked for twice. Pure: callers pass the clock.
 */
class TransferGate {
    private var changes = 0L
    private var lastChangeMs = 0L
    private var queued = false
    private var working = false

    /** Whether the caller must enqueue the job. */
    @Synchronized fun changed(nowMs: Long): Boolean {
        changes++
        lastChangeMs = nowMs
        if (queued || working) return false
        queued = true
        return true
    }

    /** Enqueueing failed, so the next change must try again. */
    @Synchronized fun notQueued() {
        queued = false
    }

    /** Whether a job is enqueued and has not started. */
    @Synchronized fun waiting(): Boolean = queued && !working

    @Synchronized fun started() {
        queued = false
        working = true
    }

    /** The changes so far; a burst that begins after this covers them. */
    @Synchronized fun claim(): Long = changes

    /** Whether the job may end: false when a change came after `claimed`. */
    @Synchronized fun finish(claimed: Long): Boolean {
        if (changes != claimed) return false
        working = false
        return true
    }

    /** The job ends however it ends. */
    @Synchronized fun stopped() {
        working = false
    }

    /** How long until the last change is `DEBOUNCE` old; 0 if it already is, or there was none. */
    @Synchronized fun quietFor(nowMs: Long): Long =
        if (lastChangeMs == 0L) 0 else maxOf(0, lastChangeMs + SyncPolicy.LOCAL_CHANGE_DEBOUNCE_MS - nowMs)
}
