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
        // `null` is an unknown: no peer answered. A folder in error is still waiting, since a
        // burst that ran out of its time reports an error with `remaining` already at 0.
        val waiting = burst.folders.filter { (it.remaining ?: 0) > 0 || it.error != null }
        val last = before
        before = burst.folders.mapNotNull { f -> f.remaining?.let { f.id to it } }.toMap()
        if (waiting.isEmpty()) return Next.Stop(Why.Done)
        val stuck = waiting.all { it.pushed == 0L && (it.error != null || last[it.id] == it.remaining) }
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

    /** A trigger found work left and wants the job: whether the caller must enqueue it. No change is recorded. */
    @Synchronized fun wanted(): Boolean {
        if (queued || working) return false
        queued = true
        return true
    }

    /**
     * The job ended without being stopped: whether it must be enqueued again, because a change
     * came after `claimed`. A job that ended on no progress with no new change asks for nothing.
     */
    @Synchronized fun resume(claimed: Long): Boolean {
        if (changes == claimed || queued || working) return false
        queued = true
        return true
    }

    /** Whether a job is enqueued and has not started. */
    @Synchronized fun waiting(): Boolean = queued && !working

    @Synchronized fun started() {
        queued = false
        working = true
    }

    /** The changes so far; a burst that begins after this covers them. */
    @Synchronized fun claim(): Long = changes

    /** Whether a change came after `claimed`. It changes nothing: the job goes on either way. */
    @Synchronized fun changedSince(claimed: Long): Boolean = changes != claimed

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

/**
 * One transfer job's rounds. What the owner just made goes first, and what is kept downloads with
 * nothing left to send. The gate stays working all along, so a change needs no job of its own: this
 * one hears of it through `changed`, stops the download, sends it and goes on with the download.
 * `send` says whether it ended with nothing left to send, and when it did not, nothing is downloaded:
 * no PC is taking what it was given. `claimed` is what its last burst covered.
 */
internal suspend fun transferRounds(
    gate: TransferGate,
    claimed: () -> Long,
    send: suspend () -> Boolean,
    download: suspend (changed: () -> Boolean) -> Unit,
) {
    while (true) {
        if (!send()) return
        download { gate.changedSince(claimed()) }
        if (gate.finish(claimed())) return
    }
}
