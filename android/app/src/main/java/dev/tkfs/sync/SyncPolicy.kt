package dev.tkfs.sync

enum class Trigger { Foreground, Manual, Listing, Periodic, LocalChange }

/** What a burst moves; `Skip` is no burst. */
enum class Mode { Full, MetadataOnly, Skip }

/** Whether a trigger runs a burst, and which. Pure: the caller passes the clock. */
class SyncPolicy {
    fun decide(trigger: Trigger, nowMs: Long, lastSyncMs: Long, metered: Boolean, syncOnMobile: Boolean): Mode {
        // "Sync now" is the owner asking, so it moves files on any network.
        if (trigger == Trigger.Manual) return Mode.Full
        val age = nowMs - lastSyncMs
        val idle = metered && !syncOnMobile
        return when (trigger) {
            Trigger.Foreground -> if (age < FOREGROUND_EVERY_MS) Mode.Skip else if (idle) Mode.MetadataOnly else Mode.Full
            Trigger.Listing -> if (age < LISTING_STALE_MS) Mode.Skip else if (idle) Mode.MetadataOnly else Mode.Full
            Trigger.Periodic -> if (idle) Mode.MetadataOnly else Mode.Full
            // Files the owner made wait for an unmetered network.
            Trigger.LocalChange -> if (idle) Mode.Skip else Mode.Full
            Trigger.Manual -> Mode.Full
        }
    }

    companion object {
        const val FOREGROUND_EVERY_MS = 30_000L
        const val LISTING_STALE_MS = 120_000L
        const val LOCAL_CHANGE_DEBOUNCE_MS = 5_000L
        const val DEFAULT_PERIOD_MINUTES = 30
    }
}
