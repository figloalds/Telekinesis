package dev.tkfs.ui

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.ZoneOffset
import java.time.ZonedDateTime

class FolderLineTest {
    private val zone = ZoneOffset.UTC
    private fun at(hour: Int, minute: Int) =
        ZonedDateTime.of(2026, 11, 5, hour, minute, 0, 0, zone).toInstant().toEpochMilli()

    private val now = at(14, 4)
    private val synced = at(14, 2)
    private fun line(status: FolderStatus) = folderLine(status, now, zone)
    private fun folder(
        error: String? = null,
        conflicts: Int = 0,
        waiting: Int = 0,
        reachable: Boolean = true,
        lastSync: Long? = synced,
    ) = FolderStatus("id", "Documents", error, conflicts, reachable, lastSync, waiting)

    @Test fun conflictsOutrankWaitingAndWaitingOutranksSynced() {
        assertEquals(Line("2 conflicts to settle", Tone.Bad), line(folder(conflicts = 2, waiting = 3)))
        assertEquals(Line("1 conflict to settle", Tone.Bad), line(folder(conflicts = 1)))
        assertEquals(Line("3 files waiting to send", Tone.Waiting), line(folder(waiting = 3)))
        assertEquals(Line("1 file waiting to send", Tone.Waiting), line(folder(waiting = 1, reachable = false)))
        assertEquals(Line("Synced 2 min ago", Tone.Ok), line(folder()))
        // With the PC away and nothing waiting, the time of the last sync is the honest answer.
        assertEquals(Line("Synced at 14:02", Tone.Quiet), line(folder(reachable = false)))
        assertEquals(Line("Not synced yet", Tone.Quiet), line(folder(lastSync = null)))
        assertEquals(Line("Not synced yet", Tone.Quiet), line(folder(lastSync = null, reachable = false)))
        assertEquals("Synced just now", line(folder(lastSync = now - 20_000)).text)
        assertEquals("Synced 3 h ago", line(folder(lastSync = now - 3 * 3_600_000L)).text)
    }

    @Test fun aFolderErrorIsShownInWords() {
        val shown = line(folder(error = "WS_CONNECT_FAILED: refused ws://10.0.2.2:1", conflicts = 2, waiting = 1))
        assertEquals(Tone.Bad, shown.tone)
        assertFalse(shown.text, shown.text.contains("WS_CONNECT_FAILED"))
        assertTrue(shown.text, shown.text.startsWith("Could not reach"))
        // A code the app has no sentence for keeps the orchestrator's own words.
        assertEquals("the store is broken", line(folder(error = "FATAL_STORE_ERROR: the store is broken")).text)
    }

    @Test fun aRevokedPeerSaysToPairAgain() {
        val shown = line(folder(error = "PEER_REVOKED: the peer credential was not accepted"))
        assertEquals(Line("My PC no longer recognizes this phone. Pair again.", Tone.Bad), shown)
        assertTrue(needsPairing("PEER_REVOKED: anything"))
        assertFalse(needsPairing("WS_CONNECT_FAILED: x"))
    }

    @Test fun theStripNamesThePcAndWhenItWasLastSeen() {
        assertEquals("My PC isn't reachable. Last synced at 14:02.", unreachableStrip("My PC", synced, zone))
        assertEquals("My PC isn't reachable.", unreachableStrip("My PC", null, zone))
    }

    @Test fun aPcIsNamedMyPcOnlyWhileItIsTheOnlyOne() {
        assertEquals("My PC", pcName(listOf("ws://a:1")))
        assertEquals("ws://b:2", pcName(listOf("ws://a:1", "ws://b:2"), "ws://b:2"))
    }
}
