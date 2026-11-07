package dev.tkfs.ui

import dev.tkfs.sync.BurstError
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** The shapes `status` really sends: `folder_entry` and `Network::peers`, as seen in ui_folders. */
class SnapshotTest {
    private fun synced(sync: String) = """{"folders": [{"record": {"id": "f", "label": "L"}, "sync": $sync}], "peers": []}"""

    /** After a process death nothing in memory has it, and what is stored is still true. */
    @Test fun aFolderWithNoExchangeInMemoryGoesByTheStoredTime() {
        val stored = { folder: String -> if (folder == "f") 1_000L else 0L }
        assertEquals(1_000L, parseSnapshot(synced("{}"), storedSyncMs = stored).folders[0].lastSyncMs)
        assertEquals(1_000L, parseSnapshot(synced("""{"pc": {}}"""), storedSyncMs = stored).lastSyncMs)
    }

    /** A slot exists with 0 before its first exchange finishes: that is no time at all, not 1970. */
    @Test fun anExchangeTimeOfZeroIsNone() {
        val zero = synced("""{"pc": {"last_exchange_ms": 0}}""")
        assertEquals(null, parseSnapshot(zero).folders[0].lastSyncMs)
        assertEquals(1_000L, parseSnapshot(zero, storedSyncMs = { 1_000L }).folders[0].lastSyncMs)
        assertEquals(emptyMap<String, Long>(), parseExchanges(zero))
    }

    @Test fun nothingStoredIsNone() {
        assertEquals(null, parseSnapshot(synced("{}"), storedSyncMs = { 0L }).folders[0].lastSyncMs)
    }

    @Test fun anExchangeInMemoryGoesBeforeTheStoredTime() {
        val live = synced("""{"pc": {"last_exchange_ms": 5000}}""")
        assertEquals(5_000L, parseSnapshot(live, storedSyncMs = { 9_000L }).folders[0].lastSyncMs)
    }

    /** What clears a burst's error is a newer exchange, and the stored time is not one. */
    @Test fun theStoredTimeDoesNotClearABurstsError() {
        val bursts = mapOf("f" to BurstError("SYNC_NOW_TIMEOUT: x", 1_000))
        val folder = parseSnapshot(synced("{}"), bursts, storedSyncMs = { 9_000L }).folders[0]
        assertEquals("SYNC_NOW_TIMEOUT: x", folder.error)
        assertEquals(9_000L, folder.lastSyncMs)
    }

    private fun status(lane: String) = """
        {"daemon": {}, "folders": [{
            "record": {"id": "f1", "repo": "r1", "label": "Documents", "state": "/x"},
            "phase": {"phase": "Running"}, "heartbeat": {"conflicts": 2, "error": null},
            "conflicts": 2, "error": null,
            "sync": {"ws://10.0.2.2:1": {"sent": 1, "received": 2, "last_exchange_ms": 1000, "error": null}}
        }], "peers": [
            {"endpoint": "ws://10.0.2.2:1", "installation": "pc", "lanes": [$lane]},
            {"endpoint": null, "installation": "inbound", "lanes": []}
        ]}
    """

    @Test fun aReadyLaneIsAReachablePc() {
        val snapshot = parseSnapshot(status("""{"state": "Ready", "last_error": null}"""))
        assertEquals(listOf(PeerStatus("pc", "ws://10.0.2.2:1", true, null)), snapshot.peers)
        val folder = snapshot.folders.single()
        assertEquals(FolderStatus("f1", "Documents", null, 2, true, 1000, 0, "r1"), folder)
        assertFalse(snapshot.unreachable)
    }

    @Test fun aRevokedLaneErrorReachesEveryFolder() {
        val lane = """{"state": "Unauthorized", "last_error": "PEER_REVOKED: PEER_REVOKED: this installation is not paired"}"""
        val snapshot = parseSnapshot(status(lane))
        assertTrue(snapshot.revoked)
        assertFalse(snapshot.unreachable)
        assertEquals("My PC no longer recognizes this phone. Pair again.", folderLine(snapshot.folders.single(), 2000).text)
    }

    @Test fun aDownLaneIsAnUnreachablePcAndABurstErrorShowsOnItsFolder() {
        val lane = """{"state": "Backoff", "last_error": "WS_CONNECT_FAILED: refused", "error": "x"}"""
        val snapshot = parseSnapshot(status(lane), mapOf("f1" to BurstError("SYNC_NOW_TIMEOUT: late", 1000)))
        assertTrue(snapshot.unreachable)
        assertEquals("SYNC_NOW_TIMEOUT: late", snapshot.folders.single().error)
    }

    @Test fun aBurstErrorGoesWhenTheFolderReportsANewerExchange() {
        val lane = """{"state": "Ready", "last_error": null}"""
        val older = mapOf("f1" to BurstError("SYNC_NOW_TIMEOUT: late", 999))
        assertEquals(null, parseSnapshot(status(lane), older).folders.single().error)
        val same = mapOf("f1" to BurstError("SYNC_NOW_TIMEOUT: late", 1000))
        assertEquals("SYNC_NOW_TIMEOUT: late", parseSnapshot(status(lane), same).folders.single().error)
    }

    @Test fun exchangesAreReadByFolderId() {
        val lane = """{"state": "Ready", "last_error": null}"""
        assertEquals(mapOf("f1" to 1000L), parseExchanges(status(lane)))
    }

    @Test fun theWaitingAnswersAreRead() {
        assertEquals(0, parseWaiting("[]"))
        assertEquals(2, parseWaiting("""[{"entity": "a", "path": "/a"}, {"entity": "b", "path": "/b"}]"""))
        assertEquals(Confirmed(3, 1), parseConfirmed("""{"waiting": 3, "unsupported": 1}"""))
    }

    @Test fun waitingFilesAreShownAndAFailedReadIsNeverAClear() {
        val lane = """{"state": "Ready", "last_error": null}"""
        val polled = parseSnapshot(status(lane))
        val first = polled.withWaiting(mapOf("f1" to 2), emptyMap(), null, mapOf("f1" to Confirmed(2, 5)))
        assertEquals(2, first.folders.single().waiting)
        // Never more files unsupported than there are waiting.
        assertEquals(2, first.folders.single().unsupported)
        // The next read failed: the folder keeps what it was said to have, not zero.
        val failed = polled.withWaiting(emptyMap(), emptyMap(), first, emptyMap())
        assertEquals(2, failed.folders.single().waiting)
        assertEquals(0, failed.folders.single().unsupported)
        // A read that worked and found none clears, and the old confirmation says nothing then.
        val cleared = polled.withWaiting(mapOf("f1" to 0), emptyMap(), failed, mapOf("f1" to Confirmed(2, 2)))
        assertEquals(0, cleared.folders.single().waiting)
        assertEquals(0, cleared.folders.single().unsupported)
        // Before anything is known, nothing is claimed either way.
        assertEquals(0, polled.withWaiting(emptyMap(), emptyMap(), null, emptyMap()).folders.single().waiting)
    }

    @Test fun aFailedReadIsShownAsAnErrorNotAsSynced() {
        val lane = """{"state": "Ready", "last_error": null}"""
        val polled = parseSnapshot(status(lane)).let { it.copy(folders = it.folders.map { f -> f.copy(conflicts = 0) }) }
        val shown = polled.withWaiting(emptyMap(), mapOf("f1" to "PHONE_DB_UNAVAILABLE: /x/phone.sqlite"), null, emptyMap())
        val line = folderLine(shown.folders.single(), 2000)
        assertEquals(Line("Could not check what is waiting to send.", Tone.Bad), line)
        // Another error on the folder is not hidden by it.
        val other = parseSnapshot(status(lane), mapOf("f1" to BurstError("SYNC_NOW_TIMEOUT: late", 1000)))
        assertEquals("SYNC_NOW_TIMEOUT: late", other.withWaiting(emptyMap(), mapOf("f1" to "X_ERROR: y"), null, emptyMap()).folders.single().error)
    }

    @Test fun aTableStillCatchingUpIsNotSynced() {
        val lane = """{"state": "Ready", "last_error": null}"""
        val polled = parseSnapshot(status(lane)).let { it.copy(folders = it.folders.map { f -> f.copy(conflicts = 0) }) }
        val shown = polled.withWaiting(mapOf("f1" to 0), emptyMap(), null, mapOf("f1" to Confirmed(0, 0, true)))
        assertEquals(Line("Checking what is waiting to send", Tone.Waiting), folderLine(shown.folders.single(), 2000))
        assertEquals(Confirmed(0, 0, true), parseConfirmed("""{"waiting": 0, "unsupported": 0, "scanning": true}"""))
        assertEquals(Confirmed(1, 0, false), parseConfirmed("""{"waiting": 1, "unsupported": 0}"""))
    }
}
