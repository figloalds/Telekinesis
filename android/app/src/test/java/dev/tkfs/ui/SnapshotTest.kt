package dev.tkfs.ui

import dev.tkfs.sync.BurstError
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** The shapes `status` really sends: `folder_entry` and `Network::peers`, as seen in ui_folders. */
class SnapshotTest {
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
}
