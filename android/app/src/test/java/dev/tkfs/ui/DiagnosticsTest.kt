package dev.tkfs.ui

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class DiagnosticsTest {
    private val code = "ab".repeat(32)

    private val snapshot = Snapshot(
        folders = listOf(
            FolderStatus("f1", "Documents", error = "WS_CONNECT_FAILED: refused", conflicts = 1, lastSyncMs = 1_000, waiting = 3),
            FolderStatus("f2", "Photos"),
        ),
        peers = listOf(PeerStatus("pc", "ws://10.0.0.2:1", false, "WS_CONNECT_FAILED: refused")),
    )

    @Test fun reportsPeersFoldersAndErrors() {
        val text = diagnosticsText(snapshot, "1 GB on this phone", now = 1_000 + 5 * 60_000)
        assertTrue(text.contains("Storage: 1 GB on this phone"))
        assertTrue(text.contains("- ws://10.0.0.2:1: not connected (WS_CONNECT_FAILED: refused)"))
        assertTrue(text.contains("- Documents: last synced 5 min ago, 3 waiting to send, 1 conflicts"))
        assertTrue(text.contains("- Photos: never synced, 0 waiting to send, 0 conflicts"))
        assertTrue(text.contains("- Documents: WS_CONNECT_FAILED: refused"))
    }

    @Test fun saysNoneWhenNothingFailed() {
        val text = diagnosticsText(Snapshot(emptyList(), emptyList()), "", now = 0)
        assertTrue(text.endsWith("Recent errors:\n- none"))
    }

    @Test fun aKeyShapedStringNeverLeaves() {
        val leaky = snapshot.copy(peers = listOf(PeerStatus("pc", "ws://h:1", false, "bad code $code")))
        val text = diagnosticsText(leaky, "", now = 0)
        assertFalse(text.contains(code))
        assertTrue(text.contains("[hidden]"))
    }

    @Test fun beforeTheFirstStatusItSaysSo() {
        assertTrue(diagnosticsText(null, "", now = 0).contains("not read its status"))
    }
}
