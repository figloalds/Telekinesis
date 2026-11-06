package dev.tkfs.provider

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.IOException

class DocumentWriterTest {
    private class FakeDocument(private val calls: MutableList<String>) : WritableDocument {
        val failing = mutableSetOf<String>()

        private fun call(name: String) {
            calls += name
            if (name in failing) throw IOException("INVALID_HANDLE: $name")
        }

        override fun open(): Long {
            call("open")
            return 100
        }

        override fun read(token: Long, offset: Long, out: ByteArray, size: Int): Int {
            call("read")
            return size
        }

        override fun length(token: Long): Long {
            call("length")
            return 42
        }

        override fun close(token: Long) = call("close")

        override fun write(token: Long, offset: Long, data: ByteArray, size: Int, append: Boolean): Int {
            call("write $token@$offset $size${if (append) " append" else ""}")
            return size
        }

        override fun truncate(token: Long) = call("truncate")

        override fun sync(token: Long) = call("sync")
    }

    private val calls = mutableListOf<String>()
    private val document = FakeDocument(calls)
    private val data = ByteArray(8)

    private fun writer(append: Boolean = false, truncate: Boolean = false, log: (String) -> Unit = {}) =
        DocumentWriter(document, append, truncate, log) { calls += "released" }

    @Test fun releaseSyncsThenClosesThenSaysItWasReleased() {
        writer().release()
        assertEquals(listOf("open", "sync", "close", "released"), calls)
    }

    /** The draft stays on disk, for the sealer or the next start: a burst can still follow it. */
    @Test fun aFailedSyncStillClosesAndSaysSoAndLogsItsCode() {
        val logged = mutableListOf<String>()
        document.failing += "sync"
        writer(log = { logged += it }).release()
        assertEquals(listOf("open", "sync", "close", "released"), calls)
        assertEquals(listOf("sync: INVALID_HANDLE: sync"), logged)
    }

    @Test fun aFailedCloseStillSaysSoAndLogsItsCode() {
        val logged = mutableListOf<String>()
        document.failing += "close"
        writer(log = { logged += it }).release()
        assertEquals(listOf("open", "sync", "close", "released"), calls)
        assertEquals(listOf("close: INVALID_HANDLE: close"), logged)
    }

    @Test fun aWriteGoesToItsTokenAtItsOffset() {
        assertEquals(8, writer().write(16, data, 8))
        assertEquals("write 100@16 8", calls.last())
    }

    @Test fun anAppendingHandleSaysSoOnEveryWrite() {
        writer(append = true).write(0, data, 8)
        assertEquals("write 100@0 8 append", calls.last())
    }

    /** Reopening would lose or misplace what was written, so a stale token is an error for good. */
    @Test fun aStaleTokenFailsWritesReadsAndLengthAndNeverReopens() {
        val writer = writer()
        document.failing += setOf("write 100@0 8", "read", "length", "sync")
        for (failing in listOf<() -> Unit>(
            { writer.write(0, data, 8) },
            { writer.read(0, data, 8) },
            { writer.length() },
            { writer.sync() },
        )) {
            assertThrows(IOException::class.java) { failing() }
        }
        writer.release()
        assertEquals(1, calls.count { it == "open" })
    }

    @Test fun aTruncatingOpenChangesNothingUntilItStartsAndIsEmptyAllAlong() {
        val writer = writer(truncate = true)
        assertEquals(0, writer.length())
        assertEquals(listOf("open"), calls)
        writer.start()
        assertEquals(listOf("open", "truncate"), calls)
        assertEquals(42, writer.length())
    }

    @Test fun anOpenThatKeepsTheContentsDoesNotTruncate() {
        val writer = writer()
        writer.start()
        assertEquals(42, writer.length())
        assertEquals(listOf("open", "length"), calls)
    }

    @Test fun aFailedTruncateFailsTheStart() {
        val writer = writer(truncate = true)
        document.failing += "truncate"
        assertThrows(IOException::class.java) { writer.start() }
    }

    /** No descriptor could be made, so nothing was written and nobody is told of a change. */
    @Test fun abandoningOnlyClosesEvenWithATruncatePending() {
        writer(truncate = true).abandon()
        assertEquals(listOf("open", "close"), calls)
    }

    @Test fun anOpenThatFailsMakesNoWriterAndNothingIsReleased() {
        document.failing += "open"
        assertThrows(IOException::class.java) { writer() }
        assertEquals(listOf("open"), calls)
    }
}
