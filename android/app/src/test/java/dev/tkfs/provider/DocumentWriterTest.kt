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

        override fun abandon(token: Long) = call("abandon")
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

    /** A kill now finds nothing changed, however long the client holds its descriptor. */
    @Test fun aTruncatingOpenChangesNothingBeforeItsFirstWriteAndIsEmptyAllAlong() {
        val writer = writer(truncate = true)
        assertEquals(0, writer.length())
        assertEquals(0, writer.read(0, data, 8))
        assertEquals(listOf("open"), calls)
    }

    @Test fun theFirstWriteTruncatesAndNoLaterOneDoes() {
        val writer = writer(truncate = true)
        writer.write(0, data, 8)
        writer.write(8, data, 8)
        assertEquals(listOf("open", "truncate", "write 100@0 8", "write 100@8 8"), calls)
        assertEquals(42, writer.length())
        assertEquals(8, writer.read(0, data, 8))
    }

    /** An fsync says the file is as it looks, and it looks empty. */
    @Test fun anFsyncBeforeAnyWriteTruncatesFirst() {
        writer(truncate = true).sync()
        assertEquals(listOf("open", "truncate", "sync"), calls)
    }

    /** `w` and a close with nothing written leaves an empty file. */
    @Test fun aReleaseWithNothingWrittenTruncatesThenSeals() {
        writer(truncate = true).release()
        assertEquals(listOf("open", "truncate", "sync", "close", "released"), calls)
    }

    @Test fun anOpenThatKeepsTheContentsDoesNotTruncate() {
        val writer = writer()
        writer.write(0, data, 8)
        writer.sync()
        writer.release()
        assertEquals(listOf("open", "write 100@0 8", "sync", "sync", "close", "released"), calls)
        assertEquals(0, calls.count { it == "truncate" })
    }

    /** The writer gets the failure, and nothing was written over a file that was not cut. */
    @Test fun aFailedTruncateFailsTheWriteAndWritesNothing() {
        val writer = writer(truncate = true)
        document.failing += "truncate"
        assertThrows(IOException::class.java) { writer.write(0, data, 8) }
        assertEquals(listOf("open", "truncate"), calls)
        assertEquals(0, writer.length())
    }

    @Test fun aWriteAfterAFailedTruncateTriesItAgain() {
        val writer = writer(truncate = true)
        document.failing += "truncate"
        assertThrows(IOException::class.java) { writer.write(0, data, 8) }
        document.failing.clear()
        writer.write(0, data, 8)
        assertEquals(listOf("open", "truncate", "truncate", "write 100@0 8"), calls)
    }

    /** It was told the write failed, so its close must not empty the file after all. */
    @Test fun aReleaseAfterAFailedTruncateOnlyClosesAndIsNotAChange() {
        val writer = writer(truncate = true)
        document.failing += "truncate"
        assertThrows(IOException::class.java) { writer.write(0, data, 8) }
        document.failing.clear()
        calls.clear()
        writer.release()
        assertEquals(listOf("close"), calls)
    }

    @Test fun aTruncateThatFailsAtTheReleaseOnlyClosesAndLogsItsCode() {
        val logged = mutableListOf<String>()
        document.failing += "truncate"
        writer(truncate = true, log = { logged += it }).release()
        assertEquals(listOf("open", "truncate", "close"), calls)
        assertEquals(listOf("truncate: INVALID_HANDLE: truncate"), logged)
    }

    /** A full disk at the first write: the file stays as it was, not empty and not half of anything. */
    @Test fun aFailedFirstWriteTakesTheCutBackAtOnce() {
        val writer = writer(truncate = true)
        document.failing += "write 100@0 8"
        assertThrows(IOException::class.java) { writer.write(0, data, 8) }
        assertEquals(listOf("open", "truncate", "write 100@0 8", "abandon"), calls)
    }

    @Test fun aReleaseAfterAFailedFirstWriteOnlyClosesAndIsNotAChange() {
        val writer = writer(truncate = true)
        document.failing += "write 100@0 8"
        assertThrows(IOException::class.java) { writer.write(0, data, 8) }
        calls.clear()
        writer.release()
        assertEquals(listOf("close"), calls)
    }

    /** A copy that ran out of room partway leaves no part of itself to replace what was there. */
    @Test fun aFailedLaterWriteTakesBackWhatWentBefore() {
        val writer = writer(truncate = true)
        writer.write(0, data, 8)
        document.failing += "write 100@8 8"
        assertThrows(IOException::class.java) { writer.write(8, data, 8) }
        assertEquals("abandon", calls.last())
        calls.clear()
        writer.release()
        assertEquals(listOf("close"), calls)
    }

    /** What it was told failed stays failed: going on would leave a hole where the bytes it lost go. */
    @Test fun afterAFailedWriteEverythingFailsUntilItCloses() {
        val writer = writer(truncate = true)
        document.failing += "write 100@0 8"
        val failure = assertThrows(IOException::class.java) { writer.write(0, data, 8) }
        document.failing.clear()
        calls.clear()
        assertEquals(failure.message, assertThrows(IOException::class.java) { writer.write(8, data, 8) }.message)
        assertEquals(failure.message, assertThrows(IOException::class.java) { writer.sync() }.message)
        assertEquals(emptyList<String>(), calls)
        assertEquals(0, writer.length())
        assertEquals(0, writer.read(0, data, 8))
    }

    /** An fsync told it those bytes were safe, so they stay; a burst still has them to carry. */
    @Test fun whatAnFsyncSealedStaysAChangeAfterALaterFailure() {
        val writer = writer(truncate = true)
        writer.write(0, data, 8)
        writer.sync()
        document.failing += "write 100@8 8"
        assertThrows(IOException::class.java) { writer.write(8, data, 8) }
        calls.clear()
        writer.release()
        assertEquals(listOf("close", "released"), calls)
    }

    @Test fun aFailedTakeBackIsLoggedAndTheWriterStillGetsTheFailure() {
        val logged = mutableListOf<String>()
        val writer = writer(truncate = true, log = { logged += it })
        document.failing += setOf("write 100@0 8", "abandon")
        val failure = assertThrows(IOException::class.java) { writer.write(0, data, 8) }
        assertEquals("INVALID_HANDLE: write 100@0 8", failure.message)
        assertEquals(listOf("abandon: INVALID_HANDLE: abandon"), logged)
    }

    /** Only a replacement is all or nothing. An edit in place keeps what it wrote before the failure. */
    @Test fun aFailedWriteOfAnOpenThatKeepsTheContentsTakesNothingBack() {
        val writer = writer()
        writer.write(0, data, 8)
        document.failing += "write 100@8 8"
        assertThrows(IOException::class.java) { writer.write(8, data, 8) }
        document.failing.clear()
        assertEquals(8, writer.write(16, data, 8))
        writer.release()
        assertEquals(0, calls.count { it == "abandon" })
        assertEquals(listOf("sync", "close", "released"), calls.takeLast(3))
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
