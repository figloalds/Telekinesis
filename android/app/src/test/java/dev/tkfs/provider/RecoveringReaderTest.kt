package dev.tkfs.provider

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.IOException

class RecoveringReaderTest {
    /** Tokens count up from 100; `failures` are thrown by the reads to come, in order. */
    private class FakeDocument(vararg failures: String?) : OpenDocument {
        val calls = mutableListOf<String>()
        val readFailures = failures.toMutableList()
        val openFailures = mutableListOf<String>()
        val closeFailures = mutableListOf<String>()
        private var next = 100L

        override fun open(): Long {
            calls += "open"
            openFailures.removeFirstOrNull()?.let { throw IOException(it) }
            return next++
        }

        override fun read(token: Long, offset: Long, out: ByteArray, size: Int): Int {
            calls += "read $token"
            if (readFailures.isNotEmpty()) {
                val failure = readFailures.removeAt(0)
                throw IOException(failure)
            }
            return size
        }

        override fun close(token: Long) {
            calls += "close $token"
            closeFailures.removeFirstOrNull()?.let { throw IOException(it) }
        }
    }

    private val buffer = ByteArray(8)

    @Test fun aReadUsesTheTokenItOpened() {
        val document = FakeDocument()
        assertEquals(8, RecoveringReader(document).read(0, buffer, 8))
        assertEquals(listOf("open", "read 100"), document.calls)
    }

    @Test fun aStaleTokenIsClosedThenReplacedAndTheReadRetried() {
        val document = FakeDocument("INVALID_HANDLE")
        assertEquals(8, RecoveringReader(document).read(0, buffer, 8))
        assertEquals(listOf("open", "read 100", "close 100", "open", "read 101"), document.calls)
    }

    @Test fun everyStoppingOrNotReadyCodeReopens() {
        val codes = listOf(
            "INVALID_HANDLE",
            "STOPPING",
            "ORCHESTRATOR_STOPPING",
            "WORKER_NOT_READY: F",
            "ORCHESTRATOR_NOT_READY",
        )
        for (code in codes) {
            val document = FakeDocument(code)
            assertEquals(code, 8, RecoveringReader(document).read(0, buffer, 8))
            assertEquals(code, "read 101", document.calls.last())
        }
    }

    @Test fun anyOtherFailureFailsTheReadAndKeepsTheHandle() {
        val failures = listOf(
            "OBJECT_MISSING: 1 object(s) not supplied",
            "CONTENT_DEADLINE: too slow",
            "NATIVE_PANIC: boom",
            "UNKNOWN_ERROR: INVALID_HANDLE was only mentioned",
            "FOLDER_NOT_FOUND: F",
            null,
        )
        for (failure in failures) {
            val document = FakeDocument(failure)
            val reader = RecoveringReader(document)
            val thrown = assertThrows(IOException::class.java) { reader.read(0, buffer, 8) }
            assertEquals(failure, thrown.message)
            assertEquals(listOf("open", "read 100"), document.calls)
        }
    }

    @Test fun onlyOneReopenIsTriedPerRead() {
        val document = FakeDocument("INVALID_HANDLE", "INVALID_HANDLE")
        val reader = RecoveringReader(document)
        assertThrows(IOException::class.java) { reader.read(0, buffer, 8) }
        assertEquals(listOf("open", "read 100", "close 100", "open", "read 101"), document.calls)
    }

    /** On a restarted engine the old token is unknown, which is no reason to give up. */
    @Test fun aFailedCloseOfTheOldTokenDoesNotStopTheReopen() {
        val document = FakeDocument("INVALID_HANDLE")
        document.closeFailures += "INVALID_HANDLE"
        assertEquals(8, RecoveringReader(document).read(0, buffer, 8))
        assertEquals("read 101", document.calls.last())
    }

    @Test fun aFailedReopenFailsThisReadAndTheNextOneTriesAgain() {
        val document = FakeDocument("WORKER_NOT_READY", "INVALID_HANDLE")
        val reader = RecoveringReader(document)
        document.calls.clear()
        document.openFailures += "WORKER_NOT_READY: F"
        val thrown = assertThrows(IOException::class.java) { reader.read(0, buffer, 8) }
        assertEquals("WORKER_NOT_READY: F", thrown.message)

        assertEquals(8, reader.read(0, buffer, 8))
        assertEquals(
            listOf("read 100", "close 100", "open", "read 100", "close 100", "open", "read 101"),
            document.calls,
        )
    }

    @Test fun closingClosesTheCurrentToken() {
        val document = FakeDocument("INVALID_HANDLE")
        val reader = RecoveringReader(document)
        reader.read(0, buffer, 8)
        reader.close()
        assertEquals("close 101", document.calls.last())
    }
}
