package dev.tkfs.share

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ShareLedgerTest {
    private class Memory : LedgerStore {
        var text: String? = null
        override fun read() = text
        override fun write(text: String) {
            this.text = text
        }
    }

    private val store = Memory()

    /** A ledger over the same text, as a new process would open it. */
    private fun ledger() = ShareLedger(store)

    @Test fun aShareThatFinishedLeavesOnlyItsFailures() {
        val ledger = ledger()
        ledger.begin("one", "f", listOf("a.jpg", "b.jpg"))
        ledger.working("one", "/.tkfs-part-1")
        ledger.cleared("one", "/.tkfs-part-1")
        ledger.done("one", "a.jpg")
        ledger.finish("one", failed = listOf("b.jpg"))
        assertEquals(listOf("b.jpg"), ledger().notices())
        assertTrue(ledger().orphans().isEmpty())
    }

    @Test fun aWorkingFileThatCouldNotBeRemovedBecomesAnOrphanAtTheEnd() {
        val ledger = ledger()
        ledger.begin("one", "f", listOf("a.jpg"))
        ledger.working("one", "/.tkfs-part-1")
        ledger.finish("one", failed = listOf("a.jpg"))
        assertEquals(listOf(Orphan("f", "/.tkfs-part-1")), ledger().orphans())
    }

    @Test fun aKillIsReportedByTheNextProcessWithOnlyWhatWasNotSaved() {
        val first = ledger()
        first.begin("one", "f", listOf("a.jpg", "b.jpg", "c.jpg"))
        first.done("one", "a.jpg")
        first.working("one", "/dir/.tkfs-part-2")
        // The process dies here. The next one has no live share.
        val next = ledger()
        next.recover(live = emptySet())
        assertEquals(listOf("b.jpg", "c.jpg"), next.notices())
        assertEquals(listOf(Orphan("f", "/dir/.tkfs-part-2")), next.orphans())
        // Once, not again.
        ledger().recover(live = emptySet())
        assertEquals(listOf("b.jpg", "c.jpg"), ledger().notices())
    }

    @Test fun aShareThatIsStillRunningIsNotRecovered() {
        val ledger = ledger()
        ledger.begin("one", "f", listOf("a.jpg"))
        ledger.working("one", "/.tkfs-part-1")
        ledger.recover(live = setOf("one"))
        assertTrue(ledger.notices().isEmpty())
        assertTrue(ledger.orphans().isEmpty())
    }

    @Test fun anOrphanStaysUntilItIsSwept() {
        val ledger = ledger()
        ledger.begin("one", "f", emptyList())
        ledger.working("one", "/.tkfs-part-1")
        ledger.recover(live = emptySet())
        ledger.swept(Orphan("f", "/.tkfs-part-1"))
        assertTrue(ledger().orphans().isEmpty())
    }

    @Test fun noticesAreKeptUntilDismissedAndNeverRepeatAName() {
        val ledger = ledger()
        ledger.addNotices(listOf("a.jpg", "b.jpg"))
        ledger.addNotices(listOf("b.jpg", "c.jpg"))
        assertEquals(listOf("a.jpg", "b.jpg", "c.jpg"), ledger().notices())
        ledger.dismiss()
        assertTrue(ledger().notices().isEmpty())
    }

    @Test fun anUnreadableLedgerIsEmptyNotACrash() {
        store.text = "{not json"
        assertTrue(ledger().notices().isEmpty())
        ledger().addNotices(listOf("a.jpg"))
        assertEquals(listOf("a.jpg"), ledger().notices())
    }

    @Test fun theStripNamesWhatWasNotSaved() {
        assertNull(stripText(emptyList()))
        assertEquals("Couldn't save IMG_2041.jpg. Share it again.", stripText(listOf("IMG_2041.jpg")))
        assertEquals("Couldn't save a and b. Share them again.", stripText(listOf("a", "b")))
        assertEquals("Couldn't save a, b and 2 more. Share them again.", stripText(listOf("a", "b", "c", "d")))
    }
}
