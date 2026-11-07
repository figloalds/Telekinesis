package dev.tkfs.sync

import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class BurstsTest {
    private val started = CopyOnWriteArrayList<Mode>()
    private val release = CountDownLatch(1)
    private val entered = CountDownLatch(1)

    private fun bursts() = Bursts { mode ->
        started += mode
        entered.countDown()
        assertTrue("the test released the burst", release.await(5, TimeUnit.SECONDS))
        BurstResult()
    }

    private fun async(bursts: Bursts, mode: Mode): Thread {
        val results = arrayOfNulls<BurstResult>(1)
        return Thread { results[0] = bursts.request(mode) }.also { it.start() }
    }

    @Test fun aTriggerThatWantsNoMoreThanTheRunningBurstIsDropped() {
        val bursts = bursts()
        val first = async(bursts, Mode.Full)
        assertTrue(entered.await(5, TimeUnit.SECONDS))
        assertTrue(bursts.isRunning.value)
        assertTrue(bursts.request(Mode.Full).dropped)
        assertTrue(bursts.request(Mode.MetadataOnly).dropped)
        release.countDown()
        first.join(5_000)
        assertEquals(listOf(Mode.Full), started.toList())
        assertFalse(bursts.isRunning.value)
    }

    @Test fun aFullBurstAfterAMetadataOnlyOneRunsAfterIt() {
        val bursts = bursts()
        val first = async(bursts, Mode.MetadataOnly)
        assertTrue(entered.await(5, TimeUnit.SECONDS))
        val second = async(bursts, Mode.Full)
        val third = async(bursts, Mode.Full)
        // Both are blocked in `request` before anything is released, so "waits" is not a race.
        listOf(second, third).forEach { blocked ->
            val end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
            while (blocked.state != Thread.State.WAITING && System.nanoTime() < end) Thread.sleep(5)
            assertEquals("a full burst waits behind the metadata-only one", Thread.State.WAITING, blocked.state)
        }
        assertEquals("the full one waits", listOf(Mode.MetadataOnly), started.toList())
        release.countDown()
        listOf(first, second, third).forEach { it.join(5_000) }
        assertEquals("two waiting full bursts run once", listOf(Mode.MetadataOnly, Mode.Full), started.toList())
        assertFalse(bursts.isRunning.value)
    }

    private fun waitsInRequest(thread: Thread) {
        val end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (thread.state != Thread.State.WAITING && System.nanoTime() < end) Thread.sleep(5)
        assertEquals(Thread.State.WAITING, thread.state)
    }

    /** It would otherwise start after `end-bursts`, on a network that may not carry it. */
    @Test fun holdingBackDropsAFullBurstThatWaits() {
        val bursts = bursts()
        val first = async(bursts, Mode.MetadataOnly)
        assertTrue(entered.await(5, TimeUnit.SECONDS))
        val waiting = async(bursts, Mode.Full)
        waitsInRequest(waiting)
        assertFalse("a metadata-only burst moves no files, so it is not ended", bursts.holdBack())
        waiting.join(5_000)
        assertFalse("it was let go at once", waiting.isAlive)
        release.countDown()
        first.join(5_000)
        assertEquals(listOf(Mode.MetadataOnly), started.toList())
    }

    @Test fun holdingBackLeavesAFullBurstTheOwnerAskedFor() {
        val bursts = bursts()
        val first = async(bursts, Mode.MetadataOnly)
        assertTrue(entered.await(5, TimeUnit.SECONDS))
        val asked = Thread { bursts.request(Mode.Full, asked = true) }.also { it.start() }
        waitsInRequest(asked)
        assertFalse(bursts.holdBack())
        release.countDown()
        listOf(first, asked).forEach { it.join(5_000) }
        assertEquals(listOf(Mode.MetadataOnly, Mode.Full), started.toList())
    }

    @Test fun holdingBackSaysToEndARunningFullBurstUnlessTheOwnerAskedForIt() {
        val bursts = bursts()
        val unasked = async(bursts, Mode.Full)
        assertTrue(entered.await(5, TimeUnit.SECONDS))
        assertTrue(bursts.holdBack())
        release.countDown()
        unasked.join(5_000)
        assertFalse("nothing runs by now", bursts.holdBack())

        val theirs = CountDownLatch(1)
        val letGo = CountDownLatch(1)
        val owners = Bursts {
            theirs.countDown()
            assertTrue(letGo.await(5, TimeUnit.SECONDS))
            BurstResult()
        }
        val asked = Thread { owners.request(Mode.Full, asked = true) }.also { it.start() }
        assertTrue(theirs.await(5, TimeUnit.SECONDS))
        assertFalse(owners.holdBack())
        letGo.countDown()
        asked.join(5_000)
    }

    /** What is asked for after the network changed is for the caller to judge, by the network as it is now. */
    @Test fun holdingBackDoesNotDropWhatComesAfterIt() {
        val bursts = bursts()
        bursts.holdBack()
        release.countDown()
        assertFalse(bursts.request(Mode.Full).dropped)
        assertEquals(listOf(Mode.Full), started.toList())
    }

    @Test fun aMalformedReplyIsABurstThatFailed() {
        assertEquals(UNREADABLE_BURST, reportOf { BurstResult.parse("not json", emptyMap()) }.failure)
        val wrongShape = reportOf { BurstResult.parse("""{"folders": [{"exchanged": 1}]}""", emptyMap()) }
        assertEquals(UNREADABLE_BURST, wrongShape.failure)
        assertEquals("BOOM: x", reportOf { throw java.io.IOException("BOOM: x") }.failure)
        assertEquals(UNREADABLE_BURST, reportOf { throw IllegalStateException("x") }.failure)
        assertNull(reportOf { BurstResult() }.failure)
    }

    /** One that nothing caught in the background is logged, and the next still runs. */
    @Test fun whatFailsInTheBackgroundIsHeardAndTheRestGoesOn() {
        val heard = CopyOnWriteArrayList<Throwable>()
        val scope = backgroundScope { heard += it }
        runBlocking {
            scope.launch { throw IllegalStateException("not caught") }.join()
            val next = scope.async { "still running" }
            assertEquals("still running", next.await())
        }
        assertEquals(listOf("not caught"), heard.map { it.message })
    }

    @Test fun skipRunsNothing() {
        assertTrue(Bursts { error("no burst") }.request(Mode.Skip).dropped)
    }

    @Test fun aBurstErrorGoesWhenTheFolderReportsANewerExchange() {
        val result = BurstResult.parse(
            """{"folders": [{"id": "a", "exchanged": 0, "remaining": 0, "error": "SYNC_NOW_TIMEOUT: x"},
                {"id": "b", "exchanged": 2, "remaining": null, "error": null}]}""",
            mapOf("a" to 1000L),
        )
        assertEquals(2L, result.exchanged)
        assertNull(result.folders[1].error)
        assertNull(result.folders[1].remaining)
        val error = result.errors.getValue("a")
        assertTrue(error.stillApplies(1000))
        assertTrue(error.stillApplies(null))
        assertFalse(error.stillApplies(1001))
    }

    @Test fun anErrorOfAFolderThatNeverExchangedGoesWithItsFirstExchange() {
        val error = BurstError("x", null)
        assertTrue(error.stillApplies(null))
        assertFalse(error.stillApplies(5))
    }

    @Test fun aBurstTheTransferJobEndedIsNotOneToAskThePcAfter() {
        val ended = BurstResult(listOf(FolderBurst("f", 0, null, BurstError("SYNC_NOW_ENDED: the job stopped", 1), 0)))
        val failed = BurstResult(listOf(FolderBurst("f", 0, null, BurstError("WS_CONNECT_FAILED: refused", 1), 0)))
        assertTrue(ended.ended)
        // A PC that is away is the case "waiting" exists for: that burst is still confirmed, and finds nothing.
        assertFalse(failed.ended)
        assertFalse(BurstResult().ended)
    }
}
