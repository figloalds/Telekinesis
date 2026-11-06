package dev.tkfs.sync

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
        Thread.sleep(200)
        assertEquals("the full one waits", listOf(Mode.MetadataOnly), started.toList())
        release.countDown()
        listOf(first, second, third).forEach { it.join(5_000) }
        assertEquals("two waiting full bursts run once", listOf(Mode.MetadataOnly, Mode.Full), started.toList())
        assertFalse(bursts.isRunning.value)
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
}
