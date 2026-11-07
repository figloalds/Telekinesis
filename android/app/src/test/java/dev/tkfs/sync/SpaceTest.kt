package dev.tkfs.sync

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SpaceTest {
    private val gib = 1024L * 1024 * 1024
    private val MIB = 1024L * 1024

    @Test fun theFloorIsTwoGibOrATenthOfTheVolumeIfThatIsMore() {
        assertEquals(2 * gib, floorBytes(6 * gib))
        assertEquals(2 * gib, floorBytes(20 * gib))
        assertEquals(128 * gib / 10, floorBytes(128 * gib))
    }

    @Test fun whatIsNeededIsHowFarBelowTheFloorItIs() {
        assertEquals(gib, spaceNeeded(gib, 6 * gib))
        assertEquals(1, spaceNeeded(2 * gib - 1, 6 * gib))
        assertEquals(0, spaceNeeded(2 * gib, 6 * gib))
        assertEquals(0, spaceNeeded(5 * gib, 6 * gib))
    }

    /** A download whose steps are written down, with free space going down as it goes. */
    private class Download(var free: Int, val canFree: Int = 0) {
        val steps = mutableListOf<String>()
        var stopped = false
        var left = 3

        fun run() = downloadKept(
            hydrate = { cancel ->
                steps += "hydrate"
                var brought = 0L
                while (left > 0 && !cancel()) {
                    left--
                    free--
                    brought++
                    steps += "window"
                }
                brought
            },
            missing = { left > 0 },
            belowFloor = { free < 0 },
            freeUp = {
                steps += "free"
                free += canFree
            },
            stop = { stopped },
        )
    }

    /** It looks between windows, and does not go on until the bytes it used are back. */
    @Test fun aDownloadStopsBelowTheFloorFreesAndGoesOn() {
        val download = Download(free = 0, canFree = 10)
        assertEquals(Kept.Done, download.run())
        assertEquals(listOf("hydrate", "window", "free", "hydrate", "window", "window"), download.steps)
    }

    /** With nothing to free it ends, and says so. A disk at 0 could not even free. */
    @Test fun aDownloadThatDoesNotFitEndsThereAndSaysSo() {
        val download = Download(free = 0, canFree = 0)
        assertEquals(Kept.DoesNotFit, download.run())
        assertEquals(listOf("hydrate", "window", "free"), download.steps)
        assertEquals("one window past the floor and no more", -1, download.free)
    }

    @Test fun aDownloadDoesNotStartBelowTheFloorIfFreeingDoesNotHelp() {
        val download = Download(free = -1, canFree = 0)
        assertEquals(Kept.DoesNotFit, download.run())
        assertEquals(listOf("free"), download.steps)
    }

    @Test fun aDownloadStartsBelowTheFloorOnceFreeingHelps() {
        val download = Download(free = -1, canFree = 10)
        assertEquals(Kept.Done, download.run())
        assertEquals(listOf("free", "hydrate", "window", "window", "window"), download.steps)
    }

    @Test fun aDownloadWithNothingMissingDoesNothing() {
        val download = Download(free = 5)
        download.left = 0
        assertEquals(Kept.Done, download.run())
        assertEquals(emptyList<String>(), download.steps)
    }

    /** A stop, or a change to send first: what came stays, and the next round goes on from there. */
    @Test fun aStopEndsADownloadBetweenWindowsWithNothingFreed() {
        val download = Download(free = 5)
        download.stopped = true
        assertEquals(Kept.Stopped, download.run())
        assertEquals(emptyList<String>(), download.steps)
    }

    /** A PC that brings nothing is not asked again until the next job. */
    @Test fun aDownloadThatBringsNothingEndsThere() {
        var asked = 0
        val outcome = downloadKept(
            hydrate = { asked++; 0L },
            missing = { true },
            belowFloor = { false },
            freeUp = {},
            stop = { false },
        )
        assertEquals(Kept.Done, outcome)
        assertEquals(1, asked)
    }

    /** What was marked while it ran is downloaded by this job too. */
    @Test fun aDownloadGoesRoundForWhatIsStillMissing() {
        val left = mutableListOf(1L, 1L)
        var asked = 0
        val outcome = downloadKept(
            hydrate = { asked++; left.removeAt(0) },
            missing = { left.isNotEmpty() },
            belowFloor = { false },
            freeUp = {},
            stop = { false },
        )
        assertEquals(Kept.Done, outcome)
        assertEquals(2, asked)
    }

    @Test fun theFirstCheckBelowTheFloorIsDue() {
        assertTrue(floorCheckDue(null, 1_000, 5))
    }

    @Test fun belowTheFloorTheCheckRunsOnceAnHour() {
        val last = FloorChecked(atMs = 1_000_000, freeBytes = 500 * MIB)
        assertFalse(floorCheckDue(last, last.atMs + 1, last.freeBytes))
        assertFalse(floorCheckDue(last, last.atMs + FLOOR_CHECK_EVERY_MS - 1, last.freeBytes))
        assertTrue(floorCheckDue(last, last.atMs + FLOOR_CHECK_EVERY_MS, last.freeBytes))
    }

    @Test fun freeSpaceMovingByMoreThan100MiBRunsItSooner() {
        val last = FloorChecked(atMs = 1_000_000, freeBytes = 500 * MIB)
        val soon = last.atMs + 60_000
        assertFalse(floorCheckDue(last, soon, last.freeBytes + 100 * MIB))
        assertFalse(floorCheckDue(last, soon, last.freeBytes - 100 * MIB))
        assertTrue(floorCheckDue(last, soon, last.freeBytes + 100 * MIB + 1))
        assertTrue(floorCheckDue(last, soon, last.freeBytes - 100 * MIB - 1))
    }

    /** It says nothing of how long it has been, and a clock set far ahead must not stop checks for good. */
    @Test fun aClockThatWentBackCountsAsTimeUp() {
        val last = FloorChecked(atMs = 1_000_000, freeBytes = 500 * MIB)
        assertTrue(floorCheckDue(last, last.atMs - 1, last.freeBytes))
    }

    @Test fun anAnswerSaysWhatLeftAndWhyNoMoreCould() {
        assertEquals(Freed(7, null), parseFreed("""{"bytes":7,"blocked":null}"""))
        assertEquals(Freed(0, "PC_UNREACHABLE"), parseFreed("""{"bytes":0,"blocked":"PC_UNREACHABLE"}"""))
    }
}
