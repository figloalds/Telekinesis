package dev.tkfs.ui

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class FileRowTest {
    private fun line(size: Long, local: Long, waiting: Boolean = false, kept: Boolean = false) =
        rowLine("file.bin", size, local, waiting, kept)

    @Test fun aFileOnThePhoneSaysSoWithItsSize() {
        assertEquals(Line("On this phone, 3.4 MB", Tone.Ok), line(3_400_000, 3_400_000))
    }

    @Test fun aFileThePhoneNeverDownloadedSaysSo() {
        assertEquals(Line("Not downloaded, 812 MB", Tone.Quiet), line(812_000_000, 0))
    }

    @Test fun aPartlyDownloadedFileIsNotOnThisPhone() {
        assertEquals(Line("Not downloaded, 812 MB", Tone.Quiet), line(812_000_000, 400_000_000))
        assertFalse(isOnThisPhone(812_000_000, 811_999_999))
        assertTrue(isOnThisPhone(812_000_000, 812_000_000))
    }

    @Test fun waitingOutranksOnThisPhone() {
        assertEquals(Line("Waiting to send, 2.3 MB", Tone.Waiting), line(2_300_000, 2_300_000, waiting = true))
        // Whatever else is true of it.
        assertEquals(Line("Waiting to send, 2.3 MB", Tone.Waiting), line(2_300_000, 2_300_000, waiting = true, kept = true))
    }

    @Test fun aKeptFileSaysSoOnceItIsAllHere() {
        assertEquals(Line("Kept on this phone, 3.4 MB", Tone.Ok), line(3_400_000, 3_400_000, kept = true))
        assertEquals(Line("Not downloaded, 3.4 MB", Tone.Quiet), line(3_400_000, 1, kept = true))
    }

    @Test fun anEmptyFileIsOnThePhone() {
        assertEquals(Line("On this phone, 0 B", Tone.Ok), line(0, 0))
    }

    @Test fun sizesReadAsAndroidShowsThem() {
        assertEquals("999 B", sizeText(999))
        assertEquals("1.0 kB", sizeText(1_000))
        assertEquals("3.4 MB", sizeText(3_400_000))
        assertEquals("812 MB", sizeText(812_000_000))
        assertEquals("1.5 GB", sizeText(1_500_000_000))
        assertEquals("12 GB", sizeText(12_300_000_000))
    }

    @Test fun theStripNamesWhatCouldNotOpen() {
        assertNull(openStripText(emptyList()))
        assertEquals("My PC isn't reachable, so video-obra.mp4 can't open yet.", openStripText(listOf("video-obra.mp4")))
        assertEquals("My PC isn't reachable, so a.mp4 and b.mp4 can't open yet.", openStripText(listOf("a.mp4", "b.mp4")))
        assertEquals(
            "My PC isn't reachable, so a.mp4, b.mp4 and 2 more can't open yet.",
            openStripText(listOf("a.mp4", "b.mp4", "c.mp4", "d.mp4")),
        )
    }

    @Test fun theNotificationNamesTheFile() {
        assertEquals("My PC isn't reachable, so video-obra.mp4 can't open yet.", unreachableOpen("video-obra.mp4"))
    }
}
