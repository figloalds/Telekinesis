package dev.tkfs.ui

import dev.tkfs.sync.Freed
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Test

class SpaceTextTest {
    @Test fun theBatteryExemptionIsAskedForOnce() {
        assertEquals(true, asksForBattery(alreadyAsked = false, allowed = false))
        assertEquals(false, asksForBattery(alreadyAsked = true, allowed = false))
        assertEquals(false, asksForBattery(alreadyAsked = false, allowed = true))
    }

    @Test fun onlyAPlainAddressSaysItIsNotEncrypted() {
        val note = "This connection isn't encrypted. Use it only on your own network or a VPN."
        assertEquals(note, unencryptedNote("ws://192.168.1.5:43180"))
        assertEquals(note, unencryptedNote("WS://pc.local:43180"))
        assertNull(unencryptedNote("wss://pc.example.com"))
        assertNull(unencryptedNote("WSS://pc.example.com"))
    }

    @Test fun theStripSaysWhatOnlyAPcCanChange() {
        assertEquals("Low on space. Can't free anything until My PC is reachable.", lowSpaceStrip("PC_UNREACHABLE"))
        assertEquals("Low on space. Can't free anything until My PC is updated.", lowSpaceStrip("PC_TOO_OLD"))
        assertNull(lowSpaceStrip(null))
        // Of one file: it says nothing of the rest.
        assertNull(lowSpaceStrip("NOT_ON_PC"))
    }

    @Test fun theStripSaysWhenWhatIsKeptDoesNotFit() {
        val short = "Low on space. Not everything you marked to keep fits on this phone."
        assertEquals(short, lowSpaceStrip(null, keptDoesNotFit = true))
        assertNull(lowSpaceStrip(null, keptDoesNotFit = false))
        // What only a PC can change comes first.
        assertEquals(
            "Low on space. Can't free anything until My PC is reachable.",
            lowSpaceStrip("PC_UNREACHABLE", keptDoesNotFit = true),
        )
    }

    /** Below the floor after freeing, whatever else is so, a strip says it. */
    @Test fun theStripSaysSoWheneverSpaceStaysBelowTheFloor() {
        val low = "Low on space. Recent files stay unless you free them in Settings."
        assertEquals(low, lowSpaceStrip(null, belowFloor = true))
        assertNull(lowSpaceStrip(null, belowFloor = false))
        // What it can say of why comes first.
        assertEquals(
            "Low on space. Can't free anything until My PC is reachable.",
            lowSpaceStrip("PC_UNREACHABLE", belowFloor = true),
        )
        assertEquals(
            "Low on space. Not everything you marked to keep fits on this phone.",
            lowSpaceStrip(null, keptDoesNotFit = true, belowFloor = true),
        )
    }

    @Test fun removeDownloadHasASentenceForEachWayItCanBeRefused() {
        assertEquals("Not on My PC yet, so it can't be removed.", removeSentence("a.bin", Freed(0, "NOT_ON_PC")))
        val away = removeSentence("a.bin", Freed(0, "PC_UNREACHABLE"))
        val old = removeSentence("a.bin", Freed(0, "PC_TOO_OLD"))
        assertEquals("My PC isn't reachable, so the download of a.bin can't be removed yet.", away)
        assertEquals("My PC needs an update before the download of a.bin can be removed.", old)
        assertNotEquals(away, old)
    }

    @Test fun aRemovedDownloadNeedsNoSentenceAndOneThatStayedSaysWhy() {
        assertNull(removeSentence("a.bin", Freed(10, null)))
        assertEquals("a.bin is in use or has a conflict to settle, so it stays.", removeSentence("a.bin", Freed(0, null)))
    }

    @Test fun freeingNowSaysWhatItDidAndWhyNoMore() {
        assertEquals("Freed 3.4 MB.", freedSentence(Freed(3_400_000, null), stillLow = false))
        assertEquals("Enough is free already.", freedSentence(Freed(0, null), stillLow = false))
        assertEquals(
            "Freed 3.4 MB. Can't free more until My PC is reachable.",
            freedSentence(Freed(3_400_000, "PC_UNREACHABLE"), stillLow = true),
        )
        assertEquals("Can't free more until My PC is updated.", freedSentence(Freed(0, "PC_TOO_OLD"), stillLow = true))
        assertEquals(
            "Nothing else can be freed: the rest is kept, in use, or not on My PC yet.",
            freedSentence(Freed(0, null), stillLow = true),
        )
    }

    @Test fun theSettingsLineIsTheMockups() {
        assertEquals("13 GB. Oldest downloads go first.", floorLine(12_800_000_000))
    }

    @Test fun whatKeepingOffersDependsOnWhoMarkedIt() {
        assertEquals("Keep on this phone", keepAction(marked = false, kept = false))
        assertEquals("Stop keeping on this phone", keepAction(marked = true, kept = true))
        assertEquals("Kept with its folder", keepAction(marked = false, kept = true))
    }
}
