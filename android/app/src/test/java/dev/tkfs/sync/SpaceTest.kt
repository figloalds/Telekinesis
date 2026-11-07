package dev.tkfs.sync

import org.junit.Assert.assertEquals
import org.junit.Test

class SpaceTest {
    private val gib = 1024L * 1024 * 1024

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

    @Test fun anAnswerSaysWhatLeftAndWhyNoMoreCould() {
        assertEquals(Freed(7, null), parseFreed("""{"bytes":7,"blocked":null}"""))
        assertEquals(Freed(0, "PC_UNREACHABLE"), parseFreed("""{"bytes":0,"blocked":"PC_UNREACHABLE"}"""))
    }
}
