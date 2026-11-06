package dev.tkfs

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class PairLinkTest {
    private val code = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

    @Test fun parsesEndpointAndCode() {
        // Exactly what pairing_link::link builds for this endpoint.
        val link = "tkfs://pair?endpoint=ws%3A%2F%2F192.168.1.20%3A43180&code=$code"
        assertEquals(PairInfo("ws://192.168.1.20:43180", code), PairLink.parse(link))
        assertEquals(PairInfo("ws://192.168.1.20:43180", code), PairLink.parse("  $link\n"))
    }

    @Test fun aLinkWithNoEndpointStillGivesTheCode() {
        assertEquals(PairInfo(null, code), PairLink.parse("tkfs://pair?code=$code"))
    }

    @Test fun refusesACodeThatIsNot64Hex() {
        assertNull(PairLink.parse("tkfs://pair?code=${code.dropLast(1)}"))
        assertNull(PairLink.parse("tkfs://pair?code=${code.dropLast(1)}g"))
        assertNull(PairLink.parse("tkfs://pair?code=${code}0"))
        assertNull(PairLink.parse("tkfs://pair?endpoint=ws%3A%2F%2Fh%3A1"))
        assertNull(PairLink.parse("https://pair?code=$code"))
        assertNull(PairLink.parse(code))
    }
}
