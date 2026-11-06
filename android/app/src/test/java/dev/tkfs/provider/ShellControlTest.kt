package dev.tkfs.provider

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.IOException
import java.util.Base64

class ShellControlTest {
    @Test fun aDebugBuildAnswersTheShellAndRoot() {
        assertTrue(ShellControl.admits(debugBuild = true, callingUid = 2000))
        assertTrue(ShellControl.admits(debugBuild = true, callingUid = 0))
    }

    @Test fun anotherAppIsNotAnswered() {
        assertFalse(ShellControl.admits(debugBuild = true, callingUid = 10123))
        assertFalse(ShellControl.admits(debugBuild = true, callingUid = 1000))
    }

    @Test fun aReleaseBuildAnswersNobody() {
        assertFalse(ShellControl.admits(debugBuild = false, callingUid = 2000))
        assertFalse(ShellControl.admits(debugBuild = false, callingUid = 0))
    }

    @Test fun aPayloadIsBase64OfUtf8Json() {
        val json = """{"label":"é: it's"}"""
        val encoded = Base64.getEncoder().encodeToString(json.toByteArray())
        assertEquals(json, ShellControl.payload(encoded))
    }

    @Test fun noPayloadIsAnEmptyObject() {
        assertEquals("{}", ShellControl.payload(null))
    }

    @Test fun aPayloadThatIsNotBase64IsARefusalWithACode() {
        val refused = assertThrows(IOException::class.java) { ShellControl.payload("{\"a\":1}") }
        assertTrue(refused.message.orEmpty().startsWith("INVALID_REQUEST"))
    }
}
