package dev.tkfs.provider

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.FileNotFoundException

class OpenModeTest {
    @Test fun rOnlyReads() {
        assertEquals(OpenMode(read = true, write = false, truncate = false, append = false), parseOpenMode("r"))
    }

    @Test fun wTruncates() {
        val truncating = OpenMode(read = false, write = true, truncate = true, append = false)
        assertEquals(truncating, parseOpenMode("w"))
        assertEquals(truncating, parseOpenMode("wt"))
    }

    @Test fun rwKeepsContents() {
        assertEquals(OpenMode(read = true, write = true, truncate = false, append = false), parseOpenMode("rw"))
    }

    @Test fun rwtReadsAndTruncates() {
        assertEquals(OpenMode(read = true, write = true, truncate = true, append = false), parseOpenMode("rwt"))
    }

    @Test fun waAppends() {
        assertEquals(OpenMode(read = false, write = true, truncate = false, append = true), parseOpenMode("wa"))
    }

    @Test fun anUnknownModeIsRefused() {
        for (mode in listOf("", "a", "t", "ra", "rwa", "tw", "W", "wr", "rw ", "rwta")) {
            val thrown = assertThrows(mode, FileNotFoundException::class.java) { parseOpenMode(mode) }
            assertEquals("INVALID_PARAMETER", thrown.message?.substringBefore(':'))
        }
    }

    @Test fun aFreeNameIsKept() {
        assertEquals("a.txt", numberedName(setOf("b.txt"), "a.txt"))
        assertEquals("a.txt", numberedName(emptySet(), "a.txt"))
    }

    @Test fun aClashGetsTheFirstFreeNumberBeforeTheExtension() {
        assertEquals("a (1).txt", numberedName(setOf("a.txt"), "a.txt"))
        assertEquals("a (3).txt", numberedName(setOf("a.txt", "a (1).txt", "a (2).txt"), "a.txt"))
        assertEquals("a (1).txt", numberedName(setOf("a.txt", "a (2).txt"), "a.txt"))
    }

    @Test fun aNameWithNoExtensionGetsANumber() {
        assertEquals("notes (1)", numberedName(setOf("notes"), "notes"))
    }

    @Test fun onlyTheLastDotStartsTheExtension() {
        assertEquals("a.tar (1).gz", numberedName(setOf("a.tar.gz"), "a.tar.gz"))
    }

    /** A dot that starts a name, or ends it, starts no extension. */
    @Test fun aHiddenNameOrATrailingDotHasNoExtension() {
        assertEquals(".profile (1)", numberedName(setOf(".profile"), ".profile"))
        assertEquals("a. (1)", numberedName(setOf("a."), "a."))
    }

    /** The core treats these as one name, so they clash there. */
    @Test fun aClashIgnoresCaseAndUnicodeForm() {
        assertEquals("A (1).TXT", numberedName(setOf("a.txt"), "A.TXT"))
        assertEquals("é (1).txt", numberedName(setOf("é.txt"), "é.txt"))
        assertEquals("a (2).txt", numberedName(setOf("a.txt", "A (1).TXT"), "a.txt"))
    }
}
