package dev.tkfs.share

import org.junit.Assert.assertEquals
import org.junit.Test

class ShareTextTest {
    @Test fun theLeadNamesWhatIsSharedAndWhoSharesIt() {
        assertEquals("2 photos from Photos", leadLine(listOf("image/jpeg", "image/png"), "Photos"))
        assertEquals("1 photo", leadLine(listOf("image/jpeg"), null))
        assertEquals("1 video from Camera", leadLine(listOf("video/mp4"), "Camera"))
        assertEquals("3 files from Files", leadLine(listOf("image/jpeg", "application/pdf", "text/plain"), "Files"))
        assertEquals("1 file", leadLine(listOf("*/*"), null))
    }

    @Test fun theButtonCountsFiles() {
        assertEquals("Save 2 files", saveLabel(2))
        assertEquals("Save 1 file", saveLabel(1))
    }

    @Test fun theFolderFieldSaysTopLevelOrTheSubfolder() {
        assertEquals("Transfer (top level)", insideLine("Transfer", "/"))
        assertEquals("Transfer / Trips / 2026", insideLine("Transfer", "/Trips/2026"))
    }

    @Test fun aSharedNameBecomesOneEntryOfADirectory() {
        assertEquals("a b.jpg", entryName("a b.jpg"))
        assertEquals("a_b.jpg", entryName("a/b.jpg"))
        assertEquals("a_b.jpg", entryName("a\\b.jpg"))
        assertEquals("shared", entryName(".."))
        assertEquals("shared", entryName(""))
    }

    @Test fun theResultSaysWhatWasSavedAndWhatWasNot() {
        assertEquals("Saved 2 files" to "to Transfer. They go to My PC when it's reachable.",
            resultLines(saved = 2, failed = emptyList(), where = "Transfer"))
        val (title, text) = resultLines(saved = 1, failed = listOf("big.mov" to "The sender went away"), where = "Transfer")
        assertEquals("Saved 1 of 2 files", title)
        assertEquals("to Transfer. big.mov could not be saved: The sender went away", text)
        assertEquals("Nothing was saved" to "a.txt could not be saved: no room",
            resultLines(saved = 0, failed = listOf("a.txt" to "no room"), where = "Transfer"))
    }
}
