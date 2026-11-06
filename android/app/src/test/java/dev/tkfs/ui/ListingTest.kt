package dev.tkfs.ui

import org.junit.Assert.assertEquals
import org.junit.Test

class ListingTest {
    private fun row(name: String, directory: Boolean = false, id: String = name) =
        """{"id":"$id","name":"$name","directory":$directory,"size":10,"local_bytes":4,"modified_ms":7}"""

    @Test fun directoriesComeFirstThenNamesWithoutRegardToCase() {
        val json = "[" + listOf(row("b.txt"), row("Zeta", true), row("A.txt"), row("alpha", true)).joinToString(",") + "]"
        assertEquals(listOf("alpha", "Zeta", "A.txt", "b.txt"), parseEntries(json).map { it.name })
    }

    @Test fun theWorkingFilesOfAShareInProgressAreNotListed() {
        val json = "[" + listOf(row(".tkfs-part-1234"), row("photo.jpg")).joinToString(",") + "]"
        assertEquals(listOf("photo.jpg"), parseEntries(json).map { it.name })
    }

    @Test fun anEntryKeepsWhatTheNativeSideSaid() {
        val entry = parseEntries("[" + row("a.bin", id = "e1") + "]").single()
        assertEquals(FileEntry("e1", "a.bin", false, 10, 4, 7), entry)
    }

    @Test fun theWaitingFilesAreNamedByEntity() {
        assertEquals(setOf("e1", "e2"), parseWaitingEntities("""[{"entity":"e1","path":"/a"},{"entity":"e2","path":"/b"}]"""))
        assertEquals(emptySet<String>(), parseWaitingEntities("[]"))
    }
}
