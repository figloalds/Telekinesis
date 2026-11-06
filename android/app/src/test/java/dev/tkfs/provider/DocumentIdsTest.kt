package dev.tkfs.provider

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.FileNotFoundException

class DocumentIdsTest {
    @Test fun splitsAtTheFirstColonOnly() {
        assertEquals("F" to "/a:b c%#é.txt", DocumentIds.split("F:/a:b c%#é.txt"))
    }

    @Test fun anIdWithNoColonIsNotADocument() {
        assertThrows(FileNotFoundException::class.java) { DocumentIds.split("no-colon") }
    }

    @Test fun makeIsTheInverseOfSplit() {
        val id = DocumentIds.make("F", "/a:b c%#é.txt")
        assertEquals("F:/a:b c%#é.txt", id)
        assertEquals("F" to "/a:b c%#é.txt", DocumentIds.split(id))
    }

    @Test fun aChildPathAddsOneNameUnderItsParent() {
        assertEquals("/x", DocumentIds.childPath("/", "x"))
        assertEquals("/a/b:c", DocumentIds.childPath("/a", "b:c"))
    }

    @Test fun aSiblingWithTheSamePrefixIsNotAChild() {
        assertFalse(DocumentIds.isChild("F:/a", "F:/ab/c"))
    }

    @Test fun theRootHoldsItsChildren() {
        assertTrue(DocumentIds.isChild("F:/", "F:/x"))
    }

    @Test fun aDeeperDocumentIsAChild() {
        assertTrue(DocumentIds.isChild("F:/a", "F:/a/b/c"))
        assertTrue(DocumentIds.isChild("F:/a b%#:é", "F:/a b%#:é/c"))
    }

    @Test fun aDocumentIsNotItsOwnChild() {
        assertFalse(DocumentIds.isChild("F:/", "F:/"))
        assertFalse(DocumentIds.isChild("F:/a", "F:/a"))
        assertFalse(DocumentIds.isChild("F:/a", "F:/a/"))
    }

    @Test fun anotherFolderIsNotAChild() {
        assertFalse(DocumentIds.isChild("F:/", "G:/x"))
    }

    /** A tree grant on /a must not reach /b through a dot-dot. */
    @Test fun aPathThatClimbsOutIsNotAChild() {
        assertFalse(DocumentIds.isChild("F:/a", "F:/a/../b"))
        assertFalse(DocumentIds.isChild("F:/a", "F:/a/./b"))
        assertFalse(DocumentIds.isChild("F:/a", "F:/a/..\\b"))
    }
}
