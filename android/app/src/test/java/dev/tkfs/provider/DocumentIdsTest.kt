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

    @Test fun aDocumentsParentIsTheDirectoryAboveIt() {
        assertEquals("F:/", DocumentIds.parent("F:/x"))
        assertEquals("F:/a", DocumentIds.parent("F:/a/b:c d"))
        assertEquals("F:/a/b", DocumentIds.parent("F:/a/b/c"))
    }

    @Test fun aRootIsItsOwnParent() {
        assertEquals("F:/", DocumentIds.parent("F:/"))
    }

    @Test fun aHardNameIsAName() {
        for (name in listOf("a b%#:é.txt", "...", "..a", "a..", ".profile", " ")) {
            assertEquals(name, DocumentIds.requireName(name))
        }
    }

    /** A separator would put the file somewhere else, and the core treats a backslash as one. */
    @Test fun whatIsNotANameIsRefusedWithItsCode() {
        for (name in listOf("", ".", "..", "a/b", "/", "a\\b", "\\", "a\u0000b")) {
            val thrown = assertThrows(name, FileNotFoundException::class.java) { DocumentIds.requireName(name) }
            assertEquals(name, "INVALID_NAME", thrown.message?.substringBefore(':'))
        }
    }

    @Test fun aScreenAsksTheSameQuestionTheProviderDoes() {
        for (name in listOf("a b%#:é.txt", "...", ".profile", " ")) assertTrue(name, DocumentIds.isValidName(name))
        for (name in listOf("", ".", "..", "a/b", "a\\b", "a\u0000b")) assertFalse(name, DocumentIds.isValidName(name))
    }

    /** A tree grant on /a must not reach /b through a dot-dot. */
    @Test fun aPathThatClimbsOutIsNotAChild() {
        assertFalse(DocumentIds.isChild("F:/a", "F:/a/../b"))
        assertFalse(DocumentIds.isChild("F:/a", "F:/a/./b"))
        assertFalse(DocumentIds.isChild("F:/a", "F:/a/..\\b"))
    }
}
