package dev.tkfs.share

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream

/** A folder in memory: files by path, open handles by token, and what was synced. */
private class FakeFolder(existing: Map<String, ByteArray> = emptyMap()) : FolderFiles {
    val files = existing.toMutableMap()
    private val open = HashMap<Long, String>()
    val synced = mutableListOf<String>()
    var failWritesAfter = Int.MAX_VALUE
    var takeNameBeforeRename: String? = null

    override fun names(directory: String) = files.keys.filter { it.substringBeforeLast('/').ifEmpty { "/" } == directory }
        .map { it.substringAfterLast('/') }.toSet()

    override fun create(path: String) {
        if (path in files) throw IOException("ALREADY_EXISTS: $path")
        files[path] = ByteArray(0)
    }

    override fun open(path: String): Long = (open.size + 1L).also { open[it] = path }

    override fun write(token: Long, offset: Long, data: ByteArray, size: Int): Int {
        if (failWritesAfter-- <= 0) throw IOException("STORAGE_FULL: no room")
        val path = open.getValue(token)
        val old = files.getValue(path)
        files[path] = old.copyOf(maxOf(old.size, offset.toInt() + size)).also {
            data.copyInto(it, offset.toInt(), 0, size)
        }
        return size
    }

    override fun sync(token: Long) {
        synced += open.getValue(token)
    }

    override fun close(token: Long) {
        open.remove(token)
    }

    override fun rename(from: String, to: String) {
        takeNameBeforeRename?.let { files[it] = ByteArray(0); takeNameBeforeRename = null }
        if (to in files) throw IOException("ALREADY_EXISTS: $to")
        files[to] = files.remove(from)!!
    }

    override fun delete(path: String) {
        files.remove(path)
    }
}

private class Breaking(private val data: ByteArray, private val after: Int) : InputStream() {
    private var position = 0
    override fun read(): Int = throw UnsupportedOperationException()
    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (position >= after) throw IOException("the sender went away")
        val n = minOf(length, after - position, 4)
        System.arraycopy(data, position, buffer, offset, n)
        position += n
        return n
    }
}

class ImporterTest {
    private val bytes = ByteArray(50) { it.toByte() }

    @Test fun aStreamIsCopiedSealedAndNamedOnlyWhenWhole() {
        val folder = FakeFolder()
        val path = Importer(folder, bufferSize = 8).import("/", "a.bin", ByteArrayInputStream(bytes)) {}
        assertEquals("/a.bin", path)
        assertEquals(listOf("a.bin"), folder.names("/").toList())
        assertTrue(folder.files.getValue("/a.bin").contentEquals(bytes))
        assertEquals("it was sealed before it got its name", 1, folder.synced.size)
        assertTrue(folder.synced.single().startsWith("/.tkfs-part-"))
    }

    @Test fun aClashGetsANumberedNameAndTheOldFileIsKept() {
        val old = byteArrayOf(1)
        val folder = FakeFolder(mapOf("/dir/a.bin" to old))
        val path = Importer(folder).import("/dir", "a.bin", ByteArrayInputStream(bytes)) {}
        assertEquals("/dir/a (1).bin", path)
        assertTrue(folder.files.getValue("/dir/a.bin").contentEquals(old))
    }

    @Test fun aNameTakenBetweenTheCheckAndTheRenameTakesTheNextNumber() {
        val folder = FakeFolder().apply { takeNameBeforeRename = "/a.bin" }
        assertEquals("/a (1).bin", Importer(folder).import("/", "a.bin", ByteArrayInputStream(bytes)) {})
    }

    @Test fun aStreamThatFailsPartwayLeavesNothing() {
        val folder = FakeFolder(mapOf("/keep.txt" to byteArrayOf(9)))
        try {
            Importer(folder, bufferSize = 8).import("/", "a.bin", Breaking(bytes, after = 20)) {}
            fail("the copy should fail")
        } catch (error: ImportFailed) {
            assertTrue(error.message, "the sender went away" in error.message.orEmpty())
        }
        assertEquals("the half copy is gone, whole or under its working name", setOf("/keep.txt"), folder.files.keys)
    }

    @Test fun aWriteThatFailsLeavesNothingEither() {
        val folder = FakeFolder().apply { failWritesAfter = 2 }
        try {
            Importer(folder, bufferSize = 8).import("/", "a.bin", ByteArrayInputStream(bytes)) {}
            fail("the copy should fail")
        } catch (error: ImportFailed) {
            assertTrue(error.message, "STORAGE_FULL" in error.message.orEmpty())
        }
        assertTrue(folder.files.isEmpty())
    }

    @Test fun progressCountsTheBytesCopied() {
        val seen = mutableListOf<Long>()
        Importer(FakeFolder(), bufferSize = 20).import("/", "a.bin", ByteArrayInputStream(bytes)) { seen += it }
        assertEquals(50L, seen.last())
        assertFalse(seen.isEmpty())
    }

    @Test fun aWorkingFileIsWrittenDownBeforeItExistsAndClearedWhenItIsGone() {
        val folder = FakeFolder()
        val told = mutableListOf<String>()
        val importer = Importer(
            folder,
            working = { told += "working ${it.substringAfterLast('/').take(11)} exists=${it in folder.files}" },
            cleared = { told += "cleared ${it.substringAfterLast('/').take(11)}" },
        )
        importer.import("/", "a.bin", ByteArrayInputStream(bytes)) {}
        assertEquals(listOf("working .tkfs-part- exists=false", "cleared .tkfs-part-"), told)
        told.clear()
        try {
            importer.import("/", "b.bin", Breaking(bytes, after = 8)) {}
            fail("the copy should fail")
        } catch (error: ImportFailed) {
            assertEquals(listOf("working .tkfs-part- exists=false", "cleared .tkfs-part-"), told)
        }
    }

    @Test fun aWorkingFileThatCannotBeRemovedIsNotClearedSoTheSweepFindsIt() {
        val folder = object : FolderFiles by FakeFolder() {
            override fun delete(path: String) = throw IOException("WORKER_NOT_RUNNING: gone")
        }
        val told = mutableListOf<String>()
        try {
            Importer(folder, working = { told += "working" }, cleared = { told += "cleared" })
                .import("/", "a.bin", Breaking(bytes, after = 8)) {}
            fail("the copy should fail")
        } catch (error: ImportFailed) {
            assertEquals(listOf("working"), told)
        }
    }

    @Test fun aCreateThatFailsLeavesNothingForTheSweep() {
        val folder = object : FolderFiles by FakeFolder() {
            override fun create(path: String) = throw IOException("WORKER_NOT_READY: x")
        }
        val told = mutableListOf<String>()
        try {
            Importer(folder, working = { told += "working" }, cleared = { told += "cleared" })
                .import("/", "a.bin", ByteArrayInputStream(bytes)) {}
            fail("the copy should fail")
        } catch (error: ImportFailed) {
            assertEquals(listOf("working", "cleared"), told)
        }
    }

    @Test fun aWorkingFileThatIsAlreadyGoneCountsAsRemoved() {
        assertTrue(isNotFound(IOException("NOT_FOUND: /a")))
        assertTrue(isNotFound(IOException("FILE_NOT_FOUND: /a")))
        assertFalse(isNotFound(IOException("WORKER_NOT_READY: f")))
    }

    @Test fun anEmptyStreamIsAnEmptyFile() {
        val folder = FakeFolder()
        Importer(folder).import("/", "empty.txt", ByteArrayInputStream(ByteArray(0))) {}
        assertEquals(0, folder.files.getValue("/empty.txt").size)
    }
}
