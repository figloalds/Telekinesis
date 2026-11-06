package dev.tkfs.ui

import org.junit.Assert.assertEquals
import org.junit.Test
import java.time.LocalDateTime
import java.time.ZoneId

class ConflictsTest {
    private val zone = ZoneId.of("UTC")
    private fun at(day: Int, hour: Int, minute: Int) =
        LocalDateTime.of(2026, 11, day, hour, minute).atZone(zone).toInstant().toEpochMilli()

    private val now = at(5, 12, 0)
    private fun version(author: String, ms: Long, size: Long = 2_300_000, deleted: Boolean = false) =
        VersionEvent("v-$author", author, size, ms, deleted)

    @Test fun thisPhoneToday() {
        assertEquals("This phone, Today at 11:42, 2.3 MB", versionLine(version("phone", at(5, 11, 42)), "phone", now, zone = zone))
    }

    @Test fun aPeerIsNamedByTheCaller() {
        assertEquals("My PC, Today at 11:20, 2.1 MB", versionLine(version("pc", at(5, 11, 20), 2_100_000), "phone", now, zone = zone))
        assertEquals("1a2b3c4d, Today at 11:20, 2.1 MB", versionLine(version("pc", at(5, 11, 20), 2_100_000), "phone", now, "1a2b3c4d", zone))
    }

    @Test fun anEarlierDayNamesTheDate() {
        assertEquals("My PC, Yesterday at 09:05, 12 kB", versionLine(version("pc", at(4, 9, 5), 12_000), "phone", now, zone = zone))
        assertEquals("This phone, 28 Oct at 23:59, 812 MB", versionLine(version("phone", LocalDateTime.of(2026, 10, 28, 23, 59).atZone(zone).toInstant().toEpochMilli(), 812_000_000), "phone", now, zone = zone))
    }

    @Test fun aHeadThatDeletesSaysWhereItWasDeleted() {
        assertEquals("Deleted on My PC, Today at 11:20", versionLine(version("pc", at(5, 11, 20), 0, deleted = true), "phone", now, zone = zone))
        assertEquals("Deleted on this phone, Today at 11:42", versionLine(version("phone", at(5, 11, 42), 0, deleted = true), "phone", now, zone = zone))
    }

    @Test fun anAuthorIsThisPhoneMyPcOrAShortId() {
        assertEquals("This phone", versionAuthor("dev-phone", "dev-phone", 2))
        assertEquals("My PC", versionAuthor("0123456789abcdef", "dev-phone", 1))
        assertEquals("01234567", versionAuthor("0123456789abcdef", "dev-phone", 2))
    }

    @Test fun conflictsReadFromTheOrchestratorsRows() {
        val modified = 116_444_736_000_000_000L + 1_700_000_000_000L * 10_000
        val head = { id: String, author: String, deleted: Boolean ->
            """{"id":"$id","author":"$author","state":{"name":"a.txt","deleted":$deleted,"content":{"total_len":5},"times":{"modified":$modified}}}"""
        }
        val json = """[{"entity":"e1","branch":"b","visible":"x","heads":[${head("h1", "pc", false)},${head("h2", "phone", true)}],"resolved":false},
            {"entity":"e2","branch":"b","visible":"x","heads":[${head("h3", "pc", false)}],"resolved":true}]"""
        val parsed = parseConflicts(json)
        assertEquals(listOf("e1"), parsed.map { it.entity })
        assertEquals("a.txt", parsed[0].name)
        assertEquals(listOf("h1", "h2"), parsed[0].versions.map { it.id })
        assertEquals(1_700_000_000_000L, parsed[0].versions[0].modifiedMs)
        assertEquals(listOf(false, true), parsed[0].versions.map { it.deleted })
        assertEquals(5L, parsed[0].versions[0].size)
    }
}
