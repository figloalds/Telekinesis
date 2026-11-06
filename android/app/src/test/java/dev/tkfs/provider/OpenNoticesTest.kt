package dev.tkfs.provider

import org.junit.Assert.assertEquals
import org.junit.Test

class OpenNoticesTest {
    private class Memory : NoticeStore {
        var text: String? = null
        override fun read() = text
        override fun write(text: String) {
            this.text = text
        }
    }

    private val store = Memory()

    @Test fun aNameStaysUntilDismissedEvenInANewProcess() {
        OpenNotices(store).add("video-obra.mp4")
        OpenNotices(store).add("video-obra.mp4")
        OpenNotices(store).add("foto.jpg")
        assertEquals(listOf("video-obra.mp4", "foto.jpg"), OpenNotices(store).names())
        OpenNotices(store).dismiss()
        assertEquals(emptyList<String>(), OpenNotices(store).names())
    }

    @Test fun unreadableTextIsNoNotices() {
        store.text = "{not json"
        assertEquals(emptyList<String>(), OpenNotices(store).names())
        OpenNotices(store).add("a")
        assertEquals(listOf("a"), OpenNotices(store).names())
    }
}
