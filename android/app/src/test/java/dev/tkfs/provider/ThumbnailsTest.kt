package dev.tkfs.provider

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ThumbnailsTest {
    @Test fun aLargePhotoIsDecodedAtTheSmallestSizeThatCoversTheRequest() {
        assertEquals(8, sampleSize(4000, 3000, 320, 320))
        assertEquals(1, sampleSize(300, 200, 320, 320))
        assertEquals(1, sampleSize(640, 640, 320, 320).let { it / 2 })
    }

    @Test fun neitherSideFallsBelowTheRequest() {
        // Halving 4000x600 once more would leave 300 high, under the 320 wanted.
        assertEquals(1, sampleSize(4000, 600, 320, 320))
    }

    @Test fun onlyAnImageThatIsAllHereOffersAThumbnail() {
        assertTrue(offersThumbnail("image/jpeg", 100, 100))
        assertFalse(offersThumbnail("image/jpeg", 100, 99))
        assertFalse(offersThumbnail("image/jpeg", 100, 0))
        assertFalse(offersThumbnail("video/mp4", 100, 100))
        assertFalse(offersThumbnail("image/jpeg", 0, 0))
    }
}
