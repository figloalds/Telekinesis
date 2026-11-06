package dev.tkfs.provider

/**
 * The `inSampleSize` that decodes a `width` by `height` image to the smallest power of two that
 * still covers `wantedWidth` by `wantedHeight`, so a thumbnail never costs a full decode.
 */
fun sampleSize(width: Int, height: Int, wantedWidth: Int, wantedHeight: Int): Int {
    var sample = 1
    while (width / (sample * 2) >= wantedWidth && height / (sample * 2) >= wantedHeight) sample *= 2
    return sample
}

/** Whether a thumbnail is offered for a file: an image that is all on the phone. */
fun offersThumbnail(mime: String, size: Long, localBytes: Long): Boolean =
    mime.startsWith("image/") && size > 0 && localBytes >= size
