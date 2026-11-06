package dev.tkfs.sync

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class TransferStateTest {
    private fun folder(id: String, remaining: Int?, pushed: Long = 0, error: String? = null) =
        FolderBurst(id, exchanged = 1, remaining = remaining, error = error?.let { BurstError(it, null) }, pushed = pushed)

    private fun burst(vararg folders: FolderBurst) = BurstResult(folders.toList())

    @Test fun aBurstWithNothingLeftEndsTheJob() {
        assertEquals(Next.Stop(Why.Done), Progress().after(burst(folder("a", 0), folder("b", null))))
    }

    @Test fun aBurstThatLeavesWorkGoesAgain() {
        assertEquals(Next.Again, Progress().after(burst(folder("a", 5, pushed = 2))))
    }

    @Test fun aFirstBurstThatMovedNothingGetsASecondChance() {
        // There is nothing to compare it with yet.
        assertEquals(Next.Again, Progress().after(burst(folder("a", 5))))
    }

    @Test fun theSameRemainingWithNothingPushedIsNoProgress() {
        val progress = Progress()
        assertEquals(Next.Again, progress.after(burst(folder("a", 5))))
        assertEquals(Next.Stop(Why.NoProgress), progress.after(burst(folder("a", 5))))
    }

    @Test fun fewerRemainingOrAPushIsProgress() {
        val progress = Progress()
        progress.after(burst(folder("a", 5)))
        assertEquals(Next.Again, progress.after(burst(folder("a", 4))))
        assertEquals(Next.Again, progress.after(burst(folder("a", 4, pushed = 1))))
    }

    @Test fun progressIsComparedPerFolderAcrossBursts() {
        val progress = Progress()
        progress.after(burst(folder("a", 5), folder("b", 3)))
        // "a" is stuck but "b" moved: the job goes on.
        assertEquals(Next.Again, progress.after(burst(folder("a", 5), folder("b", 2))))
        // A folder that was not in the last burst has nothing to be stuck against.
        assertEquals(Next.Again, progress.after(burst(folder("a", 5), folder("c", 7))))
        // Now "a" and "c" are both as they were.
        assertEquals(Next.Stop(Why.NoProgress), progress.after(burst(folder("a", 5), folder("c", 7))))
    }

    @Test fun aFolderInErrorThatPushedNothingDoesNotKeepTheJobGoing() {
        assertEquals(Next.Stop(Why.NoProgress), Progress().after(burst(folder("a", 5, error = "PC not reachable"))))
        assertEquals(Next.Stop(Why.NoProgress), Progress().after(burst(folder("a", null, error = "PC not reachable"))))
    }

    @Test fun aBurstThatRanOutOfTimeWithNothingLeftToAskForButPushedGoesAgain() {
        assertEquals(Next.Again, Progress().after(burst(folder("a", 0, pushed = 1120, error = "SYNC_NOW_TIMEOUT"))))
    }

    @Test fun aBurstThatRanOutOfTimeButPushedGoesAgain() {
        assertEquals(Next.Again, Progress().after(burst(folder("a", 12, pushed = 1364, error = "SYNC_NOW_TIMEOUT"))))
    }

    @Test fun aFailedCallEndsTheJob() {
        assertEquals(Next.Stop(Why.Failed), Progress().after(BurstResult(failure = "not running")))
    }

    @Test fun aChangeDuringAJobIsNotQueuedAgainAndMakesItGoOnce() {
        val gate = TransferGate()
        assertTrue("the first change asks for a job", gate.changed(1_000))
        assertFalse("while it waits, another needs none", gate.changed(2_000))
        gate.started()
        val claimed = gate.claim()
        assertFalse("while it runs, neither", gate.changed(3_000))
        assertFalse("a change since the claim means one more burst", gate.finish(claimed))
        assertTrue("with none since, it ends", gate.finish(gate.claim()))
        assertTrue("and the next change asks again", gate.changed(9_000))
    }

    @Test fun aStoppedJobLetsTheNextChangeAskAgain() {
        val gate = TransferGate()
        gate.changed(0)
        gate.started()
        gate.stopped()
        assertTrue(gate.changed(1))
    }

    @Test fun aJobWaitsUntilTheChangesHaveBeenQuietFor5Seconds() {
        val gate = TransferGate()
        gate.changed(10_000)
        gate.changed(12_000)
        assertEquals(5_000, gate.quietFor(12_000))
        assertEquals(1_000, gate.quietFor(16_000))
        assertEquals(0, gate.quietFor(17_000))
        assertEquals(0, TransferGate().quietFor(1))
    }
}
