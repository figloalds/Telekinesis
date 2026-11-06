package dev.tkfs.ui

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class AppUiTest {
    private val empty = Snapshot(emptyList(), emptyList())
    private val late = Failure("REQUEST_TIMEOUT", "The request took too long.")

    @Test fun aPollFailureClearsOnTheNextPollThatWorks() {
        val failed = AppUi().pollFailed(late)
        assertEquals(late, failed.pollFailure)
        assertNull(failed.polled(empty, "", 5).pollFailure)
    }

    @Test fun aPollFailureIsNeverAnActionsFailureAndDoesNotReplaceIt() {
        val refused = Failure("INVALID_PAIRING_CODE", "Wrong code.")
        val failed = AppUi(failure = refused).pollFailed(late)
        assertEquals(refused, failed.failure)
        assertNull(AppUi().pollFailed(late).failure)
        // A good poll leaves the action's failure for its own screen to clear.
        assertEquals(refused, failed.polled(empty, "", 5).failure)
    }

    @Test fun theBatteryRowSaysWhetherTheAppMayRunInTheBackground() {
        assertEquals("Allowed to run in the background", batteryLine(true))
        assertEquals("Not allowed. Sending files can wait until the phone is in use.", batteryLine(false))
    }
}
