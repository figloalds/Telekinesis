package dev.tkfs.sync

import org.junit.Assert.assertEquals
import org.junit.Test

class SyncPolicyTest {
    private val policy = SyncPolicy()
    private val now = 10_000_000L

    @Test fun foregroundWaitsThirtySeconds() {
        assertEquals(Mode.Skip, policy.decide(Trigger.Foreground, now, now - 29_999, metered = false, syncOnMobile = false))
        assertEquals(Mode.Full, policy.decide(Trigger.Foreground, now, now - 30_000, metered = false, syncOnMobile = false))
        assertEquals(Mode.Full, policy.decide(Trigger.Foreground, now, 0, metered = false, syncOnMobile = false))
    }

    @Test fun aListingSyncsOnlyWhenOlderThanTwoMinutes() {
        assertEquals(Mode.Skip, policy.decide(Trigger.Listing, now, now - 119_999, metered = false, syncOnMobile = false))
        assertEquals(Mode.Full, policy.decide(Trigger.Listing, now, now - 120_000, metered = false, syncOnMobile = false))
    }

    @Test fun meteredWithTheSettingOffMovesMetadataOnly() {
        for (trigger in listOf(Trigger.Foreground, Trigger.Listing, Trigger.Periodic)) {
            assertEquals(trigger.name, Mode.MetadataOnly, policy.decide(trigger, now, 0, metered = true, syncOnMobile = false))
        }
    }

    @Test fun manualIsAlwaysFull() {
        for (metered in listOf(true, false)) {
            for (mobile in listOf(true, false)) {
                assertEquals(Mode.Full, policy.decide(Trigger.Manual, now, now, metered, mobile))
            }
        }
    }

    @Test fun meteredWithTheSettingOnIsFull() {
        for (trigger in Trigger.entries) {
            assertEquals(trigger.name, Mode.Full, policy.decide(trigger, now, 0, metered = true, syncOnMobile = true))
        }
    }

    @Test fun periodicNeedsNoAge() {
        assertEquals(Mode.Full, policy.decide(Trigger.Periodic, now, now, metered = false, syncOnMobile = false))
    }

    @Test fun onlyAMeteredNetworkWithTheSettingOffHoldsFilesBack() {
        assertEquals(true, holdsFilesBack(metered = true, syncOnMobile = false))
        assertEquals(false, holdsFilesBack(metered = true, syncOnMobile = true))
        assertEquals(false, holdsFilesBack(metered = false, syncOnMobile = false))
        assertEquals(false, holdsFilesBack(metered = false, syncOnMobile = true))
    }

    @Test fun aLocalChangeWaitsForAnUnmeteredNetworkWhenTheSettingIsOff() {
        assertEquals(Mode.Skip, policy.decide(Trigger.LocalChange, now, now, metered = true, syncOnMobile = false))
        assertEquals(Mode.Full, policy.decide(Trigger.LocalChange, now, now, metered = false, syncOnMobile = false))
    }
}
