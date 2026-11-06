package dev.tkfs

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import androidx.work.ForegroundInfo
import dev.tkfs.ui.MainActivity
import dev.tkfs.ui.unreachableOpen

/**
 * The notifications of the transfer job and of sharing in. Without the notification permission
 * they are not shown, and the work behind them runs all the same.
 */
object Notifications {
    private const val WORKING = "working"
    private const val RESULT = "result"

    const val TRANSFER_ID = 41
    const val SHARE_ID = 42
    private const val SHARE_RESULT_ID = 43
    private const val OPEN_FAILED_ID = 44

    /** The job's notification: it is what lets Android keep the app's network while it sends. */
    fun transfer(context: Context, text: String): ForegroundInfo {
        val notification = builder(context, WORKING)
            .setContentTitle("Sending to your PC")
            .setContentText(text)
            .setProgress(0, 0, true)
            .setOngoing(true)
            .build()
        return ForegroundInfo(TRANSFER_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
    }

    /** Copying a shared file in. `total` is 0 when its size is unknown. */
    fun sharing(context: Context, text: String, done: Long, total: Long): Notification {
        val builder = builder(context, WORKING).setContentTitle("Saving to Telekinesis").setContentText(text).setOngoing(true)
        if (total > 0) builder.setProgress(PERCENT, (done * PERCENT / total).toInt().coerceIn(0, PERCENT), false)
        else builder.setProgress(0, 0, true)
        return builder.build()
    }

    /** What sharing in came to; it stays until dismissed, so a failure is not missed. */
    fun shared(context: Context, title: String, text: String) {
        val notification = builder(context, RESULT)
            .setContentTitle(title)
            .setContentText(text)
            .setStyle(Notification.BigTextStyle().bigText(text))
            .setAutoCancel(true)
            .build()
        context.getSystemService(NotificationManager::class.java).notify(SHARE_RESULT_ID, notification)
    }

    /** A file the owner opened could not be, because no PC answered. Nothing is shown without the permission. */
    fun cannotOpen(context: Context, name: String) {
        val manager = context.getSystemService(NotificationManager::class.java)
        if (!manager.areNotificationsEnabled()) return
        val notification = builder(context, RESULT)
            .setContentTitle("Couldn't open $name")
            .setContentText(unreachableOpen(name))
            .setStyle(Notification.BigTextStyle().bigText(unreachableOpen(name)))
            .setAutoCancel(true)
            .build()
        // One per file, so a second file does not replace the first.
        manager.notify(name, OPEN_FAILED_ID, notification)
    }

    private fun builder(context: Context, channel: String): Notification.Builder {
        val manager = context.getSystemService(NotificationManager::class.java)
        // minSdk is 26, where channels begin.
        manager.createNotificationChannel(
            if (channel == WORKING) {
                NotificationChannel(WORKING, "Sending and saving", NotificationManager.IMPORTANCE_LOW)
            } else {
                NotificationChannel(RESULT, "Saved files", NotificationManager.IMPORTANCE_DEFAULT)
            },
        )
        val open = PendingIntent.getActivity(
            context, 0, Intent(context, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE,
        )
        return Notification.Builder(context, channel).setSmallIcon(R.drawable.ic_notification).setContentIntent(open)
    }

    private const val PERCENT = 100
}
