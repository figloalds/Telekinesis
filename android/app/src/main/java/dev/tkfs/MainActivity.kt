package dev.tkfs

import android.app.Activity
import android.os.Bundle
import android.view.Gravity
import android.widget.TextView

/** Only the launcher entry for now: opening it starts the orchestrator. */
class MainActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Native.ensureStarted(this)
        setContentView(
            TextView(this).apply {
                text = "Telekinesis"
                textSize = 24f
                gravity = Gravity.CENTER
            },
        )
    }
}
