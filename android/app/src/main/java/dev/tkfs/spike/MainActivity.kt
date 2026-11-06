package dev.tkfs.spike

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import android.provider.DocumentsContract
import android.text.InputType
import android.util.Log
import android.view.WindowInsets
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import org.json.JSONObject
import java.io.IOException
import kotlin.concurrent.thread

/**
 * Just enough to pair and receive. `am start` can drive it for testing:
 * --es action pair|remote-list|receive|status|peers|list
 * --es endpoint ... --es code ... --es repo ... --es label ...
 */
class MainActivity : Activity() {
    private lateinit var endpoint: EditText
    private lateinit var code: EditText
    private lateinit var repo: EditText
    private lateinit var label: EditText
    private lateinit var output: TextView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Native.ensureStarted(this)

        val form = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(32, 32, 32, 32)
        }
        endpoint = field(form, "Endpoint (ws://host:port)")
        code = field(form, "Pairing code").apply {
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD
        }
        repo = field(form, "Repository id")
        label = field(form, "Folder label")
        button(form, "Pair") { run("pair") }
        button(form, "List remote folders") { run("remote-list") }
        button(form, "Receive") { run("receive") }
        button(form, "Status") { run("status") }
        output = TextView(this).apply { setTextIsSelectable(true) }
        form.addView(output)

        val page = ScrollView(this).apply { addView(form) }
        page.setOnApplyWindowInsetsListener { view, insets ->
            val bars = insets.getInsets(WindowInsets.Type.systemBars())
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            insets
        }
        setContentView(page)
        handle(intent)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        handle(intent)
    }

    private fun handle(intent: Intent) {
        val action = intent.getStringExtra("action") ?: return
        intent.getStringExtra("endpoint")?.let(endpoint::setText)
        intent.getStringExtra("code")?.let(code::setText)
        intent.getStringExtra("repo")?.let(repo::setText)
        intent.getStringExtra("label")?.let(label::setText)
        run(action)
    }

    private fun run(action: String) {
        val payload = when (action) {
            "pair" -> JSONObject().put("endpoint", text(endpoint)).put("code", text(code))
            "remote-list" -> JSONObject().put("endpoint", text(endpoint))
            "receive" -> JSONObject()
                .put("endpoint", text(endpoint))
                .put("repo", text(repo))
                .put("label", text(label))
            else -> JSONObject()
        }
        output.text = "$action…"
        thread(name = "tkfs-manage") {
            val started = System.nanoTime()
            val result = try {
                Native.manage(action, payload.toString())
            } catch (error: IOException) {
                "ERROR ${error.message}"
            }
            val elapsedMs = (System.nanoTime() - started) / 1_000_000
            Log.i("tkfs", "manage $action ($elapsedMs ms): $result")
            if (action == "receive") {
                contentResolver.notifyChange(
                    DocumentsContract.buildRootsUri("dev.tkfs.spike.documents"),
                    null,
                )
            }
            runOnUiThread { output.text = "$action: $result" }
        }
    }

    private fun text(field: EditText) = field.text.toString().trim()

    private fun field(parent: LinearLayout, hint: String) = EditText(this).also {
        it.hint = hint
        it.setSingleLine()
        parent.addView(it)
    }

    private fun button(parent: LinearLayout, title: String, action: () -> Unit) {
        parent.addView(Button(this).apply {
            text = title
            setOnClickListener { action() }
        })
    }
}
