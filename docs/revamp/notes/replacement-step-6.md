# Step 6: digest fetch and event replication

The replacement `meta.exchange` applies events independently of object
availability. `objects.fetch` accepts bounded batches of digests over the
existing authenticated session transport. Responses are verified before pack
installation. There are no per-reference grants, lease generations or object
admission rows in this path.

The shared fetcher prioritizes foreground hydration over prefetch, bounds each
batch and source attempts, and keeps one exponential backoff per peer. Feed
cursors advance only after apply; replayed event identities are idempotent.

A reply can omit an event whose complete decoded fields match an event in
that same request. Its cursor and `more` flag still cover the full scanned
page, so a filtered empty page can require another exchange. This retains no
cross-request knowledge and does not filter by author; empty-request replay
still returns unknown self-authored recovery history. The
[request-echo contract and checks](feed-request-echo.md) cover this optimization.

A real TCP/session test pairs two peers, exchanges metadata with no objects,
opens a remote manifest, fetches just its short final chunk and verifies that
the unrelated first chunk stays absent. Existing session authentication and
transport code are retained. Old admission modules are removed at cutover.
