`Client.fetchHistory`, `HistoryIterator.next` and `WorkflowRun.get` now read history pages a server sends as raw history (`raw_history`, as PROTO3 or JSON blobs) as the same events as ordinary pages; they returned an empty or truncated history, and `get` asked the server again without end. A raw blob the client can't read returns `error.InvalidResponse`, logged as a warning.
<<<<<<< HEAD
=======
`WorkflowRun.get` now returns `error.InvalidResponse`, logged as a warning, for a history page with no events and no page token for a run that has closed, instead of asking the server again at once without end.
>>>>>>> e0bd20f8aa80ce0bec007586c47369f3f8171199
