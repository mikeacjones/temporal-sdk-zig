`ctx.patched`, `ctx.deprecatePatch` and `ctx.getVersion` keep old and new workflow code paths while runs on the old one remain.
`ctx.sideEffect` records a nondeterministic value once; `ctx.upsertMemo`, `memoValue` and typed search attributes from `search_attributes.key` read and update the memo and search attributes.
`ctx.info()`, `ctx.isReplaying()`, `ctx.lastCompletionResult` and `ctx.previousRunFailure()` describe the run, including cron and retried runs.
