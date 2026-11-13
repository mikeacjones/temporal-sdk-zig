`workflow.Context.executeChildWorkflow` starts a child workflow and returns a `workflow.ChildWorkflowFuture` for its start and result, which can also signal and cancel it.
`ctx.signalExternalWorkflow` and `ctx.cancelExternalWorkflow` signal or cancel another workflow by ID.
Returning `ctx.continueAsNew` continues the workflow as a new run; `ctx.continueAsNewSuggested`, `historyLength` and `historySizeBytes` say when to.
