`Client.terminateWorkflowWithOptions` records details in the terminated event and, with `client.TerminateOptions.first_execution_run_id`, terminates only the chain of runs that started with that run.
`Client.resetWorkflow` resets a workflow to the end of an earlier workflow task in a new run, with `client.ResetOptions` choosing which later signals, updates, and Nexus events are not reapplied.
`Client.updateWorkflowExecutionOptions` sets or removes a running workflow's versioning override and returns its `client.WorkflowExecutionOptions`.
