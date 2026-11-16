`Client.updateWithStartWorkflow` now logs the server's message and status code when update-with-start fails with every operation marked aborted; it returned `error.RpcFailed` without logging why.
