`testing.WorkflowEnvironment` runs workflows against a local dev server, with activity doubles and signal, query and update helpers; `WorkflowEnvironment.startTimeSkipping` uses the time-skipping test server.
`testing.ActivityEnvironment` runs an activity function without a server.
`testing.WorkflowReplayer` replays recorded histories, from JSON, files, a client or an iterator, to catch nondeterministic changes, one result per history.
`testing.DevServer` and `testing.TestServer` start a local Temporal dev server or time-skipping test server.
