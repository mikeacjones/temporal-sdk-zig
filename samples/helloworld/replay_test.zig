//! Port of samples-go/helloworld/replay_test.go.
//!
//! Replaying a recorded history is the recommended way to check that a change to workflow code
//! is backward compatible. helloworld.json was recorded from the Go sample and can be produced
//! with the Temporal CLI:
//!
//!     temporal workflow show --workflow-id hello_world_workflowID --output json > helloworld.json

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld.zig");

test "ReplayWorkflowHistoryFromFile" {
    var replayer: temporal.testing.WorkflowReplayer = .init(std.testing.allocator, std.testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(helloworld.Workflow);
    try replayer.replayHistoryJson(@embedFile("helloworld.json"));
}
