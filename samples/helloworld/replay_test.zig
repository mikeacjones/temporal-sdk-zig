//! Replaying a recorded history is how to check that a change to workflow code is backward
//! compatible. helloworld.json is a history of this sample, exported with the Temporal CLI:
//!
//!     temporal workflow show --workflow-id hello-world --output json > helloworld.json

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld.zig");

test "replay a recorded history" {
    var replayer: temporal.testing.WorkflowReplayer = .init(std.testing.allocator, std.testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(helloworld.HelloWorld);
    try replayer.replayHistoryJson(@embedFile("helloworld.json"));
}
