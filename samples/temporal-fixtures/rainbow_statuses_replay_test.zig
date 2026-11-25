//! Replaying a recorded history is how to check that a change to workflow code is backward
//! compatible. rainbow_statuses.json is the history of a `<prefix>_Completed` run of this
//! sample, exported with the Temporal CLI:
//!
//!     temporal workflow show --workflow-id <prefix>_Completed --output json > rainbow_statuses.json

const std = @import("std");
const temporal = @import("temporal");
const rainbow_statuses = @import("rainbow_statuses.zig");

test "replay a recorded history" {
    var replayer: temporal.testing.WorkflowReplayer = .init(std.testing.allocator, std.testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(rainbow_statuses.RainbowStatusesWorkflow);
    try replayer.replayHistoryJson(@embedFile("rainbow_statuses.json"));
}
