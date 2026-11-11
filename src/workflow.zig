//! APIs available inside workflow code, including the deterministic `std.Io` (Go `workflow` package parity).

const Scheduler = @import("workflow/Scheduler.zig");
const WorkflowIo = @import("workflow/WorkflowIo.zig");

test {
    _ = Scheduler;
    _ = WorkflowIo;
}
