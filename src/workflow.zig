//! APIs available inside workflow code, including the deterministic `std.Io` (Go `workflow` package parity).

const Scheduler = @import("workflow/Scheduler.zig");

test {
    _ = Scheduler;
}
