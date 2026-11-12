//! The trip workflow keeps a user's trip count. Each `trip_event` signal is a completed trip
//! and adds one to the count, which the `counter` query reads. After ten trips the run
//! continues as new, carrying the count forward in its argument.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;

/// The workflow's state, passed from one run to the next when it continues as new.
pub const UserState = struct {
    trip_counter: i64 = 0,
};

/// A completed trip, sent to the workflow as a `trip_event` signal.
pub const TripEvent = struct {
    id: []const u8,
    total: i64,
};

pub const TripSignal = workflow.SignalDef("trip_event", fn (trip: TripEvent) void);

/// Reads the trip count.
pub const Counter = workflow.QueryDef("counter", fn () i64);

/// Trips received per run before it continues as new.
pub const trips_per_run = 10;

/// What the signal and query handlers share with the workflow function. Allocated with
/// `ctx.allocator()`: handlers run outside the workflow function's coroutine.
const Trips = struct {
    gpa: std.mem.Allocator,
    state: UserState,
    /// Every trip signaled to this run, in arrival order; the workflow counts them one at a
    /// time.
    received: std.ArrayList(TripEvent) = .empty,
    counted: usize = 0,

    fn onTrip(t: *Trips, trip: TripEvent) !void {
        try t.received.append(t.gpa, trip);
    }

    fn counter(t: *Trips) i64 {
        return t.state.trip_counter;
    }

    fn hasUncounted(t: *Trips) bool {
        return t.received.items.len > t.counted;
    }
};

fn tripWorkflow(ctx: *workflow.Context, state: UserState) !void {
    const logger = ctx.logger();
    logger.info("Trip Workflow Started for User.", .{ .user = ctx.info().workflow_id, .trip_counter = state.trip_counter });

    const trips = try ctx.allocator().create(Trips);
    trips.* = .{ .gpa = ctx.allocator(), .state = state };
    ctx.setQueryHandler(Counter, trips, Trips.counter) catch |err| {
        logger.info("SetQueryHandler failed.", .{ .err = err });
        return err;
    };
    try ctx.setSignalHandler(TripSignal, trips, Trips.onTrip);

    for (0..trips_per_run) |_| {
        try ctx.await(trips, Trips.hasUncounted);
        const trip = trips.received.items[trips.counted];
        trips.counted += 1;
        logger.info("Trip complete event received.", .{ .id = trip.id, .total = trip.total });
        trips.state.trip_counter += 1;
    }

    logger.info("Starting a new run.", .{ .trip_counter = trips.state.trip_counter });
    return ctx.continueAsNew(TripWorkflow, .{}, .{trips.state});
}

pub const TripWorkflow = workflow.Def("TripWorkflow", tripWorkflow);
