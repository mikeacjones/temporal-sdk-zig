//! Mutex: a workflow that serializes access to a resource across workflows.
//!
//! A workflow that needs the resource calls `Mutex.lock`. A local activity signals-with-start
//! the resource's mutex workflow ("mutex:<namespace>:<resource_id>") with
//! "request-lock-event", carrying the caller's workflow ID. The mutex workflow takes the
//! requests one at a time: for each it records a release signal name in a side effect,
//! "unlock-event-<caller's workflow ID>", and hands it to the caller with
//! "acquire-lock-event". The caller holds the lock until it sends that signal back with
//! `Unlock.release`, or until the unlock timeout passes. The mutex workflow ends once no
//! request is waiting; the next request starts a new run.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const Duration = std.Io.Duration;

pub const task_queue = "mutex";

/// Sent by the mutex workflow to the caller it grants the lock to, with the name of the
/// signal that releases it.
pub const AcquireLock = workflow.SignalDef("acquire-lock-event", fn (release_lock_channel_name: []const u8) void);
/// Sent to the mutex workflow by a caller asking for the lock, with the caller's workflow ID.
pub const RequestLock = workflow.SignalDef("request-lock-event", fn (sender_workflow_id: []const u8) void);

/// A run of the mutex workflow.
pub const Execution = struct {
    workflow_id: []const u8,
    run_id: []const u8,
};

/// A lock held on a resource, released with `release`.
pub const Unlock = struct {
    execution: Execution,
    release_lock_channel_name: []const u8,

    /// Sends the release signal to the mutex workflow and waits for the server to deliver it.
    pub fn release(u: Unlock, ctx: *workflow.Context) !void {
        try ctx.signalExternalWorkflowByName(u.release_lock_channel_name, u.execution.workflow_id, u.execution.run_id, .{"releaseLock"}).get();
    }
};

/// How the lock's local activity and the mutex workflow it starts are retried.
const retry_policy: workflow.RetryPolicy = .{
    .initial_interval = .fromSeconds(1),
    .backoff_coefficient = 2.0,
    .maximum_interval = .fromSeconds(60),
    .maximum_attempts = 5,
};

pub const Mutex = struct {
    current_workflow_id: []const u8,
    lock_namespace: []const u8,

    /// Blocks until this workflow holds the lock on `resource_id`. The mutex workflow takes
    /// the lock back after `unlock_timeout` if `Unlock.release` hasn't released it by then.
    pub fn lock(m: Mutex, ctx: *workflow.Context, resource_id: []const u8, unlock_timeout: Duration) !Unlock {
        const lao: workflow.LocalActivityOptions = .{
            .schedule_to_close_timeout = .fromSeconds(60),
            .retry_policy = retry_policy,
        };
        const execution = try ctx.executeLocalActivity(SignalWithStartMutexWorkflowActivity, lao, .{
            m.lock_namespace, resource_id, m.current_workflow_id, unlock_timeout,
        }).get();

        const acquired = try ctx.allocator().create(Acquired);
        acquired.* = .{};
        try ctx.setSignalHandler(AcquireLock, acquired, Acquired.acquire);
        try ctx.await(acquired, Acquired.done);
        return .{ .execution = execution, .release_lock_channel_name = acquired.release_lock_channel_name.? };
    }

    const Acquired = struct {
        release_lock_channel_name: ?[]const u8 = null,

        fn acquire(a: *Acquired, release_lock_channel_name: []const u8) void {
            a.release_lock_channel_name = release_lock_channel_name;
        }
        fn done(a: *Acquired) bool {
            return a.release_lock_channel_name != null;
        }
    };
};

/// The mutex workflow's state, shared with its signal handlers.
const MutexState = struct {
    ctx: *workflow.Context,
    /// Workflow IDs of the callers waiting for the lock, in arrival order.
    requests: std.ArrayList([]const u8) = .empty,
    next_request: usize = 0,
    /// The current holder's release signal's argument, once it has arrived.
    ack: ?[]const u8 = null,

    fn requestLock(s: *MutexState, sender_workflow_id: []const u8) !void {
        try s.requests.append(s.ctx.allocator(), sender_workflow_id);
    }
    fn releaseLock(s: *MutexState, ack: []const u8) void {
        s.ack = ack;
    }
    fn released(s: *MutexState) bool {
        return s.ack != null;
    }
};

const UnlockChannel = struct {
    allocator: std.mem.Allocator,
    sender_workflow_id: []const u8,

    /// Null when out of memory: a side effect returns failures as values.
    fn name(c: UnlockChannel) ?[]const u8 {
        return std.fmt.allocPrint(c.allocator, "unlock-event-{s}", .{c.sender_workflow_id}) catch null;
    }
};

fn mutexWorkflow(ctx: *workflow.Context, namespace: []const u8, resource_id: []const u8, unlock_timeout: Duration) !void {
    _ = namespace;
    _ = resource_id;
    const logger = ctx.logger();
    logger.info("started", .{ .currentWorkflowID = ctx.info().workflow_id });

    const state = try ctx.allocator().create(MutexState);
    state.* = .{ .ctx = ctx };
    try ctx.setSignalHandler(RequestLock, state, MutexState.requestLock);
    // Requests that arrived before the handler (the one signal-with-start carried, at least)
    // start their handlers at registration; let them run before checking for requests.
    try ctx.await(ctx, workflow.Context.allHandlersFinished);

    while (state.next_request < state.requests.items.len) : (state.next_request += 1) {
        const sender_workflow_id = state.requests.items[state.next_request];
        const release_lock_channel_name = try ctx.sideEffect(?[]const u8, UnlockChannel{
            .allocator = ctx.allocator(),
            .sender_workflow_id = sender_workflow_id,
        }, UnlockChannel.name) orelse return error.OutOfMemory;
        logger.info("generated release lock channel name", .{ .releaseLockChannelName = release_lock_channel_name });

        state.ack = null;
        try ctx.setSignalHandlerByName(release_lock_channel_name, struct { []const u8 }, state, MutexState.releaseLock);
        // Send the release signal's name to the sender, so that it can release the lock.
        ctx.signalExternalWorkflow(AcquireLock, sender_workflow_id, "", .{release_lock_channel_name}).get() catch |err| switch (err) {
            // The sender has closed. Skip it rather than fail: failing would leave every
            // later sender waiting for a lock no one grants.
            error.ExternalWorkflowNotFound, error.ExternalWorkflowRequestFailed => {
                logger.info("SignalExternalWorkflow error", .{ .err = err });
                continue;
            },
            else => |e| return e,
        };
        logger.info("signaled external workflow", .{});

        if (try ctx.awaitTimeout(unlock_timeout, state, MutexState.released)) {
            logger.info("release signal received", .{});
        } else {
            logger.info("unlockTimeout exceeded", .{});
        }
    }
    logger.info("no more signals", .{});
}

pub const MutexWorkflow = workflow.Def("MutexWorkflow", mutexWorkflow);

/// The activity is a method so that it can use the worker's client, bound to it at
/// registration: the client must outlive the worker.
pub const Activities = struct {
    client: *temporal.client.Client,

    /// Signals the mutex workflow of `resource_id` in `namespace` with a request for the lock
    /// on behalf of `sender_workflow_id`, starting the mutex workflow if it isn't running.
    pub fn signalWithStartMutexWorkflow(
        a: *Activities,
        ctx: *activity.Context,
        namespace: []const u8,
        resource_id: []const u8,
        sender_workflow_id: []const u8,
        unlock_timeout: Duration,
    ) !Execution {
        const logger = ctx.logger();
        const workflow_id = try std.fmt.allocPrint(ctx.allocator(), "mutex:{s}:{s}", .{ namespace, resource_id });
        var run = a.client.signalWithStartWorkflow(MutexWorkflow, .{
            .id = workflow_id,
            .task_queue = task_queue,
            .retry_policy = retry_policy,
        }, .{ namespace, resource_id, unlock_timeout }, RequestLock, .{sender_workflow_id}) catch |err| {
            logger.err("Unable to signal with start workflow", .{ .err = err });
            return err;
        };
        defer run.deinit();
        logger.info("Signaled and started Workflow", .{ .WorkflowID = run.id, .RunID = run.run_id });
        return .{
            .workflow_id = try ctx.allocator().dupe(u8, run.id),
            .run_id = try ctx.allocator().dupe(u8, run.run_id),
        };
    }
};

pub const SignalWithStartMutexWorkflowActivity = activity.Def("SignalWithStartMutexWorkflowActivity", Activities.signalWithStartMutexWorkflow);

/// How long the sample workflow holds the lock.
pub const critical_section: Duration = .fromSeconds(10);

fn sampleWorkflowWithMutex(ctx: *workflow.Context, resource_id: []const u8) !void {
    const logger = ctx.logger();
    const current_workflow_id = ctx.info().workflow_id;
    logger.info("started", .{ .currentWorkflowID = current_workflow_id, .resourceID = resource_id });

    const mutex: Mutex = .{ .current_workflow_id = current_workflow_id, .lock_namespace = "TestUseCase" };
    const unlock = try mutex.lock(ctx, resource_id, .fromSeconds(10 * 60));
    logger.info("resource locked", .{});

    // Emulate a long running process.
    logger.info("critical operation started", .{});
    try ctx.io().sleep(critical_section, .awake);
    logger.info("critical operation finished", .{});

    // The mutex workflow takes the lock back after the unlock timeout anyway.
    unlock.release(ctx) catch |err| logger.warn("releasing the lock failed", .{ .err = err });

    logger.info("finished", .{});
}

pub const SampleWorkflowWithMutex = workflow.Def("SampleWorkflowWithMutex", sampleWorkflowWithMutex);
