//! Requests uppercasing from a running uppercase workflow: sends each request by signal and
//! waits for the workflow to call back with the response. The callback is the "responseActivity"
//! activity, run by a worker the requester starts on a task queue of its own.

const std = @import("std");
const temporal = @import("temporal");
const uppercase = @import("workflow.zig");

const Duration = std.Io.Duration;

pub const Requester = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    client: *temporal.client.Client,
    options: Options,
    /// "requester-<uuid>", the task queue the response activity runs on.
    task_queue: []const u8,
    worker: temporal.worker.Worker,
    running: std.Io.Future(temporal.worker.Worker.RunError!void),
    /// Guards `pending`: requests add and remove entries while response activities, on the
    /// worker's tasks, fill them in.
    mutex: std.Io.Mutex = .init,
    pending: std.StringHashMapUnmanaged(*Pending) = .empty,

    pub const Options = struct {
        /// The ID of the workflow taking requests.
        target_workflow_id: []const u8,
        /// How long to wait for the response.
        response_timeout: Duration = .fromSeconds(10),
    };

    pub const Error = error{
        OutOfMemory,
        Canceled,
        /// Sending the request signal failed; the client logs why.
        SignalFailed,
        /// The workflow answered with a failed response; its message is logged.
        RequestFailed,
        /// No response within `response_timeout`.
        Timeout,
    };

    /// A request waiting for its response.
    const Pending = struct {
        done: std.Io.Event = .unset,
        /// Set once, by the first response activity for the request. Its strings are owned
        /// by the requester's `gpa`.
        response: ?uppercase.Response = null,
    };

    /// The response activity's name, which requests pass to the workflow.
    pub const response_activity_name = "responseActivity";
    pub const ResponseActivity = temporal.activity.Def(response_activity_name, respond);

    /// Starts the requester's worker in the background of `io`. `client` must outlive the
    /// requester, as must the strings in `options`, and `gpa` must be safe to use from several
    /// threads: response activities run on the worker's tasks. Call `deinit` to stop the worker.
    pub fn init(gpa: std.mem.Allocator, io: std.Io, client: *temporal.client.Client, options: Options) !*Requester {
        if (options.target_workflow_id.len == 0) return error.MissingTargetWorkflowId;

        const r = try gpa.create(Requester);
        errdefer gpa.destroy(r);
        const id = try temporal.client.uuid(gpa, io);
        defer gpa.free(id);
        const task_queue = try std.fmt.allocPrint(gpa, "requester-{s}", .{id});
        errdefer gpa.free(task_queue);
        r.* = .{
            .gpa = gpa,
            .io = io,
            .client = client,
            .options = options,
            .task_queue = task_queue,
            .worker = try .init(gpa, io, client, task_queue, .{}),
            .running = undefined,
        };
        errdefer r.worker.deinit();
        try r.worker.registerActivityWith(ResponseActivity, r);
        r.running = io.concurrent(runWorker, .{&r.worker}) catch |err| {
            std.log.err("Starting the requester's worker on task queue {s} failed: {t}", .{ task_queue, err });
            return err;
        };
        return r;
    }

    /// Stops the worker, after any response activity it is running, and frees the requester.
    /// Requests must not be in progress.
    pub fn deinit(r: *Requester) void {
        r.worker.stop();
        r.running.await(r.io) catch |err| std.log.err("The requester's worker on task queue {s} failed: {t}", .{ r.task_queue, err });
        r.worker.deinit();
        std.debug.assert(r.pending.count() == 0);
        r.pending.deinit(r.gpa);
        r.gpa.free(r.task_queue);
        r.gpa.destroy(r);
    }

    fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
        return w.run();
    }

    /// Asks the workflow to uppercase `input` and returns the result, allocated from `arena`.
    pub fn requestUppercase(r: *Requester, arena: std.mem.Allocator, input: []const u8) Error![]const u8 {
        const request: uppercase.Request = .{
            .id = try temporal.client.uuid(arena, r.io),
            .input = input,
            .response_activity = response_activity_name,
            .response_task_queue = r.task_queue,
        };

        // Registered before signaling, so a response that comes back at once finds it.
        var pending: Pending = .{};
        {
            try r.mutex.lock(r.io);
            defer r.mutex.unlock(r.io);
            try r.pending.put(r.gpa, request.id, &pending);
        }
        const waited = waited: {
            r.client.signalWorkflow(uppercase.RequestSignal, r.options.target_workflow_id, "", .{request}) catch |err| {
                std.log.err("Signaling request {s} to workflow {s} failed: {t}", .{ request.id, r.options.target_workflow_id, err });
                break :waited switch (err) {
                    error.OutOfMemory, error.Canceled => |e| e,
                    else => error.SignalFailed,
                };
            };
            break :waited pending.done.waitTimeout(r.io, .{ .duration = .{ .raw = r.options.response_timeout, .clock = .awake } });
        };
        {
            r.mutex.lockUncancelable(r.io);
            defer r.mutex.unlock(r.io);
            _ = r.pending.remove(request.id);
        }
        // No response activity touches `pending` any more. A response may have arrived just
        // as the wait ended, so it is checked rather than the wait's result.
        const response = pending.response orelse {
            // `done` is set only after `response`.
            waited catch |err| {
                if (err == error.Timeout) std.log.err("No response to request {s} within {f}", .{ request.id, r.options.response_timeout });
                return err;
            };
            unreachable;
        };
        defer {
            r.gpa.free(response.id);
            r.gpa.free(response.output);
            r.gpa.free(response.error_message);
        }
        if (response.error_message.len > 0) {
            std.log.err("Request {s} failed: {s}", .{ request.id, response.error_message });
            return error.RequestFailed;
        }
        return arena.dupe(u8, response.output);
    }

    /// Hands a response to the request waiting for it. A response nobody waits for any more
    /// (the request timed out) or that came already (the activity was retried) is dropped.
    fn respond(r: *Requester, ctx: *temporal.activity.Context, response: uppercase.Response) !void {
        _ = ctx;
        try r.mutex.lock(r.io);
        defer r.mutex.unlock(r.io);
        const pending = r.pending.get(response.id) orelse return;
        if (pending.response != null) return;

        const id = try r.gpa.dupe(u8, response.id);
        errdefer r.gpa.free(id);
        const output = try r.gpa.dupe(u8, response.output);
        errdefer r.gpa.free(output);
        const error_message = try r.gpa.dupe(u8, response.error_message);
        pending.response = .{ .id = id, .output = output, .error_message = error_message };
        pending.done.set(r.io);
    }
};
