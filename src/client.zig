//! The Temporal client: connect to a server, start workflows,
//! and wait for their results.
//!
//!     var client = try temporal.client.Client.connect(gpa, io, try temporal.client.loadDefaultOptions(arena));
//!     defer client.close();
//!     var run = try client.executeWorkflow(HelloWorld, .{ .id = "hello", .task_queue = "hello-world" }, .{"Temporal"});
//!     defer run.deinit();
//!     const greeting = try run.get(arena);
//!
//! Errors follow the SDK's pattern (see `temporal.failure`): `run.get` returns
//! `error.WorkflowFailed` and the failure is read with `run.failure()`. RPC failures are
//! logged with the server's message under the `temporal` log scope. Calls that send or
//! receive payloads return `error.CodecFailed`, also logged, when a payload codec
//! (`Options.codecs`) fails.

const std = @import("std");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const search_attributes = @import("search_attributes.zig");
const failure_mod = @import("failure.zig");
const history_mod = @import("history.zig");
const schedule = @import("schedule.zig");
const workflow = @import("workflow.zig");
const log_mod = @import("log.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Duration = Io.Duration;

const wfs = proto.temporal.api.workflowservice.v1;
const enums = proto.temporal.api.enums.v1;
const log = std.log.scoped(.temporal);

pub const Options = struct {
    /// host:port of the server's frontend.
    host_port: []const u8 = "localhost:7233",
    namespace: []const u8 = "default",
    /// Empty uses "<pid>@<hostname>".
    identity: []const u8 = "",
    api_key: []const u8 = "",
    tls: ?Tls = null,
    /// gRPC headers sent with every call.
    headers: []const bridge.bytes.MetadataEntry = &.{},
    /// Payload codecs, such as `converter.ZlibCodec` or encryption, applied in order to every
    /// payload this client sends and in reverse to every payload it receives (results,
    /// failures, memos, history events), and by every worker on this client to its workflow
    /// and activity payloads. Search attributes are never encoded. Borrowed: the slice and
    /// the codecs must outlive the client and its workers.
    codecs: []const converter.PayloadCodec = &.{},
    /// Where sdk-core's logs go, and the default for every worker on this client (see
    /// `worker.Options.logger`). Null uses `log.Sink.std_log`. Borrowed: it must outlive the
    /// client and its workers. Called concurrently from worker threads and sdk-core's threads.
    logger: ?log_mod.Sink = null,
    /// The least severe sdk-core log level forwarded to `logger`; null leaves sdk-core's logs
    /// off. Logs from libraries sdk-core uses are forwarded from `warn` up.
    core_log_level: ?log_mod.Level = .warn,

    pub const Tls = struct {
        /// PEM; empty uses the system roots.
        server_root_ca_cert: []const u8 = "",
        server_name: []const u8 = "",
        client_cert: []const u8 = "",
        client_private_key: []const u8 = "",
    };
};

/// sdk-core's crates log at `level`; the libraries they use at `level` or `warn`, whichever is
/// less verbose.
fn coreLogFilter(buf: []u8, level: log_mod.Level) []const u8 {
    const core = switch (level) {
        .err => "ERROR",
        .warn => "WARN",
        .info => "INFO",
        .debug => "DEBUG",
    };
    const other = if (@backingInt(level) < @backingInt(log_mod.Level.warn)) core else "WARN";
    return std.fmt.bufPrint(buf, "{s},temporalio_sdk_core={s},temporalio_client={s},temporalio_common={s},temporalio_sdk_core_c_bridge={s}", .{ other, core, core, core, core }) catch unreachable;
}

/// Passes a core log record to the client's sink: the message, a `target` field naming the
/// Rust module it came from, and the record's own fields. Runs on the core's threads.
fn forwardCoreLog(ctx: *anyopaque, record: *const bridge.Runtime.LogRecord) void {
    const sink: *const log_mod.Sink = @ptrCast(@alignCast(ctx));
    var fields: [1 + log_mod.max_context_fields]log_mod.Field = undefined;
    fields[0] = .{ .key = "target", .value = record.target };
    var buf: [8 * 1024]u8 = undefined;
    var fba: std.heap.FixedBufferAllocator = .init(&buf);
    const n = coreLogFields(fba.allocator(), record.fieldsJson(), fields[1..]);
    sink.log(switch (record.level) {
        .trace, .debug => .debug,
        .info => .info,
        .warn => .warn,
        .err => .err,
    }, record.message, fields[0 .. 1 + n]);
}

/// Splits a core log record's JSON fields into `out`: strings as they are, other values as
/// JSON. A record whose fields don't parse within `a` gets one `fields` field with the JSON.
fn coreLogFields(a: Allocator, json: []const u8, out: []log_mod.Field) usize {
    if (out.len == 0 or json.len == 0 or std.mem.eql(u8, json, "{}")) return 0;
    const value = std.json.parseFromSliceLeaky(std.json.Value, a, json, .{}) catch null;
    if (value == null or value.? != .object) {
        out[0] = .{ .key = "fields", .value = json };
        return 1;
    }
    var n: usize = 0;
    var it = value.?.object.iterator();
    while (it.next()) |kv| {
        if (n == out.len) break;
        out[n] = .{ .key = kv.key_ptr.*, .value = switch (kv.value_ptr.*) {
            .string, .number_string => |s| s,
            else => |v| std.json.Stringify.valueAlloc(a, v, .{}) catch continue,
        } };
        n += 1;
    }
    return n;
}

pub const ConnectError = error{ OutOfMemory, InvalidOptions, RuntimeInitFailed, ConnectFailed };

pub const Client = struct {
    gpa: Allocator,
    io: Io,
    runtime: bridge.Runtime,
    connection: bridge.Connection,
    namespace: []u8,
    identity: []u8,
    /// `Options.codecs`, borrowed.
    codecs: []const converter.PayloadCodec,
    /// `Options.logger` or the default sink; workers on this client inherit it.
    logger: log_mod.Sink,
    /// A copy of `logger` that the runtime's core log forwarding points at, freed after the
    /// runtime.
    core_log_sink: ?*log_mod.Sink,

    /// Connects and checks the server. Close with `close`.
    pub fn connect(gpa: Allocator, io: Io, options: Options) ConnectError!Client {
        var message: ?[]u8 = null;
        defer if (message) |m| gpa.free(m);

        const logger = options.logger orelse log_mod.Sink.std_log;
        var core_log_sink: ?*log_mod.Sink = null;
        errdefer if (core_log_sink) |s| gpa.destroy(s);
        var filter_buf: [256]u8 = undefined;
        var logging: ?bridge.Runtime.Logging = null;
        if (options.core_log_level) |level| {
            core_log_sink = try gpa.create(log_mod.Sink);
            core_log_sink.?.* = logger;
            logging = .{
                .filter = coreLogFilter(&filter_buf, level),
                .forward = .{ .ctx = core_log_sink.?, .func = forwardCoreLog },
            };
        }
        const runtime = bridge.Runtime.init(gpa, .{ .logging = logging }, &message) catch |err| switch (err) {
            error.TooManyLogForwarders => {
                log.err("creating the core runtime failed: {d} clients in this process already forward sdk-core's logs; close one, or set Options.core_log_level to null", .{bridge.Runtime.max_forwarders});
                return error.RuntimeInitFailed;
            },
            error.OutOfMemory, error.InvalidOptions => |e| return e,
            error.RuntimeInitFailed => {
                if (message) |m| log.err("creating the core runtime failed: {s}", .{m});
                return error.RuntimeInitFailed;
            },
        };
        errdefer runtime.deinit();

        const identity = if (options.identity.len > 0) try gpa.dupe(u8, options.identity) else try defaultIdentity(gpa);
        errdefer gpa.free(identity);
        const namespace = try gpa.dupe(u8, options.namespace);
        errdefer gpa.free(namespace);

        const scheme = if (options.tls != null) "https" else "http";
        const url = try std.fmt.allocPrint(gpa, "{s}://{s}", .{ scheme, options.host_port });
        defer gpa.free(url);
        const connection = bridge.Connection.connect(gpa, io, runtime, .{
            .target_url = url,
            .identity = identity,
            .api_key = options.api_key,
            .metadata = options.headers,
            .tls = if (options.tls) |t| .{
                .server_root_ca_cert = t.server_root_ca_cert,
                .domain = t.server_name,
                .client_cert = t.client_cert,
                .client_private_key = t.client_private_key,
            } else null,
        }, &message) catch |err| {
            log.err("connecting to {s} failed: {s}", .{ options.host_port, message orelse @errorName(err) });
            return err;
        };
        return .{
            .gpa = gpa,
            .io = io,
            .runtime = runtime,
            .connection = connection,
            .namespace = namespace,
            .identity = identity,
            .codecs = options.codecs,
            .logger = logger,
            .core_log_sink = core_log_sink,
        };
    }

    pub fn close(c: *Client) void {
        c.connection.deinit();
        c.runtime.deinit();
        if (c.core_log_sink) |s| c.gpa.destroy(s);
        c.gpa.free(c.namespace);
        c.gpa.free(c.identity);
        c.* = undefined;
    }

    /// Starts a workflow defined with `temporal.workflow.Def` and returns a handle to its run.
    /// `args` is a tuple of the workflow's parameters after its context.
    pub fn executeWorkflow(c: *Client, comptime Workflow: type, options: StartWorkflowOptions, args: Workflow.Args) StartError!WorkflowRun(Workflow.Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const id = if (options.id.len > 0) options.id else try uuid(arena_state.allocator(), c.io);
        const started = try c.startWorkflow(Workflow.workflow_name, id, options, args);
        errdefer c.gpa.free(started.run_id);
        return .{ .client = c, .id = try c.gpa.dupe(u8, id), .run_id = started.run_id, .eagerly_started = started.eager };
    }

    /// Starts a workflow by its type name, for a workflow this program has no definition of.
    /// `args` is a tuple of its arguments and `Result` the type its result decodes to (`void`
    /// to ignore it).
    pub fn executeWorkflowByName(c: *Client, comptime Result: type, workflow_type: []const u8, options: StartWorkflowOptions, args: anytype) StartError!WorkflowRun(Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const id = if (options.id.len > 0) options.id else try uuid(arena_state.allocator(), c.io);
        const started = try c.startWorkflow(workflow_type, id, options, args);
        errdefer c.gpa.free(started.run_id);
        return .{ .client = c, .id = try c.gpa.dupe(u8, id), .run_id = started.run_id, .eagerly_started = started.eager };
    }

    /// The run ID is owned with the client's allocator.
    fn startWorkflow(c: *Client, workflow_type: []const u8, id: []const u8, options: StartWorkflowOptions, args: anytype) StartError!struct { run_id: []u8, eager: bool } {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var request = try c.startRequest(a, workflow_type, id, options, args);
        // Set here rather than in `startRequest`: the core reserves a workflow task slot only
        // for a plain start, so signal-with-start and update-with-start never ask for it.
        request.request_eager_execution = options.enable_eager_start;
        const response = c.rpc(wfs.StartWorkflowExecutionResponse, "StartWorkflowExecution", request, a) catch |err| switch (err) {
            error.AlreadyExists => return error.WorkflowExecutionAlreadyStarted,
            else => |e| return e,
        };
        // The core has already handed the eager task to the worker; the response keeps a copy.
        return .{ .run_id = try c.gpa.dupe(u8, response.run_id), .eager = response.eager_workflow_task != null };
    }

    /// The start request for `options`, allocated from `a`. Signal-with-start copies it.
    fn startRequest(c: *Client, a: Allocator, workflow_type: []const u8, id: []const u8, options: StartWorkflowOptions, args: anytype) search_attributes.EncodeError!wfs.StartWorkflowExecutionRequest {
        var request: wfs.StartWorkflowExecutionRequest = .{
            .namespace = c.namespace,
            .workflow_id = id,
            .workflow_type = .{ .name = workflow_type },
            .task_queue = .{ .name = options.task_queue, .kind = .TASK_QUEUE_KIND_NORMAL },
            .input = .{ .payloads = try converter.toPayloads(a, args) },
            .workflow_execution_timeout = protoDuration(options.execution_timeout),
            .workflow_run_timeout = protoDuration(options.run_timeout),
            .workflow_task_timeout = protoDuration(options.task_timeout),
            .workflow_start_delay = protoDuration(options.start_delay),
            .cron_schedule = options.cron_schedule,
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .workflow_id_reuse_policy = switch (options.id_reuse_policy) {
                .unspecified => .WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
                .allow_duplicate => .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
                .allow_duplicate_failed_only => .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
                .reject_duplicate => .WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
            },
            .workflow_id_conflict_policy = switch (options.id_conflict_policy) {
                .unspecified => .WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
                .fail => .WORKFLOW_ID_CONFLICT_POLICY_FAIL,
                .use_existing => .WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
                .terminate_existing => .WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
            },
        };
        if (options.memo.len > 0) {
            var memo: proto.temporal.api.common.v1.Memo = .{};
            try memo.fields.ensureTotalCapacity(a, options.memo.len);
            for (options.memo) |field| memo.fields.appendAssumeCapacity(.{ .key = field.name, .value = field.value });
            request.memo = memo;
        }
        if (options.search_attributes.len > 0) request.search_attributes = try search_attributes.toProto(a, options.search_attributes, .start);
        return request;
    }

    /// Sends `Signal` (a `workflow.SignalDef`) with `args` to a workflow. An empty `run_id`
    /// signals the latest run. Returns `error.NotFound` if no such workflow exists or the
    /// run has closed. The call returns once the server has recorded the signal, not once a
    /// handler has run it.
    pub fn signalWorkflow(c: *Client, comptime Signal: type, workflow_id: []const u8, run_id: []const u8, args: Signal.Args) SignalError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = c.rpc(wfs.SignalWorkflowExecutionResponse, "SignalWorkflowExecution", wfs.SignalWorkflowExecutionRequest{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .signal_name = Signal.signal_name,
            .input = .{ .payloads = try converter.toPayloads(a, args) },
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Sends `Signal` with `signal_args` to the workflow `options.id`, first starting it
    /// with `args` if it isn't running, in one atomic call. A new run's first workflow task
    /// carries the signal, which waits until the workflow registers its handler. If the
    /// workflow is already running, it is only signaled: `args` and the other start options
    /// are ignored and the returned run is the running one. An empty `options.id` generates
    /// a UUID, so the call always starts a new workflow.
    pub fn signalWithStartWorkflow(
        c: *Client,
        comptime Workflow: type,
        options: StartWorkflowOptions,
        args: Workflow.Args,
        comptime Signal: type,
        signal_args: Signal.Args,
    ) StartError!WorkflowRun(Workflow.Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const id = if (options.id.len > 0) options.id else try uuid(a, c.io);

        const start = try c.startRequest(a, Workflow.workflow_name, id, options, args);
        var request: wfs.SignalWithStartWorkflowExecutionRequest = .{
            .signal_name = Signal.signal_name,
            .signal_input = .{ .payloads = try converter.toPayloads(a, signal_args) },
        };
        // Every start field the two requests share, so a start option mapped in
        // `startRequest` reaches signal-with-start too.
        const Start = wfs.StartWorkflowExecutionRequest;
        const info = @typeInfo(wfs.SignalWithStartWorkflowExecutionRequest).@"struct";
        inline for (info.field_names, info.field_types) |name, F| {
            if (comptime @hasField(Start, name) and @FieldType(Start, name) == F and !std.mem.eql(u8, name, "_unknown_fields"))
                @field(request, name) = @field(start, name);
        }

        const response = c.rpc(wfs.SignalWithStartWorkflowExecutionResponse, "SignalWithStartWorkflowExecution", request, a) catch |err| switch (err) {
            error.AlreadyExists => return error.WorkflowExecutionAlreadyStarted,
            else => |e| return e,
        };
        const run_id = try c.gpa.dupe(u8, response.run_id);
        errdefer c.gpa.free(run_id);
        return .{ .client = c, .id = try c.gpa.dupe(u8, id), .run_id = run_id };
    }

    /// Terminates a workflow execution: it stops at once, with no chance to clean up. An empty
    /// `run_id` targets the latest run.
    pub fn terminateWorkflow(c: *Client, workflow_id: []const u8, run_id: []const u8, reason: []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = c.rpc(wfs.TerminateWorkflowExecutionResponse, "TerminateWorkflowExecution", wfs.TerminateWorkflowExecutionRequest{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .reason = reason,
            .identity = c.identity,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Requests cancellation of a workflow execution. The workflow sees it as `error.Canceled`
    /// from its next cancelable wait and may clean up before it returns; the run ends
    /// canceled when the workflow returns `error.Canceled`. An empty `run_id` targets the
    /// latest run. Returns `error.NotFound` if no such workflow exists.
    pub fn cancelWorkflow(c: *Client, workflow_id: []const u8, run_id: []const u8, reason: []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = c.rpc(wfs.RequestCancelWorkflowExecutionResponse, "RequestCancelWorkflowExecution", wfs.RequestCancelWorkflowExecutionRequest{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .reason = reason,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Completes an activity that returned `ctx.completeAsync()`, identified by the task token
    /// it handed off, with `result` (a value of the activity's result type, or `{}` for
    /// `void`). Returns `error.NotFound` when the activity is no longer waiting: it already
    /// completed, failed, or timed out, or its workflow closed. Safe to call from any process
    /// that can reach the server, not only the worker's.
    pub fn completeActivity(c: *Client, task_token: []const u8, result: anytype) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskCompletedResponse, "RespondActivityTaskCompleted", wfs.RespondActivityTaskCompletedRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .result = try resultPayloads(a, result),
            .identity = c.identity,
        }, a);
    }

    /// `completeActivity` for the activity `id`.
    pub fn completeActivityById(c: *Client, id: ActivityId, result: anytype) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskCompletedByIdResponse, "RespondActivityTaskCompletedById", wfs.RespondActivityTaskCompletedByIdRequest{
            .namespace = c.namespace,
            .workflow_id = id.workflow_id,
            .run_id = id.run_id,
            .activity_id = id.activity_id,
            .result = try resultPayloads(a, result),
            .identity = c.identity,
        }, a);
    }

    /// Fails an activity that returned `ctx.completeAsync()` with the failure `f`. The
    /// activity's retry policy applies, as for a failure returned by the activity itself.
    /// Returns `error.NotFound` when the activity is no longer waiting.
    pub fn failActivity(c: *Client, task_token: []const u8, f: failure_mod.Application) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskFailedResponse, "RespondActivityTaskFailed", wfs.RespondActivityTaskFailedRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .failure = try failure_mod.application(a, f),
            .identity = c.identity,
        }, a);
    }

    /// `failActivity` for the activity `id`.
    pub fn failActivityById(c: *Client, id: ActivityId, f: failure_mod.Application) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskFailedByIdResponse, "RespondActivityTaskFailedById", wfs.RespondActivityTaskFailedByIdRequest{
            .namespace = c.namespace,
            .workflow_id = id.workflow_id,
            .run_id = id.run_id,
            .activity_id = id.activity_id,
            .failure = try failure_mod.application(a, f),
            .identity = c.identity,
        }, a);
    }

    /// Reports that an activity that returned `ctx.completeAsync()` was canceled, with
    /// `details` (any value the converter encodes, or `{}` for none). Call it after
    /// `recordActivityHeartbeat` returns true. Returns `error.NotFound` when the activity is
    /// no longer waiting.
    pub fn reportActivityCancellation(c: *Client, task_token: []const u8, details: anytype) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskCanceledResponse, "RespondActivityTaskCanceled", wfs.RespondActivityTaskCanceledRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .details = try detailPayloads(a, details),
            .identity = c.identity,
        }, a);
    }

    /// `reportActivityCancellation` for the activity `id`.
    pub fn reportActivityCancellationById(c: *Client, id: ActivityId, details: anytype) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskCanceledByIdResponse, "RespondActivityTaskCanceledById", wfs.RespondActivityTaskCanceledByIdRequest{
            .namespace = c.namespace,
            .workflow_id = id.workflow_id,
            .run_id = id.run_id,
            .activity_id = id.activity_id,
            .details = try detailPayloads(a, details),
            .identity = c.identity,
        }, a);
    }

    /// Heartbeats an activity that returned `ctx.completeAsync()`, with progress `details`
    /// (any value the converter encodes, or `{}` for none), which a retry reads with
    /// `ctx.heartbeatDetails`. Returns true when cancellation of the activity has been
    /// requested: finish it with `reportActivityCancellation` (or complete or fail it). Not
    /// throttled: each call is a request to the server. Returns `error.NotFound` when the
    /// activity is no longer waiting.
    pub fn recordActivityHeartbeat(c: *Client, task_token: []const u8, details: anytype) ActivityCompletionError!bool {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const response = try c.activityRpc(wfs.RecordActivityTaskHeartbeatResponse, "RecordActivityTaskHeartbeat", wfs.RecordActivityTaskHeartbeatRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .details = try detailPayloads(a, details),
            .identity = c.identity,
        }, a);
        return response.cancel_requested;
    }

    /// `recordActivityHeartbeat` for the activity `id`.
    pub fn recordActivityHeartbeatById(c: *Client, id: ActivityId, details: anytype) ActivityCompletionError!bool {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const response = try c.activityRpc(wfs.RecordActivityTaskHeartbeatByIdResponse, "RecordActivityTaskHeartbeatById", wfs.RecordActivityTaskHeartbeatByIdRequest{
            .namespace = c.namespace,
            .workflow_id = id.workflow_id,
            .run_id = id.run_id,
            .activity_id = id.activity_id,
            .details = try detailPayloads(a, details),
            .identity = c.identity,
        }, a);
        return response.cancel_requested;
    }

    fn activityRpc(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator) ActivityCompletionError!Response {
        return c.rpc(Response, method, request, arena) catch |err| switch (err) {
            error.AlreadyExists => error.InvalidResponse,
            else => |e| e,
        };
    }

    fn resultPayloads(a: Allocator, result: anytype) error{OutOfMemory}!proto.temporal.api.common.v1.Payloads {
        var payloads: proto.temporal.api.common.v1.Payloads = .{};
        try payloads.payloads.append(a, try converter.toPayload(a, result));
        return payloads;
    }

    fn detailPayloads(a: Allocator, details: anytype) error{OutOfMemory}!?proto.temporal.api.common.v1.Payloads {
        if (@TypeOf(details) == void) return null;
        return try resultPayloads(a, details);
    }

    /// Returns the full event history of a workflow run (all pages). An empty `run_id` reads
    /// the latest run. Memory comes from `arena`.
    pub fn fetchHistory(c: *Client, arena: Allocator, workflow_id: []const u8, run_id: []const u8) HistoryError!History {
        var it = try c.historyIterator(workflow_id, run_id, .{});
        defer it.deinit();
        var history: History = .{};
        while (try it.next(arena)) |event| try history.events.append(arena, event);
        return history;
    }

    /// Iterates over the events of a workflow run's history, oldest first. An empty `run_id`
    /// reads the latest run. The iterator fetches a page at a time as `next` reaches it;
    /// nothing is fetched here, so a missing workflow shows up as `error.NotFound` from
    /// `next`. Free it with `deinit`.
    pub fn historyIterator(c: *Client, workflow_id: []const u8, run_id: []const u8, options: HistoryIteratorOptions) error{OutOfMemory}!HistoryIterator {
        const id = try c.gpa.dupe(u8, workflow_id);
        errdefer c.gpa.free(id);
        return .{ .client = c, .workflow_id = id, .run_id = try c.gpa.dupe(u8, run_id), .options = options, .page = .init(c.gpa) };
    }

    /// Describes a workflow run: its status, type, times, memo and search attributes. An
    /// empty `run_id` describes the latest run. Returns `error.NotFound` if no such workflow
    /// exists. The description and all its memory come from `arena`.
    pub fn describeWorkflow(c: *Client, arena: Allocator, workflow_id: []const u8, run_id: []const u8) DescribeError!WorkflowDescription {
        const response = c.rpc(wfs.DescribeWorkflowExecutionResponse, "DescribeWorkflowExecution", wfs.DescribeWorkflowExecutionRequest{
            .namespace = c.namespace,
            .execution = .{ .workflow_id = workflow_id, .run_id = run_id },
        }, arena) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
        const info = response.workflow_execution_info orelse {
            log.err("DescribeWorkflowExecution for workflow {s} returned no execution info", .{workflow_id});
            return error.InvalidResponse;
        };
        const lifted = try executionInfo(arena, info);
        var d: WorkflowDescription = undefined;
        inline for (@typeInfo(WorkflowDescription).@"struct".field_names) |name| {
            if (comptime std.mem.eql(u8, name, "raw")) continue;
            @field(d, name) = @field(lifted, name);
        }
        d.raw = response;
        return d;
    }

    /// Lists the workflow executions matching the visibility `query`, such as
    /// `WorkflowType = 'Greet' AND ExecutionStatus = 'Running'` or a condition on a search
    /// attribute (`CustomerId = 'c-42'`); an empty query lists them all. Visibility is
    /// eventually consistent: a workflow started or changed a moment ago may not match yet.
    /// The iterator fetches a page at a time as `next` reaches it; nothing is fetched here.
    /// Free it with `deinit`.
    pub fn listWorkflows(c: *Client, query: []const u8, options: ListWorkflowsOptions) error{OutOfMemory}!WorkflowExecutionIterator {
        return .{ .client = c, .query = try c.gpa.dupe(u8, query), .page_size = options.page_size, .page = .init(c.gpa) };
    }

    /// The number of workflow executions matching the visibility `query` (see
    /// `listWorkflows`); an empty query counts them all. Eventually consistent, like
    /// `listWorkflows`.
    pub fn countWorkflows(c: *Client, query: []const u8) ListError!u64 {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var status: ?RpcStatus = null;
        const response = c.rpcWithStatus(wfs.CountWorkflowExecutionsResponse, "CountWorkflowExecutions", wfs.CountWorkflowExecutionsRequest{
            .namespace = c.namespace,
            .query = query,
        }, a, &status) catch |err| return visibilityError(err, status, "counting workflows", query);
        return std.math.cast(u64, response.count) orelse {
            log.err("CountWorkflowExecutions for query \"{s}\" returned the count {d}", .{ query, response.count });
            return error.InvalidResponse;
        };
    }

    fn visibilityError(err: RpcError, status: ?RpcStatus, comptime what: []const u8, query: []const u8) ListError {
        switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                log.err(what ++ " with the query \"{s}\" failed: {s} (gRPC status {d})", .{ query, s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        }
    }

    /// Starts `Activity` (a `temporal.activity.Def`) on its own, without a workflow, and
    /// returns a handle to it: a worker polling `options.task_queue` runs it, and the server
    /// applies its timeouts and retry policy. `args` is a tuple of the activity's parameters
    /// after its context. The server must have standalone activities enabled (see
    /// `testing.DevServer.standalone_activity_args`); otherwise, and for options it rejects
    /// (it requires `start_to_close_timeout` or `schedule_to_close_timeout`), the call fails
    /// with `error.RpcFailed`, logging the server's reason.
    pub fn executeActivity(c: *Client, comptime Activity: type, options: StartActivityOptions, args: Activity.Args) StartActivityError!ActivityHandle(Activity.Result) {
        return c.executeActivityByName(Activity.Result, Activity.activity_name, options, args);
    }

    /// `executeActivity` for an activity this program has no definition of, by its type
    /// name. `args` is a tuple of its arguments and `Result` the type its result decodes to
    /// (`void` to ignore it).
    pub fn executeActivityByName(c: *Client, comptime Result: type, activity_type: []const u8, options: StartActivityOptions, args: anytype) StartActivityError!ActivityHandle(Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const id = if (options.id.len > 0) options.id else try uuid(a, c.io);
        var request: wfs.StartActivityExecutionRequest = .{
            .namespace = c.namespace,
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .activity_id = id,
            .activity_type = .{ .name = activity_type },
            .task_queue = .{ .name = options.task_queue, .kind = .TASK_QUEUE_KIND_NORMAL },
            .schedule_to_close_timeout = protoDuration(options.schedule_to_close_timeout),
            .schedule_to_start_timeout = protoDuration(options.schedule_to_start_timeout),
            .start_to_close_timeout = protoDuration(options.start_to_close_timeout),
            .heartbeat_timeout = protoDuration(options.heartbeat_timeout),
            .input = .{ .payloads = try converter.toPayloads(a, args) },
            .id_reuse_policy = switch (options.id_reuse_policy) {
                .unspecified => .ACTIVITY_ID_REUSE_POLICY_UNSPECIFIED,
                .allow_duplicate => .ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE,
                .allow_duplicate_failed_only => .ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
                .reject_duplicate => .ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE,
            },
            .id_conflict_policy = switch (options.id_conflict_policy) {
                .unspecified => .ACTIVITY_ID_CONFLICT_POLICY_UNSPECIFIED,
                .fail => .ACTIVITY_ID_CONFLICT_POLICY_FAIL,
                .use_existing => .ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
            },
        };
        if (options.retry_policy) |rp| {
            var p: proto.temporal.api.common.v1.RetryPolicy = .{
                .initial_interval = protoDuration(rp.initial_interval),
                .backoff_coefficient = rp.backoff_coefficient,
                .maximum_interval = protoDuration(rp.maximum_interval),
                .maximum_attempts = rp.maximum_attempts,
            };
            try p.non_retryable_error_types.appendSlice(a, rp.non_retryable_error_types);
            request.retry_policy = p;
        }
        if (options.search_attributes.len > 0) request.search_attributes = try search_attributes.toProto(a, options.search_attributes, .start);
        var status: ?RpcStatus = null;
        const response = c.rpcWithStatus(wfs.StartActivityExecutionResponse, "StartActivityExecution", request, a, &status) catch |err| switch (err) {
            error.AlreadyExists => return error.ActivityExecutionAlreadyStarted,
            error.RpcFailed => {
                const st = status orelse return error.RpcFailed;
                log.err("starting activity {s} of type \"{s}\" on task queue \"{s}\" failed: {s} (gRPC status {d})", .{ id, activity_type, options.task_queue, st.message, st.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        };
        return c.getActivityHandle(Result, id, response.run_id);
    }

    /// A handle to an activity started with `executeActivity`, for example by another
    /// process. An empty `run_id` follows the latest run of `activity_id`. No call is made,
    /// so a missing activity shows up as `error.NotFound` from the handle's methods.
    pub fn getActivityHandle(c: *Client, comptime Result: type, activity_id: []const u8, run_id: []const u8) error{OutOfMemory}!ActivityHandle(Result) {
        const id = try c.gpa.dupe(u8, activity_id);
        errdefer c.gpa.free(id);
        return .{ .client = c, .id = id, .run_id = try c.gpa.dupe(u8, run_id) };
    }

    /// Lists the activities started with `executeActivity` that match the visibility
    /// `query`, such as `ActivityType = 'Charge' AND ExecutionStatus = 'Running'`; an empty
    /// query lists them all. Activities run by workflows aren't listed. Eventually
    /// consistent, like `listWorkflows`. The iterator fetches a page at a time as `next`
    /// reaches it; nothing is fetched here. Free it with `deinit`.
    pub fn listActivities(c: *Client, query: []const u8, options: ListActivitiesOptions) error{OutOfMemory}!ActivityExecutionIterator {
        return .{ .client = c, .query = try c.gpa.dupe(u8, query), .page_size = options.page_size, .page = .init(c.gpa) };
    }

    /// The number of activities started with `executeActivity` that match the visibility
    /// `query` (see `listActivities`); an empty query counts them all.
    pub fn countActivities(c: *Client, query: []const u8) ListError!u64 {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var status: ?RpcStatus = null;
        const response = c.rpcWithStatus(wfs.CountActivityExecutionsResponse, "CountActivityExecutions", wfs.CountActivityExecutionsRequest{
            .namespace = c.namespace,
            .query = query,
        }, a, &status) catch |err| return visibilityError(err, status, "counting activities", query);
        return std.math.cast(u64, response.count) orelse {
            log.err("CountActivityExecutions for query \"{s}\" returned the count {d}", .{ query, response.count });
            return error.InvalidResponse;
        };
    }

    /// Creates the schedule `id` and returns a handle to it. Returns
    /// `error.ScheduleAlreadyExists` if a schedule with that ID exists. The server rejects an
    /// invalid spec or action with `error.RpcFailed`, logging why.
    pub fn createSchedule(c: *Client, id: []const u8, options: schedule.CreateOptions) CreateScheduleError!ScheduleHandle {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var request: wfs.CreateScheduleRequest = .{
            .namespace = c.namespace,
            .schedule_id = id,
            .schedule = try options.schedule.toProto(a, null),
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
        };
        if (options.trigger_immediately or options.backfill.len > 0) {
            var patch: proto.temporal.api.schedule.v1.SchedulePatch = .{};
            if (options.trigger_immediately) patch.trigger_immediately = .{};
            try patch.backfill_request.ensureTotalCapacity(a, options.backfill.len);
            for (options.backfill) |b| patch.backfill_request.appendAssumeCapacity(b.toProto());
            request.initial_patch = patch;
        }
        if (options.memo.len > 0) {
            var memo: proto.temporal.api.common.v1.Memo = .{};
            try memo.fields.ensureTotalCapacity(a, options.memo.len);
            for (options.memo) |field| memo.fields.appendAssumeCapacity(.{ .key = field.name, .value = field.value });
            request.memo = memo;
        }
        if (options.search_attributes.len > 0) request.search_attributes = try search_attributes.toProto(a, options.search_attributes, .start);
        _ = c.rpc(wfs.CreateScheduleResponse, "CreateSchedule", request, a) catch |err| switch (err) {
            error.AlreadyExists => return error.ScheduleAlreadyExists,
            else => |e| return e,
        };
        return c.getScheduleHandle(id);
    }

    /// A handle to the schedule `id`. No call is made, so a missing schedule shows up as
    /// `error.NotFound` from the handle's methods.
    pub fn getScheduleHandle(c: *Client, id: []const u8) error{OutOfMemory}!ScheduleHandle {
        return .{ .client = c, .id = try c.gpa.dupe(u8, id) };
    }

    /// Lists the namespace's schedules, or those matching `options.query`. Eventually
    /// consistent: a schedule created or changed a moment ago may be missing or out of date.
    /// The iterator fetches a page at a time as `next` reaches it; nothing is fetched here.
    /// Free it with `deinit`.
    pub fn listSchedules(c: *Client, options: schedule.ListOptions) error{OutOfMemory}!ScheduleIterator {
        return .{ .client = c, .query = try c.gpa.dupe(u8, options.query), .page_size = options.page_size, .page = .init(c.gpa) };
    }

    /// Sends `Query` (a `workflow.QueryDef`) with `args` to a workflow and returns the
    /// handler's result, decoded into `arena`. An empty `run_id` queries the latest run. A
    /// closed workflow still answers: the server has a worker replay its history. The server
    /// holds a query sent before the workflow's first workflow task completes until it does;
    /// with no worker polling, the call fails with `error.RpcFailed` when its deadline passes.
    pub fn queryWorkflow(c: *Client, comptime Query: type, arena: Allocator, workflow_id: []const u8, run_id: []const u8, args: Query.Args) QueryError!Query.Result {
        return c.queryWorkflowWithOptions(Query, arena, workflow_id, run_id, args, .{});
    }

    /// `queryWorkflow` with options. Returns `error.QueryFailed` when the handler fails or
    /// the workflow has no handler for the query type, and `error.QueryRejected` when
    /// `options.reject_condition` matches the workflow's state.
    pub fn queryWorkflowWithOptions(c: *Client, comptime Query: type, arena: Allocator, workflow_id: []const u8, run_id: []const u8, args: Query.Args, options: QueryOptions) QueryError!Query.Result {
        var status: ?RpcStatus = null;
        const response = c.rpcWithStatus(wfs.QueryWorkflowResponse, "QueryWorkflow", wfs.QueryWorkflowRequest{
            .namespace = c.namespace,
            .execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .query = .{ .query_type = Query.query_name, .query_args = .{ .payloads = try converter.toPayloads(arena, args) } },
            .query_reject_condition = switch (options.reject_condition) {
                .none => .QUERY_REJECT_CONDITION_NONE,
                .not_open => .QUERY_REJECT_CONDITION_NOT_OPEN,
                .not_completed_cleanly => .QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY,
            },
        }, arena, &status) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                if (options.failure_message) |m| m.* = s.message;
                // The server reports a failed query (a handler error or an unknown query
                // type) as INVALID_ARGUMENT carrying a QueryFailedFailure detail.
                if (s.code == 3 and s.hasDetail("temporal.api.errordetails.v1.QueryFailedFailure", arena)) return error.QueryFailed;
                log.err("QueryWorkflow \"{s}\" on workflow {s} failed: {s} (gRPC status {d})", .{ Query.query_name, workflow_id, s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        };
        if (response.query_rejected) |rejected| {
            if (options.rejected_status) |st| st.* = @fromBackingInt(@intCast(@backingInt(rejected.status)));
            return error.QueryRejected;
        }
        if (Query.Result == void) return;
        const payloads = response.query_result orelse return error.InvalidPayload;
        if (payloads.payloads.items.len == 0) return error.InvalidPayload;
        return converter.fromPayload(Query.Result, arena, payloads.payloads.items[0]);
    }

    /// Sends `Update` (a `workflow.UpdateDef`) with `args` to a workflow and waits until the
    /// update reaches `options.wait_for_stage`. An empty `run_id` targets the latest run.
    /// Read the result with `get` on the returned handle; a rejected update (by its
    /// validator, its handler, or because the workflow has no handler for it) is
    /// `error.UpdateFailed` from `get`, not an error here. Returns `error.NotFound` if no
    /// such workflow exists.
    pub fn updateWorkflow(c: *Client, comptime Update: type, workflow_id: []const u8, run_id: []const u8, args: Update.Args, options: UpdateOptions) UpdateError!WorkflowUpdateHandle(Update.Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const request = try c.updateRequest(a, Update.update_name, workflow_id, run_id, args, options);
        const response = try c.updateUntilStage(a, request);
        const ref_run_id = if (response.update_ref) |r| if (r.workflow_execution) |e| e.run_id else "" else "";
        return .init(c, workflow_id, if (ref_run_id.len > 0) ref_run_id else run_id, request.request.?.meta.?.update_id, response.outcome);
    }

    /// A handle to the update `update_id` already sent to a workflow, for example by another
    /// process. `get` waits for its outcome. An empty `run_id` targets the latest run.
    pub fn getWorkflowUpdateHandle(c: *Client, comptime Result: type, workflow_id: []const u8, run_id: []const u8, update_id: []const u8) error{OutOfMemory}!WorkflowUpdateHandle(Result) {
        return .init(c, workflow_id, run_id, update_id, null);
    }

    /// A handle to an existing workflow, for waiting for its result with `get`. An empty
    /// `run_id` follows the latest run. No call is made, so a missing workflow shows up as
    /// `error.NotFound` from `get`.
    pub fn getWorkflow(c: *Client, comptime T: type, workflow_id: []const u8, run_id: []const u8) error{OutOfMemory}!WorkflowRun(T) {
        const id = try c.gpa.dupe(u8, workflow_id);
        errdefer c.gpa.free(id);
        return .{ .client = c, .id = id, .run_id = try c.gpa.dupe(u8, run_id) };
    }

    /// Sends `Update` with `update_args` to the workflow `start_options.id`, first starting it
    /// with `args` if it isn't running, in one atomic call. `start_options.id_conflict_policy`
    /// is required: `.use_existing` updates a running workflow, `.fail` returns
    /// `error.WorkflowExecutionAlreadyStarted` for one. The handle's `run_id` is the run that
    /// was started or updated; wait for the workflow's result with `getWorkflow`. An empty
    /// `start_options.id` generates a UUID, so the call always starts a new workflow.
    pub fn updateWithStartWorkflow(
        c: *Client,
        comptime Workflow: type,
        start_options: StartWorkflowOptions,
        args: Workflow.Args,
        comptime Update: type,
        update_args: Update.Args,
        update_options: UpdateOptions,
    ) UpdateWithStartError!WorkflowUpdateHandle(Update.Result) {
        if (start_options.id_conflict_policy == .unspecified) {
            log.warn("updateWithStartWorkflow for update \"{s}\" on workflow type \"{s}\": StartWorkflowOptions.id_conflict_policy must be set (for example .use_existing), the server requires it for update-with-start", .{ Update.update_name, Workflow.workflow_name });
            return error.InvalidOptions;
        }
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const id = if (start_options.id.len > 0) start_options.id else try uuid(a, c.io);

        var update = try c.updateRequest(a, Update.update_name, id, "", update_args, update_options);
        const Operation = wfs.ExecuteMultiOperationRequest.Operation;
        var operations: std.ArrayList(Operation) = .empty;
        try operations.appendSlice(a, &.{
            .{ .operation = .{ .start_workflow = try c.startRequest(a, Workflow.workflow_name, id, start_options, args) } },
            .{ .operation = .{ .update_workflow = update } },
        });

        var status: ?RpcStatus = null;
        const response = c.rpcWithStatus(wfs.ExecuteMultiOperationResponse, "ExecuteMultiOperation", wfs.ExecuteMultiOperationRequest{
            .namespace = c.namespace,
            .operations = operations,
        }, a, &status) catch |err| switch (err) {
            // The call fails with the failing operation's status code, and only the start
            // operation can fail with ALREADY_EXISTS.
            error.AlreadyExists => return error.WorkflowExecutionAlreadyStarted,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                return multiOperationError(s, a, id, Update.update_name);
            },
            else => |e| return e,
        };

        const responses = response.responses.items;
        if (responses.len != 2 or responses[0].response == null or responses[0].response.? != .start_workflow or
            responses[1].response == null or responses[1].response.? != .update_workflow)
        {
            log.err("ExecuteMultiOperation for workflow {s} returned {d} responses, want a start and an update response", .{ id, responses.len });
            return error.InvalidResponse;
        }
        const started = responses[0].response.?.start_workflow;
        var updated = responses[1].response.?.update_workflow;
        // The workflow exists now, so a wait the server cut short continues as a plain update
        // with the same update ID, which the server deduplicates.
        if (!reachedStage(updated, update_options.wait_for_stage)) {
            update.workflow_execution.?.run_id = started.run_id;
            updated = try c.updateUntilStage(a, update);
        }
        return .init(c, id, started.run_id, update.request.?.meta.?.update_id, updated.outcome);
    }

    /// Maps a failed ExecuteMultiOperation to an error, logging which operation failed.
    fn multiOperationError(s: RpcStatus, arena: Allocator, workflow_id: []const u8, update_name: []const u8) UpdateWithStartError {
        const detail_name = "temporal.api.errordetails.v1.MultiOperationExecutionFailure";
        var reader: Io.Reader = .fixed(s.details);
        const decoded = proto.google.rpc.Status.decode(&reader, arena) catch proto.google.rpc.Status{};
        for (decoded.details.items) |any| {
            if (!std.mem.endsWith(u8, any.type_url, "/" ++ detail_name)) continue;
            var detail_reader: Io.Reader = .fixed(any.value);
            const detail = proto.temporal.api.errordetails.v1.MultiOperationExecutionFailure.decode(&detail_reader, arena) catch break;
            for (detail.statuses.items, 0..) |op, i| {
                // The server marks the operations it didn't run because another failed as
                // ABORTED; only the failing one says why.
                if (op.code == 0 or op.code == 10) continue;
                const what = if (i == 0) "starting the workflow" else "the update";
                log.err("update-with-start of update \"{s}\" on workflow {s} failed: {s} failed: {s} (gRPC status {d})", .{ update_name, workflow_id, what, op.message, op.code });
            }
            return error.RpcFailed;
        }
        log.err("update-with-start of update \"{s}\" on workflow {s} failed: {s} (gRPC status {d})", .{ update_name, workflow_id, s.message, s.code });
        return error.RpcFailed;
    }

    fn updateRequest(c: *Client, a: Allocator, name: []const u8, workflow_id: []const u8, run_id: []const u8, args: anytype, options: UpdateOptions) error{OutOfMemory}!wfs.UpdateWorkflowExecutionRequest {
        return .{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .wait_policy = .{ .lifecycle_stage = options.wait_for_stage.toProto() },
            .request = .{
                .meta = .{ .update_id = if (options.update_id.len > 0) options.update_id else try uuid(a, c.io), .identity = c.identity },
                .input = .{ .name = name, .args = .{ .payloads = try converter.toPayloads(a, args) } },
            },
        };
    }

    /// Sends `request` until the update reaches the stage it waits for: the server returns
    /// early, below that stage, when its long poll times out.
    fn updateUntilStage(c: *Client, a: Allocator, request: wfs.UpdateWorkflowExecutionRequest) UpdateError!wfs.UpdateWorkflowExecutionResponse {
        const want: UpdateStage = if (request.wait_policy.?.lifecycle_stage == .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED) .accepted else .completed;
        while (true) {
            const response = c.rpc(wfs.UpdateWorkflowExecutionResponse, "UpdateWorkflowExecution", request, a) catch |err| switch (err) {
                error.AlreadyExists => return error.InvalidResponse,
                else => |e| return e,
            };
            if (reachedStage(response, want)) return response;
        }
    }

    fn reachedStage(response: wfs.UpdateWorkflowExecutionResponse, want: UpdateStage) bool {
        return response.outcome != null or @backingInt(response.stage) >= @backingInt(want.toProto());
    }

    const RpcError = error{ OutOfMemory, Canceled, RpcFailed, AlreadyExists, NotFound, InvalidResponse, CodecFailed };

    /// Calls a WorkflowService method with a typed request and response. The response's
    /// memory comes from `arena`.
    fn rpc(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator) RpcError!Response {
        return c.rpcWithStatus(Response, method, request, arena, null);
    }

    /// A failed call's gRPC status; its slices borrow the call's arena.
    const RpcStatus = struct {
        code: u32,
        message: []const u8,
        /// Serialized google.rpc.Status, possibly empty.
        details: []const u8,

        /// Whether the status carries a detail of the protobuf message type `full_name`.
        fn hasDetail(s: RpcStatus, comptime full_name: []const u8, arena: Allocator) bool {
            var reader: Io.Reader = .fixed(s.details);
            const decoded = proto.google.rpc.Status.decode(&reader, arena) catch return false;
            for (decoded.details.items) |any| {
                if (std.mem.endsWith(u8, any.type_url, "/" ++ full_name)) return true;
            }
            return false;
        }
    };

    /// `rpc`, except that when `status` is given, a failure other than ALREADY_EXISTS or
    /// NOT_FOUND is stored there (allocated from `arena`) for the caller to report instead
    /// of being logged.
    fn rpcWithStatus(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, status: ?*?RpcStatus) RpcError!Response {
        var coded = request;
        converter.encodeMessage(@TypeOf(coded), &coded, c.codecs, arena, c.io) catch |err| {
            if (err == error.CodecFailed) log.warn("{s} failed: encoding its payloads with the client's codecs failed", .{method});
            return err;
        };
        var encoded: Io.Writer.Allocating = .init(arena);
        coded.encode(&encoded.writer, arena) catch return error.OutOfMemory;
        var failure: ?bridge.Connection.RpcFailure = null;
        defer if (failure) |f| f.deinit(c.gpa);
        const bytes = c.connection.call(c.gpa, c.io, .workflow, method, encoded.written(), .{ .retry = true }, &failure) catch |err| switch (err) {
            error.RpcFailed => {
                const f = failure.?;
                // gRPC ALREADY_EXISTS and NOT_FOUND are expected outcomes callers handle.
                if (f.status_code == 6) return error.AlreadyExists;
                if (f.status_code == 5) return error.NotFound;
                if (status) |out| {
                    out.* = .{ .code = f.status_code, .message = try arena.dupe(u8, f.message), .details = try arena.dupe(u8, f.details) };
                    return error.RpcFailed;
                }
                log.err("{s} failed: {s} (gRPC status {d})", .{ method, f.message, f.status_code });
                return error.RpcFailed;
            },
            error.InvalidOptions => return error.RpcFailed,
            else => |e| return e,
        };
        defer c.gpa.free(bytes);
        const owned = try arena.dupe(u8, bytes);
        var reader: Io.Reader = .fixed(owned);
        var response = Response.decode(&reader, arena) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidResponse,
        };
        converter.decodeMessage(Response, &response, c.codecs, arena, c.io) catch |err| {
            if (err == error.CodecFailed) log.warn("{s} failed: decoding the response's payloads with the client's codecs failed", .{method});
            return err;
        };
        return response;
    }
};

pub const StartWorkflowOptions = struct {
    /// Workflow ID. Empty generates a random UUID; read it from the run.
    id: []const u8 = "",
    task_queue: []const u8,
    execution_timeout: ?Duration = null,
    run_timeout: ?Duration = null,
    task_timeout: ?Duration = null,
    /// Delay before the first workflow task is dispatched.
    start_delay: ?Duration = null,
    /// Runs the workflow on a cron schedule ("* * * * *"); each run starts a new run of the
    /// same workflow ID. Prefer Schedules for new code.
    cron_schedule: []const u8 = "",
    id_reuse_policy: enum { unspecified, allow_duplicate, allow_duplicate_failed_only, reject_duplicate } = .unspecified,
    id_conflict_policy: enum { unspecified, fail, use_existing, terminate_existing } = .unspecified,
    /// Non-indexed values recorded with the workflow, such as a description. Build them with
    /// `converter.toFields`; they are only read while starting.
    memo: []const converter.Field = &.{},
    /// Indexed values for visibility queries, built with `search_attributes.Key.set`; unsets
    /// are ignored. Only read while starting. A value the server can't index fails the start
    /// with `error.InvalidSearchAttribute`.
    search_attributes: []const search_attributes.Update = &.{},
    /// Asks the server to hand the first workflow task back in the start response, to a
    /// worker for `task_queue` running on this same client, saving that worker a poll. The
    /// client reserves a workflow task slot on such a worker first; with none (or none free)
    /// the workflow starts normally. `WorkflowRun.eagerly_started` says which happened.
    /// Only `executeWorkflow` and `executeWorkflowByName` use it.
    enable_eager_start: bool = false,
};

/// Names an activity by its workflow and activity ID, for completing it without its task
/// token. An empty `run_id` targets the workflow's latest run.
pub const ActivityId = struct {
    workflow_id: []const u8,
    run_id: []const u8 = "",
    activity_id: []const u8,
};

pub const ActivityCompletionError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// The activity is no longer waiting: it completed, failed, or timed out, or its
    /// workflow closed.
    NotFound,
    InvalidResponse,
};

pub const StartError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    WorkflowExecutionAlreadyStarted,
    /// `StartWorkflowOptions.search_attributes` holds a value the server can't index (logged).
    InvalidSearchAttribute,
};

pub const SignalError = error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed };

pub const DescribeError = error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed };

pub const ListError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// Also an invalid query; the server's reason is logged with the query.
    RpcFailed,
    /// The client's namespace doesn't exist.
    NotFound,
    InvalidResponse,
};

pub const ListWorkflowsOptions = struct {
    /// Executions per page; 0 uses the server's default.
    page_size: u31 = 0,
};

/// A workflow run as a visibility query (`Client.listWorkflows`) reports it. Every slice
/// borrows the arena passed to `WorkflowExecutionIterator.next`.
pub const WorkflowExecutionInfo = struct {
    workflow_id: []const u8,
    run_id: []const u8,
    workflow_type: []const u8,
    task_queue: []const u8,
    status: WorkflowExecutionStatus,
    start_time: ?Io.Timestamp,
    /// When the first workflow task was, or will be, scheduled: later than `start_time` for a
    /// delayed start or a cron or retry backoff.
    execution_time: ?Io.Timestamp,
    /// Null while the run is open.
    close_time: ?Io.Timestamp,
    history_length: i64,
    /// Decode a value with `memoValue` or `converter.fieldValue`.
    memo: []const converter.Field,
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The server's record, for anything not lifted into the fields above.
    raw: proto.temporal.api.workflow.v1.WorkflowExecutionInfo,

    /// Decodes the memo value called `name` as `T`, or returns null if the memo has no such
    /// value. Memory comes from `arena`.
    pub fn memoValue(i: *const WorkflowExecutionInfo, comptime T: type, arena: Allocator, name: []const u8) converter.DecodeError!?T {
        return converter.fieldValue(T, arena, i.memo, name);
    }
};

/// Lifts the fields of `info` into a `WorkflowExecutionInfo` that borrows it.
fn executionInfo(arena: Allocator, info: proto.temporal.api.workflow.v1.WorkflowExecutionInfo) error{OutOfMemory}!WorkflowExecutionInfo {
    const execution = info.execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    return .{
        .workflow_id = execution.workflow_id,
        .run_id = execution.run_id,
        .workflow_type = if (info.type) |t| t.name else "",
        .task_queue = info.task_queue,
        .status = @fromBackingInt(@intCast(@backingInt(info.status))),
        .start_time = timestamp(info.start_time),
        .execution_time = timestamp(info.execution_time),
        .close_time = timestamp(info.close_time),
        .history_length = info.history_length,
        .memo = if (info.memo) |m| try converter.fieldsFromMap(arena, m.fields.items) else &.{},
        .search_attributes = try .fromProto(arena, info.search_attributes),
        .raw = info,
    };
}

/// The workflow executions matching a visibility query, from `Client.listWorkflows`. Fetches
/// a page from the server whenever `next` runs past the last one. Owns its memory with the
/// client's allocator: free it with `deinit`. Not thread-safe.
pub const WorkflowExecutionIterator = struct {
    client: *Client,
    query: []u8,
    page_size: u31,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    executions: []const proto.temporal.api.workflow.v1.WorkflowExecutionInfo = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *WorkflowExecutionIterator) void {
        it.page.deinit();
        it.client.gpa.free(it.query);
        it.client.gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next matching execution, copied into `arena`, or null after the last one. A
    /// failed fetch can be retried by calling `next` again.
    pub fn next(it: *WorkflowExecutionIterator, arena: Allocator) ListError!?WorkflowExecutionInfo {
        // The server may return an empty page that still has a token.
        while (it.index == it.executions.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const raw = try it.executions[it.index].dupe(arena);
        it.index += 1;
        return try executionInfo(arena, raw);
    }

    fn fetch(it: *WorkflowExecutionIterator) ListError!void {
        const c = it.client;
        it.executions = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        var status: ?Client.RpcStatus = null;
        const response = c.rpcWithStatus(wfs.ListWorkflowExecutionsResponse, "ListWorkflowExecutions", wfs.ListWorkflowExecutionsRequest{
            .namespace = c.namespace,
            .page_size = it.page_size,
            .next_page_token = it.next_page_token,
            .query = it.query,
        }, a, &status) catch |err| return Client.visibilityError(err, status, "listing workflows", it.query);
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        it.executions = response.executions.items;
        it.fetched = true;
    }
};

/// How `Client.executeActivity` starts an activity. At least one of
/// `start_to_close_timeout` and `schedule_to_close_timeout` is required.
pub const StartActivityOptions = struct {
    /// Activity ID, unique among the namespace's running standalone activities. Empty
    /// generates a random UUID; read it from the handle.
    id: []const u8 = "",
    task_queue: []const u8,
    /// How long the activity may take, retries included.
    schedule_to_close_timeout: ?Duration = null,
    /// How long an attempt may wait for a worker to pick it up. Not retried.
    schedule_to_start_timeout: ?Duration = null,
    /// How long one attempt may run. Retried.
    start_to_close_timeout: ?Duration = null,
    /// The longest gap between heartbeats before an attempt times out. Also how soon a
    /// running activity learns of `cancel`, which reaches it through a heartbeat.
    heartbeat_timeout: ?Duration = null,
    /// Null retries with the server's default policy.
    retry_policy: ?workflow.RetryPolicy = null,
    /// Whether an ID of an activity that has closed may be reused.
    id_reuse_policy: enum { unspecified, allow_duplicate, allow_duplicate_failed_only, reject_duplicate } = .unspecified,
    /// What starting an ID that is still running does: `.fail` returns
    /// `error.ActivityExecutionAlreadyStarted`, `.use_existing` returns a handle to the
    /// running activity.
    id_conflict_policy: enum { unspecified, fail, use_existing } = .unspecified,
    /// Indexed values for `Client.listActivities` queries, built with
    /// `search_attributes.Key.set`; unsets are ignored. Only read while starting.
    search_attributes: []const search_attributes.Update = &.{},
};

pub const StartActivityError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// Also options or a server the call can't work with; the server's reason is logged.
    RpcFailed,
    /// The client's namespace doesn't exist.
    NotFound,
    InvalidResponse,
    /// An activity with this ID is running and `id_conflict_policy` isn't `.use_existing`.
    ActivityExecutionAlreadyStarted,
    /// `StartActivityOptions.search_attributes` holds a value the server can't index (logged).
    InvalidSearchAttribute,
};

pub const ActivityResultError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// No such activity.
    NotFound,
    InvalidResponse,
    InvalidPayload,
    UnknownEncoding,
    WrongArgumentCount,
    /// The activity failed, retries exhausted or not retryable; read why with `failure()`.
    ActivityFailed,
    /// The activity was canceled after `ActivityHandle.cancel`.
    ActivityCanceled,
    ActivityTerminated,
    /// A timeout of `StartActivityOptions` passed.
    ActivityTimedOut,
};

pub const ActivityError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// No such activity.
    NotFound,
    InvalidResponse,
    /// `cancel` or `terminate` of an activity that has already closed.
    ActivityClosed,
};

/// Where a standalone activity is in its lifecycle. Values match the server's; the enum is
/// open so a status added by a newer server is kept rather than rejected.
pub const ActivityExecutionStatus = enum(i32) {
    unspecified = 0,
    running = 1,
    completed = 2,
    failed = 3,
    canceled = 4,
    terminated = 5,
    timed_out = 6,
    paused = 7,
    _,
};

/// A standalone activity as `ActivityHandle.describe` reports it. Every slice borrows the
/// arena passed to `describe`.
pub const ActivityDescription = struct {
    activity_id: []const u8,
    run_id: []const u8,
    activity_type: []const u8,
    task_queue: []const u8,
    status: ActivityExecutionStatus,
    /// The current attempt, from 1.
    attempt: i32,
    schedule_time: ?Io.Timestamp,
    /// When the latest attempt started; null before a worker picked up the first.
    last_started_time: ?Io.Timestamp,
    /// Null while the activity is running.
    close_time: ?Io.Timestamp,
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The server's record, with the last heartbeat's details and the last attempt's
    /// failure, for anything not lifted into the fields above.
    raw: proto.temporal.api.activity.v1.ActivityExecutionInfo,
};

pub const ListActivitiesOptions = struct {
    /// Activities per page; 0 uses the server's default.
    page_size: u31 = 0,
};

/// A standalone activity as a visibility query (`Client.listActivities`) reports it. Every
/// slice borrows the arena passed to `ActivityExecutionIterator.next`.
pub const ActivityExecutionInfo = struct {
    activity_id: []const u8,
    run_id: []const u8,
    activity_type: []const u8,
    task_queue: []const u8,
    status: ActivityExecutionStatus,
    schedule_time: ?Io.Timestamp,
    /// Null while the activity is running.
    close_time: ?Io.Timestamp,
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The server's record, for anything not lifted into the fields above.
    raw: proto.temporal.api.activity.v1.ActivityExecutionListInfo,
};

/// A standalone activity, from `Client.executeActivity` or `Client.getActivityHandle`.
/// Owns its IDs with the client's allocator; free with `deinit`. Not thread-safe.
pub fn ActivityHandle(comptime T: type) type {
    return struct {
        const Self = @This();

        client: *Client,
        id: []u8,
        /// Empty when the handle follows the latest run of `id`.
        run_id: []u8,
        failure_: ?failure_mod.Failure = null,

        pub fn deinit(h: *Self) void {
            h.client.gpa.free(h.id);
            h.client.gpa.free(h.run_id);
            h.* = undefined;
        }

        /// Waits for the activity to close and returns its result. The result and any
        /// failure are allocated from `arena`.
        pub fn get(h: *Self, arena: Allocator) ActivityResultError!T {
            const c = h.client;
            const outcome = while (true) {
                // Each call is a long poll the server may end before the activity closes.
                var status: ?Client.RpcStatus = null;
                const response = c.rpcWithStatus(wfs.PollActivityExecutionResponse, "PollActivityExecution", wfs.PollActivityExecutionRequest{
                    .namespace = c.namespace,
                    .activity_id = h.id,
                    .run_id = h.run_id,
                }, arena, &status) catch |err| switch (err) {
                    error.AlreadyExists => return error.InvalidResponse,
                    error.RpcFailed => {
                        const s = status orelse return error.RpcFailed;
                        // DEADLINE_EXCEEDED: the long poll ran out of time.
                        if (s.code == 4) continue;
                        log.err("waiting for the result of activity {s} failed: {s} (gRPC status {d})", .{ h.id, s.message, s.code });
                        return error.RpcFailed;
                    },
                    else => |e| return e,
                };
                if (response.outcome) |o| break o;
            };
            switch (outcome.value orelse return error.InvalidResponse) {
                .result => |payloads| {
                    if (T == void) return;
                    if (payloads.payloads.items.len == 0) return error.InvalidPayload;
                    return converter.fromPayload(T, arena, payloads.payloads.items[0]);
                },
                .failure => |f| {
                    h.failure_ = f;
                    const info = f.failure_info orelse return error.ActivityFailed;
                    return switch (info) {
                        .canceled_failure_info => error.ActivityCanceled,
                        .terminated_failure_info => error.ActivityTerminated,
                        .timeout_failure_info => error.ActivityTimedOut,
                        else => error.ActivityFailed,
                    };
                },
            }
        }

        /// Why the activity didn't complete, after `get` returned `error.ActivityFailed`,
        /// `ActivityCanceled`, `ActivityTerminated` or `ActivityTimedOut`. Borrows the arena
        /// passed to `get`.
        pub fn failure(h: *const Self) ?*const failure_mod.Failure {
            return if (h.failure_) |*f| f else null;
        }

        /// The activity's status, type, attempt and times. The description and all its
        /// memory come from `arena`.
        pub fn describe(h: *const Self, arena: Allocator) ActivityError!ActivityDescription {
            const c = h.client;
            const response = c.rpc(wfs.DescribeActivityExecutionResponse, "DescribeActivityExecution", wfs.DescribeActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = h.run_id,
                .include_heartbeat_details = true,
                .include_last_failure = true,
            }, arena) catch |err| switch (err) {
                error.AlreadyExists => return error.InvalidResponse,
                else => |e| return e,
            };
            const info = response.info orelse {
                log.err("DescribeActivityExecution for activity {s} returned no activity info", .{h.id});
                return error.InvalidResponse;
            };
            return .{
                .activity_id = info.activity_id,
                .run_id = info.run_id,
                .activity_type = if (info.activity_type) |t| t.name else "",
                .task_queue = info.task_queue,
                .status = @fromBackingInt(@intCast(@backingInt(info.status))),
                .attempt = info.attempt,
                .schedule_time = timestamp(info.schedule_time),
                .last_started_time = timestamp(info.last_started_time),
                .close_time = timestamp(info.close_time),
                .search_attributes = try .fromProto(arena, info.search_attributes),
                .raw = info,
            };
        }

        /// Requests the activity's cancellation. A running attempt learns of it from its
        /// next heartbeat (see `StartActivityOptions.heartbeat_timeout`) as `std.Io`
        /// cancellation, and the activity ends canceled once it returns `error.Canceled`.
        /// `reason` is recorded with the request. Returns `error.ActivityClosed` if the
        /// activity has already closed.
        pub fn cancel(h: *const Self, reason: []const u8) ActivityError!void {
            const c = h.client;
            var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
            defer arena_state.deinit();
            const a = arena_state.allocator();
            var status: ?Client.RpcStatus = null;
            _ = c.rpcWithStatus(wfs.RequestCancelActivityExecutionResponse, "RequestCancelActivityExecution", wfs.RequestCancelActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = h.run_id,
                .identity = c.identity,
                .request_id = try uuid(a, c.io),
                .reason = reason,
            }, a, &status) catch |err| return h.controlError(err, status, "canceling");
        }

        fn controlError(h: *const Self, err: Client.RpcError, status: ?Client.RpcStatus, comptime what: []const u8) ActivityError {
            switch (err) {
                error.AlreadyExists => return error.InvalidResponse,
                error.RpcFailed => {
                    const s = status orelse return error.RpcFailed;
                    // FAILED_PRECONDITION: the activity is in a terminal state.
                    if (s.code == 9) return error.ActivityClosed;
                    log.err(what ++ " activity {s} failed: {s} (gRPC status {d})", .{ h.id, s.message, s.code });
                    return error.RpcFailed;
                },
                else => |e| return e,
            }
        }

        /// Terminates the activity: it closes at once, and a running attempt's result is
        /// discarded. Returns `error.ActivityClosed` if the activity has already closed.
        pub fn terminate(h: *const Self, reason: []const u8) ActivityError!void {
            const c = h.client;
            var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
            defer arena_state.deinit();
            const a = arena_state.allocator();
            var status: ?Client.RpcStatus = null;
            _ = c.rpcWithStatus(wfs.TerminateActivityExecutionResponse, "TerminateActivityExecution", wfs.TerminateActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = h.run_id,
                .identity = c.identity,
                .request_id = try uuid(a, c.io),
                .reason = reason,
            }, a, &status) catch |err| return h.controlError(err, status, "terminating");
        }
    };
}

/// The standalone activities matching a visibility query, from `Client.listActivities`.
/// Fetches a page from the server whenever `next` runs past the last one. Owns its memory
/// with the client's allocator: free it with `deinit`. Not thread-safe.
pub const ActivityExecutionIterator = struct {
    client: *Client,
    query: []u8,
    page_size: u31,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    executions: []const proto.temporal.api.activity.v1.ActivityExecutionListInfo = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *ActivityExecutionIterator) void {
        it.page.deinit();
        it.client.gpa.free(it.query);
        it.client.gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next matching activity, copied into `arena`, or null after the last one. A
    /// failed fetch can be retried by calling `next` again.
    pub fn next(it: *ActivityExecutionIterator, arena: Allocator) ListError!?ActivityExecutionInfo {
        // The server may return an empty page that still has a token.
        while (it.index == it.executions.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const raw = try it.executions[it.index].dupe(arena);
        it.index += 1;
        return .{
            .activity_id = raw.activity_id,
            .run_id = raw.run_id,
            .activity_type = if (raw.activity_type) |t| t.name else "",
            .task_queue = raw.task_queue,
            .status = @fromBackingInt(@intCast(@backingInt(raw.status))),
            .schedule_time = timestamp(raw.schedule_time),
            .close_time = timestamp(raw.close_time),
            .search_attributes = try .fromProto(arena, raw.search_attributes),
            .raw = raw,
        };
    }

    fn fetch(it: *ActivityExecutionIterator) ListError!void {
        const c = it.client;
        it.executions = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        var status: ?Client.RpcStatus = null;
        const response = c.rpcWithStatus(wfs.ListActivityExecutionsResponse, "ListActivityExecutions", wfs.ListActivityExecutionsRequest{
            .namespace = c.namespace,
            .page_size = it.page_size,
            .next_page_token = it.next_page_token,
            .query = it.query,
        }, a, &status) catch |err| return Client.visibilityError(err, status, "listing activities", it.query);
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        it.executions = response.executions.items;
        it.fetched = true;
    }
};

pub const CreateScheduleError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// Also an invalid schedule; the server's reason is logged.
    RpcFailed,
    /// The client's namespace doesn't exist.
    NotFound,
    InvalidResponse,
    ScheduleAlreadyExists,
    /// A search attribute value the server can't index (logged).
    InvalidSearchAttribute,
};

pub const ScheduleError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// No such schedule.
    NotFound,
    InvalidResponse,
};

/// A schedule on the server, from `Client.createSchedule` or `Client.getScheduleHandle`.
/// Owns its ID with the client's allocator; free with `deinit`. Each method is one call to
/// the server, so a handle can be used from several threads at once.
pub const ScheduleHandle = struct {
    client: *Client,
    id: []u8,

    pub fn deinit(h: *ScheduleHandle) void {
        h.client.gpa.free(h.id);
        h.* = undefined;
    }

    /// The schedule, its recent and upcoming actions, memo and search attributes. The
    /// description and all its memory come from `arena`.
    pub fn describe(h: *const ScheduleHandle, arena: Allocator) ScheduleError!schedule.Description {
        const c = h.client;
        const response = c.rpc(wfs.DescribeScheduleResponse, "DescribeSchedule", wfs.DescribeScheduleRequest{
            .namespace = c.namespace,
            .schedule_id = h.id,
        }, arena) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
        return schedule.Description.fromProto(arena, h.id, response);
    }

    /// Changes the schedule by read-modify-write: describes it, calls
    /// `f(context, arena, &description)`, and sends the `schedule.Update` it returns; null
    /// leaves the schedule as it is. The update carries the description's conflict token, so
    /// the server doesn't apply it over a change made since the describe. The server applies
    /// the update's search attributes a moment after the call returns.
    ///
    /// The description lives in `arena`, and anything the callback allocates from it lives
    /// until the request is sent; `update` frees it all before returning.
    ///
    ///     try handle.update(Io.Duration.fromSeconds(3600), struct {
    ///         fn f(every: Io.Duration, arena: Allocator, d: *schedule.Description) !?schedule.Update {
    ///             const intervals = try arena.alloc(schedule.Interval, d.schedule.spec.intervals.len + 1);
    ///             @memcpy(intervals[1..], d.schedule.spec.intervals);
    ///             intervals[0] = .{ .every = every };
    ///             d.schedule.spec.intervals = intervals;
    ///             return .{ .schedule = d.schedule };
    ///         }
    ///     }.f);
    pub fn update(h: *const ScheduleHandle, context: anytype, comptime f: anytype) (UpdateScheduleError || CallbackError(f))!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var description = try h.describe(a);
        const changes: schedule.Update = (try f(context, a, &description)) orelse return;
        _ = c.rpc(wfs.UpdateScheduleResponse, "UpdateSchedule", wfs.UpdateScheduleRequest{
            .namespace = c.namespace,
            .schedule_id = h.id,
            .schedule = try changes.schedule.toProto(a, description.raw.schedule),
            .conflict_token = description.raw.conflict_token,
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .search_attributes = if (changes.search_attributes) |updates| try search_attributes.toProto(a, updates, .start) else null,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    fn CallbackError(comptime f: anytype) type {
        const Return = @typeInfo(@TypeOf(f)).@"fn".return_type.?;
        return switch (@typeInfo(Return)) {
            .error_union => |eu| eu.error_set,
            else => error{},
        };
    }

    /// Takes the schedule's action now, whatever its spec and even while it is paused.
    /// `.unspecified` uses the schedule's overlap policy.
    pub fn trigger(h: *const ScheduleHandle, overlap: schedule.OverlapPolicy) ScheduleError!void {
        return h.patch(.{ .trigger_immediately = .{ .overlap_policy = @fromBackingInt(@backingInt(overlap)) } });
    }

    /// Takes the actions the schedule would have taken in each period of `periods`.
    pub fn backfill(h: *const ScheduleHandle, periods: []const schedule.Backfill) ScheduleError!void {
        var arena_state: std.heap.ArenaAllocator = .init(h.client.gpa);
        defer arena_state.deinit();
        const requests = try arena_state.allocator().alloc(proto.temporal.api.schedule.v1.BackfillRequest, periods.len);
        for (periods, requests) |p, *r| r.* = p.toProto();
        return h.patch(.{ .backfill_request = .fromOwnedSlice(requests) });
    }

    /// Pauses the schedule: it takes no action until `unpause` (a `trigger` still runs it).
    /// `note` says why; empty records "Paused".
    pub fn pause(h: *const ScheduleHandle, note: []const u8) ScheduleError!void {
        return h.patch(.{ .pause = if (note.len > 0) note else "Paused" });
    }

    /// Resumes a paused schedule. `note` says why; empty records "Unpaused".
    pub fn unpause(h: *const ScheduleHandle, note: []const u8) ScheduleError!void {
        return h.patch(.{ .unpause = if (note.len > 0) note else "Unpaused" });
    }

    fn patch(h: *const ScheduleHandle, p: proto.temporal.api.schedule.v1.SchedulePatch) ScheduleError!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = c.rpc(wfs.PatchScheduleResponse, "PatchSchedule", wfs.PatchScheduleRequest{
            .namespace = c.namespace,
            .schedule_id = h.id,
            .patch = p,
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Deletes the schedule. Workflows it started keep running.
    pub fn delete(h: *const ScheduleHandle) ScheduleError!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        _ = c.rpc(wfs.DeleteScheduleResponse, "DeleteSchedule", wfs.DeleteScheduleRequest{
            .namespace = c.namespace,
            .schedule_id = h.id,
            .identity = c.identity,
        }, arena_state.allocator()) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }
};

pub const UpdateScheduleError = ScheduleError || error{
    /// A search attribute value in the update the server can't index (logged).
    InvalidSearchAttribute,
};

/// The schedules from `Client.listSchedules`. Fetches a page from the server whenever `next`
/// runs past the last one. Owns its memory with the client's allocator: free it with
/// `deinit`. Not thread-safe.
pub const ScheduleIterator = struct {
    client: *Client,
    query: []u8,
    page_size: u31,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    entries: []const proto.temporal.api.schedule.v1.ScheduleListEntry = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *ScheduleIterator) void {
        it.page.deinit();
        it.client.gpa.free(it.query);
        it.client.gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next schedule, copied into `arena`, or null after the last one. A failed fetch
    /// can be retried by calling `next` again.
    pub fn next(it: *ScheduleIterator, arena: Allocator) ListError!?schedule.ListEntry {
        // The server may return an empty page that still has a token.
        while (it.index == it.entries.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const raw = try it.entries[it.index].dupe(arena);
        it.index += 1;
        return try schedule.ListEntry.fromProto(arena, raw);
    }

    fn fetch(it: *ScheduleIterator) ListError!void {
        const c = it.client;
        it.entries = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        var status: ?Client.RpcStatus = null;
        const response = c.rpcWithStatus(wfs.ListSchedulesResponse, "ListSchedules", wfs.ListSchedulesRequest{
            .namespace = c.namespace,
            .maximum_page_size = it.page_size,
            .next_page_token = it.next_page_token,
            .query = it.query,
        }, a, &status) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                log.err("listing schedules with the query \"{s}\" failed: {s} (gRPC status {d})", .{ it.query, s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        };
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        it.entries = response.schedules.items;
        it.fetched = true;
    }
};

pub const History = history_mod.History;
pub const HistoryEvent = history_mod.HistoryEvent;

pub const HistoryError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// No such workflow or run.
    NotFound,
    InvalidResponse,
};

pub const HistoryIteratorOptions = struct {
    /// Keep a run that is still open going: `next` waits (long-polling the server) for the
    /// events it records next, and returns null only once the run has closed. Without it,
    /// `next` returns null after the events recorded so far.
    wait_new_event: bool = false,
    filter: enum {
        all,
        /// Only the event that closed the run (completed, failed, continued as new, ...);
        /// with `wait_new_event`, `next` waits for the run to close.
        close_event,
    } = .all,
    /// The most events per page; 0 uses the server's default.
    page_size: u31 = 0,
};

/// The events of a workflow run's history, from `Client.historyIterator`. Fetches a page
/// from the server whenever `next` runs past the last one. Owns its memory with the client's
/// allocator: free it with `deinit`. Not thread-safe.
pub const HistoryIterator = struct {
    client: *Client,
    workflow_id: []u8,
    run_id: []u8,
    options: HistoryIteratorOptions,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    events: []const HistoryEvent = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *HistoryIterator) void {
        it.page.deinit();
        const gpa = it.client.gpa;
        gpa.free(it.workflow_id);
        gpa.free(it.run_id);
        gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next event, copied into `arena`, or null after the last one. With
    /// `wait_new_event`, blocks until the run records another event or closes. A failed
    /// fetch can be retried by calling `next` again.
    pub fn next(it: *HistoryIterator, arena: Allocator) HistoryError!?HistoryEvent {
        // A long poll that times out returns an empty page that still has a token.
        while (it.index == it.events.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const event = try it.events[it.index].dupe(arena);
        it.index += 1;
        return event;
    }

    fn fetch(it: *HistoryIterator) HistoryError!void {
        const c = it.client;
        it.events = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        const response = c.rpc(wfs.GetWorkflowExecutionHistoryResponse, "GetWorkflowExecutionHistory", wfs.GetWorkflowExecutionHistoryRequest{
            .namespace = c.namespace,
            .execution = .{ .workflow_id = it.workflow_id, .run_id = it.run_id },
            .maximum_page_size = it.options.page_size,
            .next_page_token = it.next_page_token,
            .wait_new_event = it.options.wait_new_event,
            .history_event_filter_type = switch (it.options.filter) {
                .all => .HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
                .close_event => .HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
            },
            .skip_archival = true,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        if (response.history) |h| it.events = h.events.items;
        it.fetched = true;
    }
};

pub const QueryOptions = struct {
    /// When the server rejects the query instead of sending it to the workflow.
    reject_condition: QueryRejectCondition = .none,
    /// On `error.QueryFailed` (or `error.RpcFailed`), receives the server's message, allocated
    /// from the query's arena: the handler's error name, or for an unknown query type a
    /// message listing the workflow's registered query types.
    failure_message: ?*[]const u8 = null,
    /// On `error.QueryRejected`, receives the workflow's status.
    rejected_status: ?*WorkflowExecutionStatus = null,
};

pub const QueryRejectCondition = enum {
    /// Never reject: closed workflows are queried too.
    none,
    /// Reject the query if the workflow is closed.
    not_open,
    /// Reject the query if the workflow closed other than by completing.
    not_completed_cleanly,
};

pub const QueryError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    InvalidPayload,
    UnknownEncoding,
    WrongArgumentCount,
    /// The handler failed, or the workflow has no handler for the query type.
    QueryFailed,
    /// The workflow's state matched `QueryOptions.reject_condition`.
    QueryRejected,
};

/// Where a workflow run is in its lifecycle. Values match the server's; the enum is open so
/// a status added by a newer server is kept rather than rejected.
pub const WorkflowExecutionStatus = enum(i32) {
    unspecified = 0,
    running = 1,
    completed = 2,
    failed = 3,
    canceled = 4,
    terminated = 5,
    continued_as_new = 6,
    timed_out = 7,
    paused = 8,
    _,
};

/// A workflow run as `Client.describeWorkflow` reports it. Every slice borrows the arena
/// passed to `describeWorkflow`.
pub const WorkflowDescription = struct {
    workflow_id: []const u8,
    run_id: []const u8,
    workflow_type: []const u8,
    task_queue: []const u8,
    status: WorkflowExecutionStatus,
    start_time: ?Io.Timestamp,
    /// When the first workflow task was, or will be, scheduled: later than `start_time` for a
    /// delayed start or a cron or retry backoff.
    execution_time: ?Io.Timestamp,
    /// Null while the run is open.
    close_time: ?Io.Timestamp,
    history_length: i64,
    /// Decode a value with `memoValue` or `converter.fieldValue`.
    memo: []const converter.Field,
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The full server response, for anything not lifted into the fields above (pending
    /// activities, children, versioning info, ...).
    raw: wfs.DescribeWorkflowExecutionResponse,

    /// Decodes the memo value called `name` as `T`, or returns null if the memo has no such
    /// value. Memory comes from `arena`.
    pub fn memoValue(d: *const WorkflowDescription, comptime T: type, arena: Allocator, name: []const u8) converter.DecodeError!?T {
        return converter.fieldValue(T, arena, d.memo, name);
    }
};

pub const ResultError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    InvalidPayload,
    UnknownEncoding,
    WrongArgumentCount,
    WorkflowFailed,
    WorkflowCanceled,
    WorkflowTerminated,
    WorkflowTimedOut,
    WorkflowContinuedAsNew,
};

/// A started workflow run.
pub fn WorkflowRun(comptime T: type) type {
    return struct {
        const Self = @This();

        client: *Client,
        id: []u8,
        /// The run this handle follows. `get` moves it along continue-as-new chains.
        run_id: []u8,
        /// The start handed the first workflow task straight to a worker on this client
        /// (`StartWorkflowOptions.enable_eager_start`).
        eagerly_started: bool = false,
        failure_: ?failure_mod.Failure = null,

        pub fn deinit(run: *Self) void {
            run.client.gpa.free(run.id);
            run.client.gpa.free(run.run_id);
            run.* = undefined;
        }

        /// Waits for the workflow to close and returns its result, following continue-as-new
        /// (and cron and retry runs). The result and any failure are allocated from `arena`.
        pub fn get(run: *Self, arena: Allocator) ResultError!T {
            return run.getWithOptions(arena, .{});
        }

        pub const GetOptions = struct {
            /// Return the result of this run instead of following it to the runs it starts
            /// (continue-as-new, cron, retries). A run that
            /// continued as new returns `error.WorkflowContinuedAsNew`.
            follow_runs: bool = true,
        };

        /// `get` with options.
        pub fn getWithOptions(run: *Self, arena: Allocator, options: GetOptions) ResultError!T {
            const c = run.client;
            var token: []const u8 = "";
            while (true) {
                const response = c.rpc(wfs.GetWorkflowExecutionHistoryResponse, "GetWorkflowExecutionHistory", wfs.GetWorkflowExecutionHistoryRequest{
                    .namespace = c.namespace,
                    .execution = .{ .workflow_id = run.id, .run_id = run.run_id },
                    .wait_new_event = true,
                    .history_event_filter_type = .HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
                    .skip_archival = true,
                    .next_page_token = token,
                }, arena) catch |err| switch (err) {
                    error.AlreadyExists => return error.InvalidResponse,
                    else => |e| return e,
                };
                const history = response.history orelse {
                    token = response.next_page_token;
                    continue;
                };
                if (history.events.items.len == 0) {
                    token = response.next_page_token;
                    continue;
                }
                const event = history.events.items[history.events.items.len - 1];
                const attrs = event.attributes orelse return error.InvalidResponse;
                // A cron or retry run that closes names the run it started; follow it.
                const next_run: []const u8 = switch (attrs) {
                    .workflow_execution_completed_event_attributes => |e| e.new_execution_run_id,
                    .workflow_execution_failed_event_attributes => |e| e.new_execution_run_id,
                    .workflow_execution_timed_out_event_attributes => |e| e.new_execution_run_id,
                    .workflow_execution_continued_as_new_event_attributes => |e| e.new_execution_run_id,
                    else => "",
                };
                if (attrs == .workflow_execution_continued_as_new_event_attributes and !options.follow_runs)
                    return error.WorkflowContinuedAsNew;
                if (options.follow_runs and next_run.len > 0) {
                    const new_run = try c.gpa.dupe(u8, next_run);
                    c.gpa.free(run.run_id);
                    run.run_id = new_run;
                    token = "";
                    continue;
                }
                switch (attrs) {
                    .workflow_execution_completed_event_attributes => |done| {
                        if (T == void) return;
                        const payloads = done.result orelse return error.InvalidPayload;
                        if (payloads.payloads.items.len == 0) return error.InvalidPayload;
                        return converter.fromPayload(T, arena, payloads.payloads.items[0]);
                    },
                    .workflow_execution_failed_event_attributes => |failed| {
                        run.failure_ = failed.failure;
                        return error.WorkflowFailed;
                    },
                    .workflow_execution_canceled_event_attributes => return error.WorkflowCanceled,
                    .workflow_execution_terminated_event_attributes => return error.WorkflowTerminated,
                    .workflow_execution_timed_out_event_attributes => return error.WorkflowTimedOut,
                    else => return error.InvalidResponse,
                }
            }
        }

        /// The workflow's failure after `get` returned `error.WorkflowFailed`.
        pub fn failure(run: *const Self) ?*const failure_mod.Failure {
            return if (run.failure_) |*f| f else null;
        }
    };
}

/// How far an update must get before `Client.updateWorkflow` returns. (The server can't wait
/// for an update only to be admitted.)
pub const UpdateStage = enum {
    /// The workflow's validator accepted the update and its handler started.
    accepted,
    /// The handler finished, or the update was rejected.
    completed,

    fn toProto(s: UpdateStage) enums.UpdateWorkflowExecutionLifecycleStage {
        return switch (s) {
            .accepted => .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
            .completed => .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
        };
    }
};

pub const UpdateOptions = struct {
    /// Identifies the update in the workflow; an update ID sent again returns the first
    /// update's outcome instead of running it twice. Empty generates a UUID.
    update_id: []const u8 = "",
    wait_for_stage: UpdateStage = .completed,
};

pub const UpdateError = error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed };

pub const UpdateWithStartError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    /// `StartWorkflowOptions.id_conflict_policy` is unspecified.
    InvalidOptions,
    /// The workflow is running and `id_conflict_policy` is `.fail`.
    WorkflowExecutionAlreadyStarted,
    /// `StartWorkflowOptions.search_attributes` holds a value the server can't index (logged).
    InvalidSearchAttribute,
};

pub const UpdateResultError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    InvalidPayload,
    UnknownEncoding,
    WrongArgumentCount,
    /// The update was rejected or its handler failed; read why with `failure()`.
    UpdateFailed,
};

/// An update sent to a workflow. Owns its IDs with the client's allocator; free with
/// `deinit`. Not thread-safe.
pub fn WorkflowUpdateHandle(comptime T: type) type {
    return struct {
        const Self = @This();

        client: *Client,
        workflow_id: []u8,
        /// Empty when the handle was made for the latest run.
        run_id: []u8,
        update_id: []u8,
        /// The encoded outcome when the update call already returned one.
        outcome: ?[]u8,
        failure_: ?failure_mod.Failure = null,

        fn init(c: *Client, workflow_id: []const u8, run_id: []const u8, update_id: []const u8, outcome: ?proto.temporal.api.update.v1.Outcome) error{OutOfMemory}!Self {
            const wid = try c.gpa.dupe(u8, workflow_id);
            errdefer c.gpa.free(wid);
            const rid = try c.gpa.dupe(u8, run_id);
            errdefer c.gpa.free(rid);
            const uid = try c.gpa.dupe(u8, update_id);
            errdefer c.gpa.free(uid);
            var encoded: ?[]u8 = null;
            if (outcome) |o| {
                var w: Io.Writer.Allocating = .init(c.gpa);
                errdefer w.deinit();
                // The encoder allocates its scratch from the same allocator; an arena keeps
                // that out of the handle.
                var scratch: std.heap.ArenaAllocator = .init(c.gpa);
                defer scratch.deinit();
                o.encode(&w.writer, scratch.allocator()) catch return error.OutOfMemory;
                encoded = try w.toOwnedSlice();
            }
            return .{ .client = c, .workflow_id = wid, .run_id = rid, .update_id = uid, .outcome = encoded };
        }

        pub fn deinit(h: *Self) void {
            const gpa = h.client.gpa;
            gpa.free(h.workflow_id);
            gpa.free(h.run_id);
            gpa.free(h.update_id);
            if (h.outcome) |o| gpa.free(o);
            h.* = undefined;
        }

        /// Waits for the update to complete and returns its result. The result and any failure
        /// are allocated from `arena`.
        pub fn get(h: *Self, arena: Allocator) UpdateResultError!T {
            const c = h.client;
            const outcome: proto.temporal.api.update.v1.Outcome = if (h.outcome) |bytes| blk: {
                var reader: Io.Reader = .fixed(try arena.dupe(u8, bytes));
                break :blk proto.temporal.api.update.v1.Outcome.decode(&reader, arena) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.InvalidResponse,
                };
            } else while (true) {
                // Each call is a long poll the server may end before the update completes.
                const response = c.rpc(wfs.PollWorkflowExecutionUpdateResponse, "PollWorkflowExecutionUpdate", wfs.PollWorkflowExecutionUpdateRequest{
                    .namespace = c.namespace,
                    .update_ref = .{ .workflow_execution = .{ .workflow_id = h.workflow_id, .run_id = h.run_id }, .update_id = h.update_id },
                    .identity = c.identity,
                    .wait_policy = .{ .lifecycle_stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED },
                }, arena) catch |err| switch (err) {
                    error.AlreadyExists => return error.InvalidResponse,
                    else => |e| return e,
                };
                if (response.outcome) |o| break o;
            };
            switch (outcome.value orelse return error.InvalidResponse) {
                .success => |payloads| {
                    if (T == void) return;
                    if (payloads.payloads.items.len == 0) return error.InvalidPayload;
                    return converter.fromPayload(T, arena, payloads.payloads.items[0]);
                },
                .failure => |f| {
                    h.failure_ = f;
                    return error.UpdateFailed;
                },
            }
        }

        /// Why the update failed, after `get` returned `error.UpdateFailed`. Borrows the arena
        /// passed to `get`.
        pub fn failure(h: *const Self) ?*const failure_mod.Failure {
            return if (h.failure_) |*f| f else null;
        }
    };
}

/// Client options from the environment and the Temporal config file, as the Temporal CLI
/// reads them: TEMPORAL_ADDRESS,
/// TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, TEMPORAL_PROFILE, TEMPORAL_CONFIG_FILE, and the
/// default profile in the config file. Strings are allocated from `arena`.
pub fn loadDefaultOptions(arena: Allocator) error{ OutOfMemory, InvalidConfig }!Options {
    const result = bridge.c.temporal_core_client_env_config_profile_load(&.{
        .profile = bridge.bytes.ref(""),
        .path = bridge.bytes.ref(""),
        .data = bridge.bytes.ref(""),
        .disable_file = false,
        .disable_env = false,
        .config_file_strict = false,
        .env_vars = bridge.bytes.ref(""),
    });
    if (bridge.bytes.Owned.take(null, result.fail)) |fail| {
        defer fail.deinit();
        log.err("loading client configuration failed: {s}", .{fail.bytes()});
        return error.InvalidConfig;
    }
    const json = bridge.bytes.Owned.take(null, result.success) orelse return .{};
    defer json.deinit();

    const Profile = struct {
        address: ?[]const u8 = null,
        namespace: ?[]const u8 = null,
        api_key: ?[]const u8 = null,
        tls: ?struct { disabled: ?bool = null, server_name: ?[]const u8 = null } = null,
        grpc_meta: ?std.json.ArrayHashMap([]const u8) = null,
    };
    const profile = std.json.parseFromSliceLeaky(Profile, arena, json.bytes(), .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidConfig,
    };

    var options: Options = .{};
    if (profile.address) |v| options.host_port = v;
    if (profile.namespace) |v| options.namespace = v;
    if (profile.api_key) |v| options.api_key = v;
    if (profile.tls) |t| {
        if (!(t.disabled orelse false)) options.tls = .{ .server_name = t.server_name orelse "" };
    } else if (options.api_key.len > 0) {
        // An API key implies TLS, as in the other SDKs.
        options.tls = .{};
    }
    if (profile.grpc_meta) |meta| {
        const headers = try arena.alloc(bridge.bytes.MetadataEntry, meta.map.count());
        var it = meta.map.iterator();
        var i: usize = 0;
        while (it.next()) |kv| : (i += 1) headers[i] = .{ .key = kv.key_ptr.*, .value = kv.value_ptr.* };
        options.headers = headers;
    }
    return options;
}

fn defaultIdentity(gpa: Allocator) error{OutOfMemory}![]u8 {
    var host_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
    const host = std.posix.gethostname(&host_buf) catch "unknown-host";
    return std.fmt.allocPrint(gpa, "{d}@{s}", .{ std.c.getpid(), host });
}

/// A random (version 4) UUID string, for example to make unique workflow IDs.
pub fn uuid(arena: Allocator, io: Io) error{OutOfMemory}![]const u8 {
    var b: [16]u8 = undefined;
    io.random(&b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    const hex = std.fmt.bytesToHex(b, .lower);
    return std.fmt.allocPrint(arena, "{s}-{s}-{s}-{s}-{s}", .{ hex[0..8], hex[8..12], hex[12..16], hex[16..20], hex[20..32] });
}

fn timestamp(t: ?proto.google.protobuf.Timestamp) ?Io.Timestamp {
    const v = t orelse return null;
    return .fromNanoseconds(@as(i96, v.seconds) * std.time.ns_per_s + v.nanos);
}

fn protoDuration(d: ?Duration) ?proto.google.protobuf.Duration {
    const v = d orelse return null;
    return .{
        .seconds = @intCast(@divFloor(v.nanoseconds, std.time.ns_per_s)),
        .nanos = @intCast(@mod(v.nanoseconds, std.time.ns_per_s)),
    };
}

test "uuids are version 4 and well formed" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const id = try uuid(arena.allocator(), std.testing.io);
    try std.testing.expectEqual(@as(usize, 36), id.len);
    try std.testing.expectEqual(@as(u8, '4'), id[14]);
}

test "workflow execution statuses match the server's values" {
    const info = @typeInfo(WorkflowExecutionStatus).@"enum";
    inline for (info.field_names, info.field_values) |name, value| {
        const upper = comptime blk: {
            var buf: [name.len]u8 = undefined;
            _ = std.ascii.upperString(&buf, name);
            break :blk buf;
        };
        const server = @field(enums.WorkflowExecutionStatus, "WORKFLOW_EXECUTION_STATUS_" ++ upper);
        try std.testing.expectEqual(value, @backingInt(server));
    }
}

test "activity execution statuses match the server's values" {
    const info = @typeInfo(ActivityExecutionStatus).@"enum";
    inline for (info.field_names, info.field_values) |name, value| {
        const upper = comptime blk: {
            var buf: [name.len]u8 = undefined;
            _ = std.ascii.upperString(&buf, name);
            break :blk buf;
        };
        const server = @field(enums.ActivityExecutionStatus, "ACTIVITY_EXECUTION_STATUS_" ++ upper);
        try std.testing.expectEqual(value, @backingInt(server));
    }
}

const testing = std.testing;

test "core log fields split into keys, with strings unquoted and other values as JSON" {
    var buf: [1024]u8 = undefined;
    var fba: std.heap.FixedBufferAllocator = .init(&buf);
    var out: [4]log_mod.Field = undefined;
    const n = coreLogFields(fba.allocator(), "{\"task_queue\":\"q\",\"attempt\":2,\"sticky\":true}", &out);
    try testing.expectEqual(3, n);
    try testing.expectEqualStrings("task_queue", out[0].key);
    try testing.expectEqualStrings("q", out[0].value);
    try testing.expectEqualStrings("2", out[1].value);
    try testing.expectEqualStrings("true", out[2].value);
    try testing.expectEqual(0, coreLogFields(fba.allocator(), "{}", &out));

    var tiny: [8]u8 = undefined;
    var small: std.heap.FixedBufferAllocator = .init(&tiny);
    try testing.expectEqual(1, coreLogFields(small.allocator(), "{\"a\":\"long enough to run out\"}", &out));
    try testing.expectEqualStrings("fields", out[0].key);
}

test "the core log filter keeps libraries at warn or quieter" {
    var buf: [256]u8 = undefined;
    try testing.expectStringStartsWith(coreLogFilter(&buf, .debug), "WARN,temporalio_sdk_core=DEBUG,");
    try testing.expectStringStartsWith(coreLogFilter(&buf, .err), "ERROR,temporalio_sdk_core=ERROR,");
}
