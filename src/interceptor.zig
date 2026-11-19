//! Interceptors: code that runs around a workflow's, an activity's, or a client's calls, to
//! read or change their inputs, headers, and results, or to refuse them.
//!
//! Each interceptor is a runtime interface, a pointer and a table of optional methods. A
//! method takes the call's input and `next`, the rest of the chain: it may change the input,
//! call `next`, and change what comes back. A method left null passes the call through.
//! Build an interface from a struct with `from`, which fills in only the methods the struct
//! declares:
//!
//!     const Audit = struct {
//!         ctx: *temporal.workflow.Context,
//!         pub fn executeActivity(a: *Audit, input: interceptor.ActivityInput, next: interceptor.WorkflowOutbound.Next) temporal.workflow.ActivityFuture(temporal.converter.Payload) {
//!             a.ctx.logger().info("scheduling", .{ .ActivityType = input.activity_type });
//!             return next.executeActivity(input);
//!         }
//!     };
//!     const audit = try ctx.allocator().create(Audit);
//!     audit.* = .{ .ctx = ctx };
//!     return .{ .outbound = interceptor.WorkflowOutbound.from(Audit, audit) };
//!
//! Workers take `Worker` interceptors (`worker.Options.interceptors`, after the client's
//! `client.Options.worker_interceptors`). For each workflow run and each activity attempt, a
//! `Worker` interceptor hands out the run's interceptors: `WorkflowInbound` around what
//! reaches the workflow (its start, signals, queries, updates), `WorkflowOutbound` around what
//! the workflow sends (activities, children, signals, signal-with-start, continue-as-new, its
//! logger), and `ActivityInbound` and `ActivityOutbound` likewise for an activity. For each
//! Nexus request the worker handles, it hands out `NexusOperationInbound`, around the start
//! and the cancel of the operation, and `NexusOperationOutbound`, around the handler's logger.
//! A workflow's Nexus calls pass through `WorkflowOutbound.executeNexusOperation`, except a
//! signal-with-start, which passes through `WorkflowOutbound.signalWithStartWorkflow` only.
//! The first interceptor in a list is the outermost.
//!
//! Clients take `Client` interceptors (`client.Options.interceptors`), which wrap every request
//! the client sends to the server (`ClientCall`): they see each operation's method and its
//! typed request and response, change the header of calls that carry one, or refuse a call.
//!
//! Inputs are type-erased, since workflows, activities, and their handlers are typed at
//! compile time: names, `converter.EncodedValues` arguments, a `converter.Header`, and
//! options. Results are encoded payloads.
//!
//! Activity and Nexus interceptors run on the worker's threads, concurrently for different
//! attempts and requests, so a `Worker` interceptor must be thread-safe.
//!
//! Workflow interceptors run inside the workflow: they must follow the workflow rules (no
//! threads, clocks, randomness, or I/O outside `ctx.io()`), and they run again on replay, where
//! they must make the same calls. Query and update-validation interceptors run outside any
//! coroutine, like the handlers they wrap: they must not block or change state.

const std = @import("std");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const log = @import("log.zig");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const nexus = @import("nexus.zig");
const Allocator = std.mem.Allocator;
const Payload = converter.Payload;
const EncodedValues = converter.EncodedValues;
const Header = converter.Header;
const HeaderWriter = converter.HeaderWriter;

// ---------------------------------------------------------------- worker

/// Supplies the interceptors of each workflow run, activity attempt and Nexus request on a
/// worker. Set with
/// `worker.Options.interceptors` or, for every worker on a client,
/// `client.Options.worker_interceptors`. Borrowed: it must outlive the workers.
pub const Worker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Called once per workflow run, when it starts on this worker (or is replayed), before
        /// the workflow function runs. Allocate per-run state with `ctx.allocator()`. Runs as
        /// part of the workflow.
        interceptWorkflow: ?*const fn (ptr: *anyopaque, ctx: *workflow.Context) error{OutOfMemory}!Workflow = null,
        /// Called once per activity attempt, before the activity runs, on the activity's
        /// thread. Allocate per-attempt state with `ctx.allocator()`.
        interceptActivity: ?*const fn (ptr: *anyopaque, ctx: *activity.Context) error{OutOfMemory}!Activity = null,
        /// Called once per Nexus request the worker handles (the start or the cancel of an
        /// operation), before the handler runs, on the request's own task on the worker's
        /// `std.Io`, concurrently for different requests. `ctx.info()` is complete. Allocate
        /// per-request state with `ctx.allocator()`. `error.OutOfMemory` fails the request
        /// with a retryable `internal` handler error.
        interceptNexusOperation: ?*const fn (ptr: *anyopaque, ctx: *nexus.Context) error{OutOfMemory}!NexusOperation = null,
    };

    pub fn interceptWorkflow(w: Worker, ctx: *workflow.Context) error{OutOfMemory}!Workflow {
        const f = w.vtable.interceptWorkflow orelse return .{};
        return f(w.ptr, ctx);
    }

    pub fn interceptActivity(w: Worker, ctx: *activity.Context) error{OutOfMemory}!Activity {
        const f = w.vtable.interceptActivity orelse return .{};
        return f(w.ptr, ctx);
    }

    pub fn interceptNexusOperation(w: Worker, ctx: *nexus.Context) error{OutOfMemory}!NexusOperation {
        const f = w.vtable.interceptNexusOperation orelse return .{};
        return f(w.ptr, ctx);
    }

    /// The `Worker` interceptor of `impl`, from whichever of `interceptWorkflow`,
    /// `interceptActivity` and `interceptNexusOperation` `T` declares, each taking `*T` first.
    pub fn from(comptime T: type, impl: *T) Worker {
        const Erased = struct {
            fn wf(ptr: *anyopaque, ctx: *workflow.Context) error{OutOfMemory}!Workflow {
                return T.interceptWorkflow(@as(*T, @ptrCast(@alignCast(ptr))), ctx);
            }
            fn act(ptr: *anyopaque, ctx: *activity.Context) error{OutOfMemory}!Activity {
                return T.interceptActivity(@as(*T, @ptrCast(@alignCast(ptr))), ctx);
            }
            fn nx(ptr: *anyopaque, ctx: *nexus.Context) error{OutOfMemory}!NexusOperation {
                return T.interceptNexusOperation(@as(*T, @ptrCast(@alignCast(ptr))), ctx);
            }
            const vtable: VTable = .{
                .interceptWorkflow = if (@hasDecl(T, "interceptWorkflow")) wf else null,
                .interceptActivity = if (@hasDecl(T, "interceptActivity")) act else null,
                .interceptNexusOperation = if (@hasDecl(T, "interceptNexusOperation")) nx else null,
            };
        };
        return .{ .ptr = impl, .vtable = &Erased.vtable };
    }
};

/// A workflow run's interceptors; either may be null.
pub const Workflow = struct {
    inbound: ?WorkflowInbound = null,
    outbound: ?WorkflowOutbound = null,
};

/// An activity attempt's interceptors; either may be null.
pub const Activity = struct {
    inbound: ?ActivityInbound = null,
    outbound: ?ActivityOutbound = null,
};

/// A Nexus request's interceptors; either may be null.
pub const NexusOperation = struct {
    inbound: ?NexusOperationInbound = null,
    outbound: ?NexusOperationOutbound = null,
};

// ---------------------------------------------------------------- workflow inbound

/// The workflow function's start. Arguments and header are arena memory of the run.
pub const ExecuteWorkflowInput = struct {
    workflow_type: []const u8,
    args: EncodedValues,
    header: Header,
};

/// A signal, run inside its handler's coroutine. An interceptor that returns without calling
/// `next` drops the signal; an error fails the workflow as a handler's error would.
pub const HandleSignalInput = struct {
    signal_name: []const u8,
    args: EncodedValues,
    header: Header,
};

/// A query, answered outside any coroutine. `arena` holds the answer until it is sent.
pub const HandleQueryInput = struct {
    query_type: []const u8,
    args: EncodedValues,
    header: Header,
    arena: Allocator,
};

/// An update: validated outside any coroutine (`validateUpdate`, skipped on replay), then run
/// in its handler's coroutine (`executeUpdate`). An error from either rejects the update.
pub const UpdateInput = struct {
    update_name: []const u8,
    update_id: []const u8,
    args: EncodedValues,
    header: Header,
};

/// Wraps what reaches a workflow run. See the namespace documentation.
pub const WorkflowInbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        executeWorkflow: ?*const fn (ptr: *anyopaque, input: ExecuteWorkflowInput, next: Next) anyerror!Payload = null,
        handleSignal: ?*const fn (ptr: *anyopaque, input: HandleSignalInput, next: Next) anyerror!void = null,
        handleQuery: ?*const fn (ptr: *anyopaque, input: HandleQueryInput, next: Next) anyerror!Payload = null,
        validateUpdate: ?*const fn (ptr: *anyopaque, input: UpdateInput, next: Next) anyerror!void = null,
        executeUpdate: ?*const fn (ptr: *anyopaque, input: UpdateInput, next: Next) anyerror!Payload = null,
    };

    /// The interceptors after this one, ending in the workflow itself.
    pub const Next = struct {
        chain: []const WorkflowInbound,
        root: WorkflowInbound,

        pub fn executeWorkflow(n: Next, input: ExecuteWorkflowInput) anyerror!Payload {
            return dispatch(WorkflowInbound, "executeWorkflow", n, input);
        }
        pub fn handleSignal(n: Next, input: HandleSignalInput) anyerror!void {
            return dispatch(WorkflowInbound, "handleSignal", n, input);
        }
        pub fn handleQuery(n: Next, input: HandleQueryInput) anyerror!Payload {
            return dispatch(WorkflowInbound, "handleQuery", n, input);
        }
        pub fn validateUpdate(n: Next, input: UpdateInput) anyerror!void {
            return dispatch(WorkflowInbound, "validateUpdate", n, input);
        }
        pub fn executeUpdate(n: Next, input: UpdateInput) anyerror!Payload {
            return dispatch(WorkflowInbound, "executeUpdate", n, input);
        }
    };

    /// The interceptor of `impl`, from the methods of `VTable` that `T` declares, each as
    /// `fn (*T, Input, Next) Result`.
    pub fn from(comptime T: type, impl: *T) WorkflowInbound {
        return build(WorkflowInbound, T, impl);
    }
};

// ---------------------------------------------------------------- workflow outbound

/// An activity the workflow schedules. `header` holds what the propagators wrote; change it
/// in place. Arguments are arena memory of the run.
pub const ActivityInput = struct {
    activity_type: []const u8,
    options: workflow.ActivityOptions,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A local activity the workflow schedules; as `ActivityInput`.
pub const LocalActivityInput = struct {
    activity_type: []const u8,
    options: workflow.LocalActivityOptions,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A child workflow the workflow starts; as `ActivityInput`.
pub const ChildWorkflowInput = struct {
    workflow_type: []const u8,
    options: workflow.ChildWorkflowOptions,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A signal the workflow sends. `child` is set for `ChildWorkflowFuture.signal`, which
/// reaches only a child of this workflow (`run_id` is then empty).
pub const SignalExternalInput = struct {
    signal_name: []const u8,
    workflow_id: []const u8,
    run_id: []const u8,
    child: bool,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// Experimental: a signal-with-start the workflow sends, from `ctx.signalWithStartWorkflow` or
/// `signalWithStartWorkflowByName`. `header` holds what the propagators wrote; change it in
/// place. Its values become the request's header, which the target reads in
/// `ctx.propagated()`. Arguments are arena memory of the run, and a changed `args` or
/// `signal_args` must be `ctx.allocator()` memory. Options are validated after the
/// interceptors.
pub const SignalWithStartInput = struct {
    workflow_type: []const u8,
    signal_name: []const u8,
    options: workflow.SignalWithStartOptions,
    args: EncodedValues,
    signal_args: EncodedValues,
    header: *HeaderWriter,
};

/// A cancellation request the workflow sends to another workflow.
pub const CancelExternalInput = struct {
    workflow_id: []const u8,
    run_id: []const u8,
};

/// The next run the workflow continues as, from `continueAsNew` or `continueAsNewByName`; as
/// `ActivityInput`.
pub const ContinueAsNewInput = struct {
    workflow_type: []const u8,
    options: workflow.ContinueAsNewOptions,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A side effect's value, encoded: null on replay, where the value comes from history.
pub const SideEffectInput = struct {
    value: ?Payload,
};

/// A Nexus operation the workflow calls, from `ctx.executeNexusOperation` or
/// `executeNexusOperationByName`. The endpoint is `options.endpoint` and the call's Nexus
/// headers are `options.headers`. `payload` is the input encoded by the payload converter;
/// the payload codecs run later, as for activity arguments. Options are validated after the
/// interceptors, and the endpoint, names and headers are copied then, so a changed slice need
/// only live for the call; a changed `payload` must be `ctx.allocator()` memory.
pub const NexusOperationInput = struct {
    service: []const u8,
    operation: []const u8,
    options: workflow.NexusOperationOptions,
    payload: Payload,
};

/// A request for the logger workflow, activity or Nexus handler code uses.
pub const LoggerInput = struct {};

/// Wraps what a workflow run sends. See the namespace documentation.
///
/// To refuse an activity, child workflow or Nexus operation, return `rejectActivity`,
/// `rejectChildWorkflow` or `rejectNexusOperation`: the workflow's future fails with the
/// failure given, and nothing is scheduled.
pub const WorkflowOutbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const ContinueAsNewError = error{ OutOfMemory, InvalidSearchAttribute };

    pub const VTable = struct {
        executeActivity: ?*const fn (ptr: *anyopaque, input: ActivityInput, next: Next) workflow.ActivityFuture(Payload) = null,
        executeLocalActivity: ?*const fn (ptr: *anyopaque, input: LocalActivityInput, next: Next) workflow.ActivityFuture(Payload) = null,
        executeChildWorkflow: ?*const fn (ptr: *anyopaque, input: ChildWorkflowInput, next: Next) workflow.ChildWorkflowFuture(Payload) = null,
        signalExternalWorkflow: ?*const fn (ptr: *anyopaque, input: SignalExternalInput, next: Next) workflow.SignalExternalFuture = null,
        cancelExternalWorkflow: ?*const fn (ptr: *anyopaque, input: CancelExternalInput, next: Next) workflow.CancelExternalFuture = null,
        /// Records the request that the workflow function's return of `error.ContinueAsNew`
        /// sends.
        continueAsNew: ?*const fn (ptr: *anyopaque, input: ContinueAsNewInput, next: Next) ContinueAsNewError!void = null,
        sideEffect: ?*const fn (ptr: *anyopaque, input: SideEffectInput, next: Next) workflow.ActivityFuture(Payload) = null,
        /// Schedules a Nexus operation, for `ctx.executeNexusOperation` and
        /// `executeNexusOperationByName`. Cancelling the operation's future
        /// (`NexusOperationFuture.cancel`, or cancelling the coroutine that waits on it) does
        /// not pass through an interceptor, as for activities and child workflows. Changing
        /// the endpoint, service or operation is a nondeterministic change for histories
        /// recorded without it; changing headers is not.
        executeNexusOperation: ?*const fn (ptr: *anyopaque, input: NexusOperationInput, next: Next) workflow.NexusOperationFuture(Payload) = null,
        /// Experimental: sends a signal-with-start, for `ctx.signalWithStartWorkflow` and
        /// `signalWithStartWorkflowByName`. The call goes to the server's system Nexus
        /// endpoint `__temporal_system`, but `executeNexusOperation` is not called for it:
        /// this entry is its only one. The root builds the request from what the
        /// interceptors pass on, so a changed argument, option or header value is what the
        /// server receives.
        ///
        /// A workflow's own `executeNexusOperationByName` call on endpoint
        /// `__temporal_system` is an ordinary Nexus call: it goes through
        /// `executeNexusOperation`, its input is converted by the payload converter and is
        /// not marked as a system payload, so the codecs encode it whole, and propagated
        /// values are added to it as Nexus headers, which the server refuses on that
        /// endpoint (the workflow task fails).
        signalWithStartWorkflow: ?*const fn (ptr: *anyopaque, input: SignalWithStartInput, next: Next) workflow.SignalWithStartFuture = null,
        /// The logger `ctx.logger()` returns, called on every `ctx.logger()`. Add fields with
        /// `with` (cache the result: the run's arena grows with each call).
        logger: ?*const fn (ptr: *anyopaque, input: LoggerInput, next: Next) log.Logger = null,
    };

    /// The interceptors after this one, ending in the workflow's commands.
    pub const Next = struct {
        chain: []const WorkflowOutbound,
        root: WorkflowOutbound,

        pub fn executeActivity(n: Next, input: ActivityInput) workflow.ActivityFuture(Payload) {
            return dispatch(WorkflowOutbound, "executeActivity", n, input);
        }
        pub fn executeLocalActivity(n: Next, input: LocalActivityInput) workflow.ActivityFuture(Payload) {
            return dispatch(WorkflowOutbound, "executeLocalActivity", n, input);
        }
        pub fn executeChildWorkflow(n: Next, input: ChildWorkflowInput) workflow.ChildWorkflowFuture(Payload) {
            return dispatch(WorkflowOutbound, "executeChildWorkflow", n, input);
        }
        pub fn signalExternalWorkflow(n: Next, input: SignalExternalInput) workflow.SignalExternalFuture {
            return dispatch(WorkflowOutbound, "signalExternalWorkflow", n, input);
        }
        pub fn cancelExternalWorkflow(n: Next, input: CancelExternalInput) workflow.CancelExternalFuture {
            return dispatch(WorkflowOutbound, "cancelExternalWorkflow", n, input);
        }
        pub fn continueAsNew(n: Next, input: ContinueAsNewInput) ContinueAsNewError!void {
            return dispatch(WorkflowOutbound, "continueAsNew", n, input);
        }
        pub fn sideEffect(n: Next, input: SideEffectInput) workflow.ActivityFuture(Payload) {
            return dispatch(WorkflowOutbound, "sideEffect", n, input);
        }
        pub fn executeNexusOperation(n: Next, input: NexusOperationInput) workflow.NexusOperationFuture(Payload) {
            return dispatch(WorkflowOutbound, "executeNexusOperation", n, input);
        }
        pub fn signalWithStartWorkflow(n: Next, input: SignalWithStartInput) workflow.SignalWithStartFuture {
            return dispatch(WorkflowOutbound, "signalWithStartWorkflow", n, input);
        }
        pub fn logger(n: Next, input: LoggerInput) log.Logger {
            return dispatch(WorkflowOutbound, "logger", n, input);
        }
    };

    /// The interceptor of `impl`, from the methods of `VTable` that `T` declares, each as
    /// `fn (*T, Input, Next) Result`.
    pub fn from(comptime T: type, impl: *T) WorkflowOutbound {
        return build(WorkflowOutbound, T, impl);
    }
};

/// A future for an activity that is never scheduled: `get` returns `error.ActivityFailed` and
/// `failure` describes `f`, for an outbound interceptor refusing `activity_type`.
pub fn rejectActivity(ctx: *workflow.Context, activity_type: []const u8, f: failure_mod.Application) workflow.ActivityFuture(Payload) {
    return .{ .ctx = ctx, .state = ctx.instance.rejectedActivity(activity_type, f) };
}

/// A future for a child workflow that is never started: `started` and `get` return
/// `error.ChildWorkflowFailed` and `failure` describes `f`, for an outbound interceptor
/// refusing `workflow_type`.
pub fn rejectChildWorkflow(ctx: *workflow.Context, workflow_type: []const u8, f: failure_mod.Application) workflow.ChildWorkflowFuture(Payload) {
    return .{ .ctx = ctx, .state = ctx.instance.rejectedChild(workflow_type, f) };
}

/// A future for a Nexus operation that is never scheduled, for an outbound interceptor
/// refusing `input`: `started` and `get` return `error.NexusOperationFailed`, and `failure`
/// is a Nexus operation failure naming `input`'s endpoint, service and operation whose cause
/// is `f`. `f` is copied, so it may point at the stack. `cancel` on it does nothing.
pub fn rejectNexusOperation(ctx: *workflow.Context, input: NexusOperationInput, f: failure_mod.Application) workflow.NexusOperationFuture(Payload) {
    return .{ .ctx = ctx, .state = ctx.instance.rejectedNexusOperation(input.options.endpoint, input.service, input.operation, f) };
}

// ---------------------------------------------------------------- activities

/// An activity attempt. Arguments and header live as long as the activity's memory.
pub const ExecuteActivityInput = struct {
    activity_type: []const u8,
    args: EncodedValues,
    header: Header,
};

/// A heartbeat's encoded details. Heartbeats the worker drops (a local activity's) don't
/// reach interceptors.
pub const HeartbeatInput = struct {
    details: []const Payload,
};

/// Wraps an activity attempt. See the namespace documentation.
pub const ActivityInbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute: ?*const fn (ptr: *anyopaque, input: ExecuteActivityInput, next: Next) anyerror!Payload = null,
    };

    pub const Next = struct {
        chain: []const ActivityInbound,
        root: ActivityInbound,

        pub fn execute(n: Next, input: ExecuteActivityInput) anyerror!Payload {
            return dispatch(ActivityInbound, "execute", n, input);
        }
    };

    pub fn from(comptime T: type, impl: *T) ActivityInbound {
        return build(ActivityInbound, T, impl);
    }
};

/// Wraps what an activity attempt sends. See the namespace documentation.
pub const ActivityOutbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// The logger `ctx.logger()` returns, called on every `ctx.logger()`.
        logger: ?*const fn (ptr: *anyopaque, input: LoggerInput, next: Next) log.Logger = null,
        heartbeat: ?*const fn (ptr: *anyopaque, input: HeartbeatInput, next: Next) void = null,
    };

    pub const Next = struct {
        chain: []const ActivityOutbound,
        root: ActivityOutbound,

        pub fn logger(n: Next, input: LoggerInput) log.Logger {
            return dispatch(ActivityOutbound, "logger", n, input);
        }
        pub fn heartbeat(n: Next, input: HeartbeatInput) void {
            return dispatch(ActivityOutbound, "heartbeat", n, input);
        }
    };

    pub fn from(comptime T: type, impl: *T) ActivityOutbound {
        return build(ActivityOutbound, T, impl);
    }
};

// ---------------------------------------------------------------- Nexus operations

/// The start of a Nexus operation on the handling worker. `payload` is the request's input,
/// still encoded by the caller's codecs, null when the request carries none; `headers` are
/// the request's Nexus headers.
pub const StartNexusOperationInput = struct {
    service: []const u8,
    operation: []const u8,
    payload: ?Payload,
    headers: []const nexus.Header,
};

/// The cancel of a Nexus operation on the handling worker; `token` is the operation token
/// its start returned.
pub const CancelNexusOperationInput = struct {
    service: []const u8,
    operation: []const u8,
    token: []const u8,
};

/// Wraps a Nexus request a worker handles: the start or the cancel of an operation, of every
/// kind of handler. The chain ends in looking up the registered operation by the input's
/// service and operation (a `not_found` handler error when there is none) and running it, so
/// a change to the input is what the handler runs with and what `ctx.info()` reports.
///
/// An interceptor may change the input before calling `next`, change what `next` returns,
/// or refuse the request by returning `.{ .handler_error = ... }` without calling `next`.
/// It runs on the request's task on the worker's `std.Io`, concurrently with other requests,
/// may do I/O, and sees a cancel of the request as `std.Io` cancellation, like the handler.
/// Every slice in an input it changes or a result it builds must live until the response has
/// been sent, as `ctx.allocator()` memory does; a payload it substitutes (input or
/// `StartResult.succeeded.result`) must be in wire form, through the client's codecs.
/// Requests the worker fails before a handler task exists don't reach it.
pub const NexusOperationInbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        startOperation: ?*const fn (ptr: *anyopaque, input: StartNexusOperationInput, next: Next) nexus.StartResult = null,
        cancelOperation: ?*const fn (ptr: *anyopaque, input: CancelNexusOperationInput, next: Next) nexus.CancelResult = null,
    };

    /// The interceptors after this one, ending in the registered operation handler.
    pub const Next = struct {
        chain: []const NexusOperationInbound,
        root: NexusOperationInbound,

        pub fn startOperation(n: Next, input: StartNexusOperationInput) nexus.StartResult {
            return dispatch(NexusOperationInbound, "startOperation", n, input);
        }
        pub fn cancelOperation(n: Next, input: CancelNexusOperationInput) nexus.CancelResult {
            return dispatch(NexusOperationInbound, "cancelOperation", n, input);
        }
    };

    /// The interceptor of `impl`, from the methods of `VTable` that `T` declares, each as
    /// `fn (*T, Input, Next) Result`.
    pub fn from(comptime T: type, impl: *T) NexusOperationInbound {
        return build(NexusOperationInbound, T, impl);
    }
};

/// Wraps what a Nexus handler uses from its context: the logger.
pub const NexusOperationOutbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// The logger the handler's `ctx.logger()` returns, called on every `ctx.logger()`.
        /// The worker's own logging of the request's outcome doesn't pass through it.
        logger: ?*const fn (ptr: *anyopaque, input: LoggerInput, next: Next) log.Logger = null,
    };

    pub const Next = struct {
        chain: []const NexusOperationOutbound,
        root: NexusOperationOutbound,

        pub fn logger(n: Next, input: LoggerInput) log.Logger {
            return dispatch(NexusOperationOutbound, "logger", n, input);
        }
    };

    pub fn from(comptime T: type, impl: *T) NexusOperationOutbound {
        return build(NexusOperationOutbound, T, impl);
    }
};

// ---------------------------------------------------------------- client

/// What a client call can fail with; an interceptor refuses a call by returning one of these
/// (`error.RpcFailed` is reported as the server refusing it). See the client's operations
/// for what each means to its caller.
pub const ClientError = error{ OutOfMemory, Canceled, RpcFailed, AlreadyExists, NotFound, InvalidResponse, CodecFailed };

/// One request a client sends to the server. Every client operation is made of these: one
/// per operation (start, signal, query, describe, ...), or several for one that pages (list,
/// history) or long-polls (a workflow's result, an update's outcome).
pub const ClientCall = struct {
    /// The WorkflowService method, such as "StartWorkflowExecution" or "QueryWorkflow".
    method: []const u8,
    /// The request's header, for the methods whose request carries one
    /// (StartWorkflowExecution, SignalWorkflowExecution, SignalWithStartWorkflowExecution,
    /// QueryWorkflow, UpdateWorkflowExecution, StartActivityExecution), holding what the
    /// client's propagators wrote; change it in place. Null for other methods (an
    /// ExecuteMultiOperation request carries a header in each operation: change those through
    /// `request`).
    header: ?*HeaderWriter,
    /// Memory that lives until the operation returns, for anything an interceptor adds.
    arena: Allocator,
    request_ptr: *anyopaque,
    request_type: []const u8,
    response_ptr: *anyopaque,
    response_type: []const u8,

    /// The request as its generated message type, such as
    /// `proto.temporal.api.workflowservice.v1.StartWorkflowExecutionRequest`, or null if it
    /// is another type. Changes are sent; the header is replaced by `header`'s fields.
    pub fn request(c: *const ClientCall, comptime T: type) ?*T {
        if (!std.mem.eql(u8, c.request_type, @typeName(T))) return null;
        return @ptrCast(@alignCast(c.request_ptr));
    }

    /// The response as its generated message type, or null if it is another type. Filled in
    /// once `next` returns; an interceptor that answers without calling `next` fills it in.
    pub fn response(c: *const ClientCall, comptime T: type) ?*T {
        if (!std.mem.eql(u8, c.response_type, @typeName(T))) return null;
        return @ptrCast(@alignCast(c.response_ptr));
    }
};

/// Wraps every request a client sends (`client.Options.interceptors`). Runs on whichever
/// thread makes the call, so it must be thread-safe.
pub const Client = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        call: ?*const fn (ptr: *anyopaque, input: *ClientCall, next: Next) ClientError!void = null,
    };

    /// The interceptors after this one, ending in the request sent to the server.
    pub const Next = struct {
        chain: []const Client,
        root: Client,

        pub fn call(n: Next, input: *ClientCall) ClientError!void {
            return dispatch(Client, "call", n, input);
        }
    };

    /// The interceptor of `impl`, from `call` if `T` declares it as
    /// `fn (*T, *ClientCall, Next) ClientError!void`.
    pub fn from(comptime T: type, impl: *T) Client {
        return build(Client, T, impl);
    }
};

// ---------------------------------------------------------------- chains

fn MethodFn(comptime I: type, comptime method: []const u8) type {
    return @typeInfo(@typeInfo(@FieldType(I.VTable, method)).optional.child).pointer.child;
}

fn Return(comptime I: type, comptime method: []const u8) type {
    return @typeInfo(MethodFn(I, method)).@"fn".return_type.?;
}

/// Calls `method` on the first interceptor of `n.chain` that has it, handing it the rest of
/// the chain, or on `n.root` when none does.
fn dispatch(comptime I: type, comptime method: []const u8, n: I.Next, input: anytype) Return(I, method) {
    for (n.chain, 0..) |ic, i| {
        if (@field(ic.vtable, method)) |f| return f(ic.ptr, input, .{ .chain = n.chain[i + 1 ..], .root = n.root });
    }
    return @field(n.root.vtable, method).?(n.root.ptr, input, .{ .chain = &.{}, .root = n.root });
}

/// The interface `I` of `impl`, with a vtable entry for each method of `I.VTable` that `T`
/// declares and null for the rest.
fn build(comptime I: type, comptime T: type, impl: *T) I {
    const Erased = struct {
        const vtable: I.VTable = blk: {
            var vt: I.VTable = .{};
            for (@typeInfo(I.VTable).@"struct".field_names) |name| {
                if (@hasDecl(T, name)) @field(vt, name) = &Method(I, T, name).call;
            }
            break :blk vt;
        };
    };
    return .{ .ptr = impl, .vtable = &Erased.vtable };
}

fn Method(comptime I: type, comptime T: type, comptime name: []const u8) type {
    const info = @typeInfo(MethodFn(I, name)).@"fn";
    const Input = info.param_types[1].?;
    const R = info.return_type.?;
    return struct {
        fn call(ptr: *anyopaque, input: Input, next: I.Next) R {
            const self: *T = @ptrCast(@alignCast(ptr));
            return @field(T, name)(self, input, next);
        }
    };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "from fills in only the methods a type declares, and calls pass through the rest" {
    const Counting = struct {
        signals: usize = 0,
        pub fn handleSignal(c: *@This(), input: HandleSignalInput, next: WorkflowInbound.Next) anyerror!void {
            c.signals += 1;
            return next.handleSignal(input);
        }
    };
    const Root = struct {
        queries: usize = 0,
        signals: usize = 0,
        fn signal(ptr: *anyopaque, _: HandleSignalInput, _: WorkflowInbound.Next) anyerror!void {
            const r: *@This() = @ptrCast(@alignCast(ptr));
            r.signals += 1;
        }
        fn query(ptr: *anyopaque, input: HandleQueryInput, _: WorkflowInbound.Next) anyerror!Payload {
            const r: *@This() = @ptrCast(@alignCast(ptr));
            r.queries += 1;
            return converter.toPayload(input.arena, r.queries);
        }
    };

    var first: Counting = .{};
    var second: Counting = .{};
    const a = WorkflowInbound.from(Counting, &first);
    try testing.expect(a.vtable.handleSignal != null);
    try testing.expect(a.vtable.executeWorkflow == null);
    try testing.expect(a.vtable.handleQuery == null);
    try testing.expect(a.vtable.validateUpdate == null);
    try testing.expect(a.vtable.executeUpdate == null);

    var root: Root = .{};
    const root_vtable: WorkflowInbound.VTable = .{ .handleSignal = Root.signal, .handleQuery = Root.query };
    const chain = [_]WorkflowInbound{ a, WorkflowInbound.from(Counting, &second) };
    const next: WorkflowInbound.Next = .{ .chain = &chain, .root = .{ .ptr = &root, .vtable = &root_vtable } };

    try next.handleSignal(.{ .signal_name = "s", .args = .{ .payloads = &.{} }, .header = .{} });
    try testing.expectEqual(@as(usize, 1), first.signals);
    try testing.expectEqual(@as(usize, 1), second.signals);
    try testing.expectEqual(@as(usize, 1), root.signals);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const answer = try next.handleQuery(.{ .query_type = "q", .args = .{ .payloads = &.{} }, .header = .{}, .arena = arena.allocator() });
    try testing.expectEqualStrings("1", answer.data);

    const W = struct {
        pub fn interceptActivity(_: *@This(), _: *activity.Context) error{OutOfMemory}!Activity {
            return .{};
        }
    };
    var w: W = .{};
    const worker = Worker.from(W, &w);
    try testing.expect(worker.vtable.interceptWorkflow == null);
    try testing.expect(worker.vtable.interceptActivity != null);
}
