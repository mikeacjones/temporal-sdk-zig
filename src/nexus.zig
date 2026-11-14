//! Nexus: services of operations that a worker handles and workflows call through a Nexus
//! endpoint, often from another namespace.
//!
//! A service's contract holds only names and types, so the calling program imports it
//! without the handlers:
//!
//!     pub const Echo = temporal.nexus.OperationDef("echo", EchoInput, EchoOutput);
//!     pub const SayHello = temporal.nexus.OperationDef("say-hello", HelloInput, HelloOutput);
//!     pub const HelloService = temporal.nexus.ServiceDef("my-hello-service", .{ Echo, SayHello });
//!
//! The handling worker binds every operation to a handler. A synchronous handler returns the
//! operation's output; a workflow-run handler returns the workflow to start, and the
//! operation completes when that workflow closes, with its result:
//!
//!     fn echo(ctx: *temporal.nexus.Context, input: EchoInput) !EchoOutput {
//!         _ = ctx;
//!         return .{ .message = input.message };
//!     }
//!     fn sayHello(ctx: *temporal.nexus.Context, input: HelloInput) !temporal.nexus.WorkflowStart(HelloWorkflow) {
//!         return .{ .options = .{ .id = ctx.info().request_id }, .args = .{input} };
//!     }
//!     pub const HelloHandler = temporal.nexus.ServiceHandler(HelloService, .{
//!         temporal.nexus.OperationHandler(Echo, echo),
//!         temporal.nexus.OperationHandler(SayHello, sayHello),
//!     });
//!     // worker: try w.registerNexusService(HelloHandler);
//!
//! Handlers run on the worker's real `std.Io`, like activities, and may do any I/O; memory
//! from `ctx.allocator()` lives until the operation's response has been sent. A handler fails
//! the operation with `ctx.fail` or `ctx.failCanceled`, or fails the request with
//! `ctx.handlerError`; any other error it returns becomes a handler error as `Context`
//! describes.

const std = @import("std");
const converter = @import("converter.zig");
const client_mod = @import("client.zig");
const failure_mod = @import("failure.zig");
const log = @import("log.zig");
const metrics_mod = @import("metrics.zig");
const proto = @import("proto.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Payload = converter.Payload;

/// A link between Temporal executions and events, such as from the workflow that scheduled
/// an operation to the workflow the operation started.
pub const Link = proto.temporal.api.common.v1.Link;

/// A Nexus request or callback header. Names are case-insensitive.
pub const Header = client_mod.NexusHeader;

// ---------------------------------------------------------------- contract

/// Names an operation and its input and output types; either may be `void`. Both the
/// handling worker (`OperationHandler`) and calling workflows use the result, so both sides
/// are checked against one definition. An empty name, or an input or output type the payload
/// converter can't encode and decode, is a compile error naming the operation.
pub fn OperationDef(comptime name: []const u8, comptime In: type, comptime Out: type) type {
    if (comptime operationDefError(name, In, Out)) |message| @compileError(message);
    return struct {
        pub const operation_name = name;
        pub const Input = In;
        pub const Output = Out;
    };
}

fn operationDefError(comptime name: []const u8, comptime In: type, comptime Out: type) ?[]const u8 {
    const what = "nexus.OperationDef(\"" ++ name ++ "\", " ++ @typeName(In) ++ ", " ++ @typeName(Out) ++ ")";
    if (name.len == 0) return what ++ ": the operation name is empty; name the operation";
    for ([_]type{ In, Out }, [_][]const u8{ "input", "output" }) |T, role| {
        for ([_]converter.Direction{ .encode, .decode }) |direction| {
            if (converter.unconvertibleType(T, direction)) |bad|
                return what ++ ": the " ++ role ++ " type " ++ @typeName(T) ++ " can't be " ++ @tagName(direction) ++ "d by the payload converter (because of " ++ @typeName(bad) ++ ")";
        }
    }
    return null;
}

/// Names a service and its operations: `operations` is a tuple of `OperationDef` types,
/// such as `.{ Echo, SayHello }`. An empty name, no operations, something other than an
/// `OperationDef` in the tuple, or two operations with the same name is a compile error
/// naming the service and the operation.
pub fn ServiceDef(comptime name: []const u8, comptime operations: anytype) type {
    if (comptime serviceDefError(name, operations)) |message| @compileError(message);
    const list = comptime list: {
        var types: [operations.len]type = undefined;
        for (&types, 0..) |*t, i| t.* = operations[i];
        break :list types;
    };
    return struct {
        /// The service's name on the wire.
        pub const service_name = name;
        /// The service's `OperationDef` types, in the order given.
        pub const operation_defs: []const type = &list;

        /// Whether `Op` is one of the service's operations: an `OperationDef` with one of its
        /// names and the same input and output types.
        pub fn hasOperation(comptime Op: type) bool {
            const D = operationNamed(Op.operation_name) orelse return false;
            return D.Input == Op.Input and D.Output == Op.Output;
        }

        /// The service's operation called `op_name`, or null.
        pub fn operationNamed(comptime op_name: []const u8) ?type {
            inline for (list) |D| if (comptime std.mem.eql(u8, D.operation_name, op_name)) return D;
            return null;
        }
    };
}

fn serviceDefError(comptime name: []const u8, comptime operations: anytype) ?[]const u8 {
    const what = "nexus service \"" ++ name ++ "\"";
    if (name.len == 0) return "nexus.ServiceDef: the service name is empty; name the service";
    const Ops = @TypeOf(operations);
    const info = switch (@typeInfo(Ops)) {
        .@"struct" => |s| s,
        else => return what ++ ": ServiceDef takes a tuple of nexus.OperationDef types, such as .{ Echo, SayHello }, not " ++ @typeName(Ops),
    };
    if (!info.is_tuple) return what ++ ": ServiceDef takes a tuple of nexus.OperationDef types, such as .{ Echo, SayHello }, not " ++ @typeName(Ops);
    if (info.field_types.len == 0) return what ++ " has no operations; list its nexus.OperationDef types, such as .{ Echo, SayHello }";
    for (info.field_types, 0..) |F, i| {
        if (F != type or !isOperationDef(operations[i]))
            return std.fmt.comptimePrint("{s}: element {d} of the operations tuple is not a nexus.OperationDef type", .{ what, i });
        const op = operations[i].operation_name;
        for (0..i) |j| if (std.mem.eql(u8, operations[j].operation_name, op))
            return what ++ " has two operations named \"" ++ op ++ "\"; operation names are unique within a service";
    }
    return null;
}

fn isOperationDef(comptime T: type) bool {
    if (@typeInfo(T) != .@"struct") return false;
    return @hasDecl(T, "operation_name") and @hasDecl(T, "Input") and @hasDecl(T, "Output");
}

fn isServiceDef(comptime T: type) bool {
    if (@typeInfo(T) != .@"struct") return false;
    return @hasDecl(T, "service_name") and @hasDecl(T, "operation_defs");
}

// ---------------------------------------------------------------- handlers

/// The workflow a workflow-run operation starts: a handler of operation `Op` returns
/// `WorkflowStart(W)` (or an error union of it), where `W` is a `workflow.Def` whose result
/// type is `Op.Output`. The operation completes when the workflow closes, with its result.
///
/// The workflow is started through the worker's client with `options` and `args`, carrying
/// the Nexus request ID as the start's request ID, the caller's completion callback, and the
/// caller's links; a retried start with the same request ID finds the workflow the first one
/// started. An empty `options.id` uses the request ID, and an empty `options.task_queue` the
/// worker's task queue. With `options.id_conflict_policy = .use_existing`, a start that finds
/// the workflow running attaches the operation to it instead of failing.
/// `options.enable_eager_start` and `options.error_when_already_started` are ignored.
/// Strings in `options` and `args` must live until the handler's response has been sent, as
/// `ctx.allocator()` memory does.
pub fn WorkflowStart(comptime W: type) type {
    if (@typeInfo(W) != .@"struct" or !@hasDecl(W, "workflow_name") or !@hasDecl(W, "Args") or !@hasDecl(W, "Result"))
        @compileError("nexus.WorkflowStart takes a temporal.workflow.Def, not " ++ @typeName(W));
    return struct {
        pub const Workflow = W;
        options: client_mod.StartWorkflowOptions = .{},
        args: W.Args,
    };
}

fn isWorkflowStart(comptime T: type) bool {
    if (@typeInfo(T) != .@"struct" or !@hasDecl(T, "Workflow")) return false;
    if (@TypeOf(T.Workflow) != type) return false;
    const W = T.Workflow;
    if (@typeInfo(W) != .@"struct" or !@hasDecl(W, "workflow_name") or !@hasDecl(W, "Args") or !@hasDecl(W, "Result")) return false;
    return T == WorkflowStart(W);
}

/// Binds operation `Op` (an `OperationDef`) to `func`, for `ServiceHandler`. `func` is one of
///
///     fn (ctx: *nexus.Context, input: Op.Input) !Op.Output             // synchronous
///     fn (ctx: *nexus.Context, input: Op.Input) !nexus.WorkflowStart(W) // workflow-run
///
/// or the same with a receiver first, `fn (self: *T, ctx: *nexus.Context, input: Op.Input)`,
/// for handlers that share state; `ServiceHandler` binds them to one instance. For an
/// operation whose input is `void`, the input parameter may be left out. The return type may
/// be an error union or not. `ServiceHandler` checks the shape and reports a mismatch as a
/// compile error naming the service and the operation.
pub fn OperationHandler(comptime Op: type, comptime func: anytype) type {
    return struct {
        pub const Operation = Op;
        pub const function = func;
    };
}

fn isOperationHandler(comptime T: type) bool {
    if (@typeInfo(T) != .@"struct") return false;
    return @hasDecl(T, "Operation") and @hasDecl(T, "function") and @TypeOf(T.Operation) == type and T == OperationHandler(T.Operation, T.function);
}

/// What a handler function looks like, worked out from its type.
const Shape = struct {
    bound: bool,
    takes_input: bool,
    Receiver: type,
    /// The handler's return type, and the type of its value without the error union.
    Return: type,
    Value: type,
    runs_workflow: bool,
};

/// The shape of `func` as a handler of `Op` in `service`, or a message saying why it isn't
/// one.
fn handlerShape(comptime service: []const u8, comptime Op: type, comptime func: anytype) union(enum) { ok: Shape, bad: []const u8 } {
    const what = "nexus service \"" ++ service ++ "\" operation \"" ++ Op.operation_name ++ "\"";
    const expected = "fn (ctx: *nexus.Context, input: " ++ @typeName(Op.Input) ++ ") !" ++ @typeName(Op.Output) ++
        " or !nexus.WorkflowStart(W), optionally with a receiver pointer first";
    const F = @TypeOf(func);
    const f = switch (@typeInfo(F)) {
        .@"fn" => |f| f,
        else => return .{ .bad = what ++ ": the handler must be a function " ++ expected ++ ", not a " ++ @typeName(F) },
    };
    if (f.is_generic or f.return_type == null) return .{ .bad = what ++ ": the handler must have concrete parameter and return types: " ++ expected };
    const params = f.param_types;
    const bound = params.len >= 2 and params[0] != *Context and params[1] == *Context;
    if (!bound and (params.len == 0 or params[0] != *Context))
        return .{ .bad = what ++ ": the handler must take *nexus.Context first, or a receiver pointer and then *nexus.Context: " ++ expected };
    var Receiver: type = void;
    if (bound) switch (@typeInfo(params[0].?)) {
        .pointer => |p| if (p.size == .one and @typeInfo(p.child) == .@"struct") {
            Receiver = p.child;
        } else return .{ .bad = what ++ ": the handler's receiver must be a pointer to a struct, not " ++ @typeName(params[0].?) },
        else => return .{ .bad = what ++ ": the handler's receiver must be a pointer to a struct, not " ++ @typeName(params[0].?) },
    };
    const rest = params[if (bound) 2 else 1..];
    if (rest.len > 1) return .{ .bad = what ++ ": the handler takes one input parameter, of type " ++ @typeName(Op.Input) ++ "; " ++ expected };
    if (rest.len == 0 and Op.Input != void)
        return .{ .bad = what ++ ": the handler takes no input, but the operation's input type is " ++ @typeName(Op.Input) ++ "; " ++ expected };
    if (rest.len == 1 and rest[0].? != Op.Input)
        return .{ .bad = what ++ ": the handler's input type " ++ @typeName(rest[0].?) ++ " is not the operation's input type " ++ @typeName(Op.Input) };
    const Return = f.return_type.?;
    const Value = switch (@typeInfo(Return)) {
        .error_union => |eu| eu.payload,
        else => Return,
    };
    const runs_workflow = isWorkflowStart(Value);
    if (runs_workflow) {
        const W = Value.Workflow;
        if (W.Result != Op.Output)
            return .{ .bad = what ++ ": the handler starts workflow \"" ++ W.workflow_name ++ "\", whose result type " ++ @typeName(W.Result) ++ " is not the operation's output type " ++ @typeName(Op.Output) };
    } else if (Value != Op.Output) {
        return .{ .bad = what ++ ": the handler returns " ++ @typeName(Value) ++ ", not the operation's output type " ++ @typeName(Op.Output) ++ " or a nexus.WorkflowStart of a workflow with that result" };
    }
    return .{ .ok = .{ .bound = bound, .takes_input = rest.len == 1, .Receiver = Receiver, .Return = Return, .Value = Value, .runs_workflow = runs_workflow } };
}

/// Binds every operation of `Service` (a `ServiceDef`) to a handler: `handlers` is a tuple
/// of `OperationHandler` types, one per operation, in any order. The result is a type that
/// workers register with `Worker.registerNexusService`, or with
/// `Worker.registerNexusServiceWith` when handlers take a receiver.
///
/// Compile errors, each naming the service and the operation: an operation with no handler,
/// two handlers for one operation, a handler for an operation the service doesn't have (or
/// with other input or output types than the service's operation of that name), a handler
/// whose function doesn't fit its operation (see `OperationHandler`), and handlers taking
/// receivers of different types.
pub fn ServiceHandler(comptime Service: type, comptime handlers: anytype) type {
    if (comptime serviceHandlerError(Service, handlers)) |message| @compileError(message);
    const service = Service.service_name;
    const ops = comptime ops: {
        var list: [Service.operation_defs.len]Operation = undefined;
        for (&list, Service.operation_defs) |*entry, Op| {
            const H = handlerFor(handlers, Op.operation_name);
            const Impl = Implementation(service, Op, H.function);
            entry.* = .{ .name = Op.operation_name, .kind = Impl.kind, .start = &Impl.start, .cancel = &Impl.cancel };
        }
        break :ops list;
    };
    const R = comptime receiverOf(Service, handlers);
    return struct {
        pub const ServiceDefinition = Service;
        pub const service_name = service;
        /// The type that handlers taking a receiver are bound to; `void` when none does.
        pub const Receiver = R;
        /// The service's operations, in the order of `Service`.
        pub const operations: []const Operation = &ops;

        /// The registration when no handler takes a receiver. Otherwise use `with`.
        pub const registered: Registered = if (R != void)
            @compileError("nexus service \"" ++ service ++ "\" has handlers taking *" ++ @typeName(R) ++ "; register it with registerNexusServiceWith and an instance")
        else
            .{ .name = service, .operations = operations };

        /// The registration binding the handlers that take a receiver to `receiver`, which
        /// must outlive the worker and be safe to use from several handlers at once.
        pub fn with(receiver: *Receiver) Registered {
            return .{ .name = service, .operations = operations, .receiver = receiver };
        }
    };
}

fn handlerFor(comptime handlers: anytype, comptime op_name: []const u8) type {
    for (0..handlers.len) |i| if (std.mem.eql(u8, handlers[i].Operation.operation_name, op_name)) return handlers[i];
    unreachable;
}

fn receiverOf(comptime Service: type, comptime handlers: anytype) type {
    var R: type = void;
    for (0..handlers.len) |i| switch (handlerShape(Service.service_name, handlers[i].Operation, handlers[i].function)) {
        .ok => |s| if (s.bound) {
            R = s.Receiver;
        },
        .bad => unreachable,
    };
    return R;
}

fn serviceHandlerError(comptime Service: type, comptime handlers: anytype) ?[]const u8 {
    if (!isServiceDef(Service)) return "nexus.ServiceHandler takes a nexus.ServiceDef, not " ++ @typeName(Service);
    const service = Service.service_name;
    const what = "nexus service \"" ++ service ++ "\"";
    const H = @TypeOf(handlers);
    const tuple = switch (@typeInfo(H)) {
        .@"struct" => |s| s.is_tuple,
        else => false,
    };
    if (!tuple) return what ++ ": ServiceHandler takes a tuple of nexus.OperationHandler types, not " ++ @typeName(H);
    var receiver: ?struct { type, []const u8 } = null;
    for (@typeInfo(H).@"struct".field_types, 0..) |F, i| {
        if (F != type or !isOperationHandler(handlers[i]))
            return std.fmt.comptimePrint("{s}: element {d} of the handlers tuple is not a nexus.OperationHandler(Op, function) type", .{ what, i });
        const Op = handlers[i].Operation;
        if (!isOperationDef(Op))
            return std.fmt.comptimePrint("{s}: handler {d} names {s}, not a nexus.OperationDef", .{ what, i, @typeName(Op) });
        const op = Op.operation_name;
        const op_what = what ++ " operation \"" ++ op ++ "\"";
        const D = Service.operationNamed(op) orelse {
            var names: []const u8 = "";
            for (Service.operation_defs, 0..) |S, k| names = names ++ (if (k > 0) ", " else "") ++ "\"" ++ S.operation_name ++ "\"";
            return what ++ " has no operation \"" ++ op ++ "\", but a handler is bound to it; its operations are " ++ names;
        };
        if (D.Input != Op.Input or D.Output != Op.Output)
            return op_what ++ " takes " ++ @typeName(D.Input) ++ " and returns " ++ @typeName(D.Output) ++ ", but the handler is bound to an OperationDef of that name taking " ++ @typeName(Op.Input) ++ " and returning " ++ @typeName(Op.Output);
        for (0..i) |j| if (std.mem.eql(u8, handlers[j].Operation.operation_name, op))
            return op_what ++ " has two handlers; bind each operation once";
        switch (handlerShape(service, Op, handlers[i].function)) {
            .bad => |message| return message,
            .ok => |s| if (s.bound) {
                if (receiver) |r| if (r[0] != s.Receiver)
                    return what ++ ": operation \"" ++ r[1] ++ "\"'s handler takes *" ++ @typeName(r[0]) ++ " and operation \"" ++ op ++ "\"'s takes *" ++ @typeName(s.Receiver) ++ "; one service handler binds one receiver";
                receiver = .{ s.Receiver, op };
            },
        }
    }
    for (Service.operation_defs) |D| {
        const bound = for (0..handlers.len) |i| {
            if (std.mem.eql(u8, handlers[i].Operation.operation_name, D.operation_name)) break true;
        } else false;
        if (!bound)
            return what ++ " operation \"" ++ D.operation_name ++ "\" has no handler; add nexus.OperationHandler(" ++ @typeName(D) ++ ", function) to the handlers";
    }
    return null;
}

// ---------------------------------------------------------------- failures

/// The kinds of handler error, with their names on the wire.
pub const HandlerErrorType = enum {
    /// The request is malformed: its input doesn't decode, or its token doesn't parse.
    bad_request,
    unauthenticated,
    unauthorized,
    /// The service, operation, or the operation's target doesn't exist.
    not_found,
    request_timeout,
    conflict,
    resource_exhausted,
    /// The handler failed; the default for errors a handler returns.
    internal,
    not_implemented,
    unavailable,
    upstream_timeout,

    /// The name on the wire, such as "BAD_REQUEST".
    pub fn wireName(t: HandlerErrorType) []const u8 {
        return switch (t) {
            inline else => |tag| comptime upper: {
                const name = @tagName(tag);
                var buf: [name.len]u8 = undefined;
                _ = std.ascii.upperString(&buf, name);
                const final = buf;
                break :upper &final;
            },
        };
    }

    /// The type named `name` on the wire, or null for a name this SDK doesn't know.
    pub fn fromWireName(name: []const u8) ?HandlerErrorType {
        inline for (@typeInfo(HandlerErrorType).@"enum".field_names) |field| {
            const t = @field(HandlerErrorType, field);
            if (std.mem.eql(u8, t.wireName(), name)) return t;
        }
        return null;
    }

    /// Whether the caller retries a handler error of this type that doesn't say otherwise:
    /// true for request_timeout, resource_exhausted, internal, unavailable, and
    /// upstream_timeout.
    pub fn retryableByDefault(t: HandlerErrorType) bool {
        return switch (t) {
            .request_timeout, .resource_exhausted, .internal, .unavailable, .upstream_timeout => true,
            .bad_request, .unauthenticated, .unauthorized, .not_found, .conflict, .not_implemented => false,
        };
    }
};

/// A failed request: the handler couldn't run the operation, as opposed to an operation that
/// ran and failed (`OperationError`). The caller retries it unless it is non-retryable.
pub const HandlerError = struct {
    /// What kind of error it is, which also decides whether it is retried by default.
    type: HandlerErrorType,
    /// What went wrong, for the caller's failure.
    message: []const u8,
    /// Overrides the type's default retry behavior.
    retry: Retry = .default,
    /// The failure behind the error, if any, which the caller sees as its cause.
    cause: ?failure_mod.Application = null,

    /// Whether the caller retries the request.
    pub const Retry = enum {
        /// As `HandlerErrorType.retryableByDefault` says for the type.
        default,
        retryable,
        non_retryable,
    };

    /// Whether the caller retries the request.
    pub fn retryable(e: HandlerError) bool {
        return switch (e.retry) {
            .default => e.type.retryableByDefault(),
            .retryable => true,
            .non_retryable => false,
        };
    }
};

/// An operation that ran and ended unsuccessfully. The caller sees the operation failed or
/// canceled and doesn't retry it.
pub const OperationError = struct {
    state: State,
    /// The message, and for a failed operation the type and details the caller sees on the
    /// failure's cause.
    failure: failure_mod.Application,

    /// How the operation ended, as the caller sees it.
    pub const State = enum { failed, canceled };
};

// ---------------------------------------------------------------- context

/// What a handler knows about the request it handles. Borrowed from the task: valid as long
/// as `ctx.allocator()` memory.
pub const Info = struct {
    service: []const u8,
    operation: []const u8,
    /// Identifies the start request; the caller sends the same ID when it retries a start.
    /// Empty for a cancel.
    request_id: []const u8 = "",
    /// The request's Nexus headers.
    headers: []const Header = &.{},
    /// Links to what scheduled the operation, such as the caller workflow's scheduling event.
    /// Empty for a cancel.
    links: []const Link = &.{},
    /// Where the server delivers an asynchronous operation's completion, and the headers it
    /// sends; empty for a cancel or a caller that takes no completion.
    callback_url: []const u8 = "",
    callback_headers: []const Header = &.{},
    /// The endpoint the caller called the operation through.
    endpoint: []const u8 = "",
    namespace: []const u8 = "",
    task_queue: []const u8 = "",
    /// When the caller stops waiting for this request, if it said.
    request_deadline: ?Io.Timestamp = null,

    /// The value of the request header `name` (compared case-insensitively), or null.
    pub fn header(i: *const Info, name: []const u8) ?[]const u8 {
        for (i.headers) |h| if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        return null;
    }
};

/// Why a handler was canceled.
pub const CancelReason = enum(u8) {
    /// The request's deadline passed.
    timed_out,
    /// The worker is shutting down.
    worker_shutdown,
};

/// A handler's context. The worker builds one per request: `arena`, `io_`, and `info_` are
/// required, the rest default to "absent". `arena` must live until the request's response
/// has been sent: the operation's results and failures are allocated from it, and `info_`
/// borrows memory that lives as long. The handler uses the context from its own task;
/// `cancelReason` and `setCancelReason` are safe from any thread.
///
/// A handler fails in one of three ways:
/// - `ctx.fail(...)`: the operation failed. Not retried.
/// - `ctx.failCanceled(...)`: the operation was canceled. Not retried.
/// - `ctx.handlerError(...)`: the request failed with a handler error type, retried or not as
///   the type and `retry` say.
///
/// Any other error the handler returns is a handler error of type `internal` (retried) whose
/// cause is an application failure named after the error, except: `error.NotFound` (as the
/// client returns it for a workflow that doesn't exist) is `not_found`, and
/// `error.WorkflowExecutionAlreadyStarted`, `error.InvalidOptions` and
/// `error.InvalidSearchAttribute` are a non-retryable `internal`. Errors from starting or
/// canceling a workflow-run operation's workflow map the same way, and a start or cancel the
/// server rejects maps by its gRPC status: INVALID_ARGUMENT is `bad_request`; ALREADY_EXISTS,
/// FAILED_PRECONDITION and OUT_OF_RANGE a non-retryable `internal`; ABORTED and UNAVAILABLE
/// `unavailable`; RESOURCE_EXHAUSTED `resource_exhausted`; UNIMPLEMENTED `not_implemented`;
/// DEADLINE_EXCEEDED `upstream_timeout`; NOT_FOUND `not_found`; any other a retried
/// `internal`.
///
/// A handler the worker cancels (`cancelReason` set: the request's deadline passed, or the
/// worker is shutting down) whose request comes to a handler error caused by `error.Canceled`
/// (the handler returned it, or a workflow start or cancel failed with it) sends no response:
/// the worker acknowledges the cancel to the core, the caller's attempt times out, and the
/// server retries the request until the caller's timeout. A handler that returns anything
/// else after a cancel has that reported as usual.
pub const Context = struct {
    arena: Allocator,
    io_: Io,
    info_: Info,
    logger_: log.Logger = .{},
    /// The worker's client; null outside a worker unless a test sets one.
    client_: ?*client_mod.Client = null,
    /// Records with the operation's attributes; the worker frees it after the request.
    metrics_: metrics_mod.Handler = .noop,
    /// Applied to the input before decoding and to a synchronous result after encoding:
    /// the client's (`client.Options.codecs`). Borrowed.
    codecs: []const converter.PayloadCodec = &.{},
    /// Set by `fail`, `failCanceled`, and `handlerError`, and reported when the handler
    /// returns the error they gave back.
    pending: ?union(enum) { operation: OperationError, handler: HandlerError } = null,
    /// Written once by the worker when a cancel arrives, possibly from another thread.
    cancel_reason_: std.atomic.Value(u8) = .init(not_canceled),

    const not_canceled = std.math.maxInt(u8);

    /// Memory that lives until the request's response has been sent.
    pub fn allocator(ctx: *Context) Allocator {
        return ctx.arena;
    }

    /// The worker's Io. Handlers may block, sleep, and do I/O on it. A cancel of the request
    /// (its deadline passing, or the worker shutting down) arrives as `std.Io` cancellation:
    /// the handler's next wait returns `error.Canceled`, and `cancelReason` says why.
    pub fn io(ctx: *Context) Io {
        return ctx.io_;
    }

    /// The request being handled. Valid as long as `allocator()` memory.
    pub fn info(ctx: *const Context) *const Info {
        return &ctx.info_;
    }

    /// Why the request was canceled, or null if it hasn't been. Set before the cancel reaches
    /// the handler's `std.Io`.
    pub fn cancelReason(ctx: *const Context) ?CancelReason {
        const r = ctx.cancel_reason_.load(.acquire);
        return if (r == not_canceled) null else @fromBackingInt(@intCast(r));
    }

    /// Records why the request is being canceled, before its task is canceled. The first
    /// reason wins; returns whether this call set it. Called by the worker (any thread).
    pub fn setCancelReason(ctx: *Context, reason: CancelReason) bool {
        return ctx.cancel_reason_.cmpxchgStrong(not_canceled, @backingInt(reason), .acq_rel, .acquire) == null;
    }

    /// The client of the worker handling the request, for starting, signaling, and querying
    /// workflows (see `activity.Context.client` for why it is safe to share). Don't `close`
    /// it. Returns `error.NoClient` (logged) for a context built without one.
    pub fn client(ctx: *const Context) error{NoClient}!*client_mod.Client {
        return ctx.client_ orelse {
            ctx.logger_.warn("nexus.Context.client: this handler runs without a client", .{});
            return error.NoClient;
        };
    }

    /// The logger the worker set up for the request, carrying its service and operation as
    /// fields and writing to the worker's sink (`worker.Options.logger`); the std log for a
    /// context built without one.
    pub fn logger(ctx: *const Context) log.Logger {
        return ctx.logger_;
    }

    /// Records metrics through the client's exporter (`client.Options.metrics`); records
    /// nothing without one. Valid until the handler returns.
    pub fn metrics(ctx: *const Context) metrics_mod.Handler {
        var h = ctx.metrics_;
        h.owned = false;
        return h;
    }

    /// Fails the operation: it ran, and its outcome is a failure with `a`'s message, type,
    /// and details. The caller doesn't retry it. Return the error this gives back. `a` is
    /// stored as given, not copied: its strings and details must live until the response has
    /// been sent, as `ctx.allocator()` memory does, so never point them at the handler's stack.
    pub fn fail(ctx: *Context, a: failure_mod.Application) error{OperationFailed} {
        ctx.pending = .{ .operation = .{ .state = .failed, .failure = a } };
        return error.OperationFailed;
    }

    /// Ends the operation as canceled, with `message`. The caller doesn't retry it. Return the
    /// error this gives back. `message` is stored as given and must live until the response
    /// has been sent, as `ctx.allocator()` memory does.
    pub fn failCanceled(ctx: *Context, message: []const u8) error{OperationCanceled} {
        ctx.pending = .{ .operation = .{ .state = .canceled, .failure = .{ .message = message } } };
        return error.OperationCanceled;
    }

    /// Fails the request with a handler error, such as `.{ .type = .bad_request, .message =
    /// "the order ID is empty" }`. Return the error this gives back. `e` is stored as given:
    /// its message and cause must live until the response has been sent, as `ctx.allocator()`
    /// memory does.
    pub fn handlerError(ctx: *Context, e: HandlerError) error{HandlerError} {
        ctx.pending = .{ .handler = e };
        return error.HandlerError;
    }

    /// The result for `err`, returned by the handler of `operation` in `service`.
    fn errorResult(ctx: *Context, comptime service: []const u8, comptime operation: []const u8, err: anyerror) HandlerOutcome {
        if (ctx.pending) |p| switch (p) {
            .operation => |o| if (err == error.OperationFailed or err == error.OperationCanceled) return .{ .operation_error = o },
            .handler => |h| if (err == error.HandlerError) return .{ .handler_error = h },
        };
        return .{ .handler_error = plainError(ctx.arena, "nexus operation \"" ++ operation ++ "\" of service \"" ++ service ++ "\" failed", err) };
    }
};

const HandlerOutcome = union(enum) { operation_error: OperationError, handler_error: HandlerError };

/// The handler error for a plain Zig error (see `Context`). `what` says what failed.
fn plainError(arena: Allocator, what: []const u8, err: anyerror) HandlerError {
    return rpcError(arena, what, err, null);
}

/// The handler error for `err` from a client call, which the server rejected with gRPC
/// status `rejected` when `err` is `error.RpcFailed` and the status is known (see `Context`).
/// Its message carries the server's.
fn rpcError(arena: Allocator, what: []const u8, err: anyerror, rejected: ?client_mod.RpcRejection) HandlerError {
    const plain = err == error.WorkflowExecutionAlreadyStarted or err == error.InvalidOptions or err == error.InvalidSearchAttribute;
    var e: HandlerError = .{
        .type = if (err == error.NotFound) .not_found else .internal,
        .message = std.fmt.allocPrint(arena, "{s}: {t}", .{ what, err }) catch @errorName(err),
        .retry = if (plain) .non_retryable else .default,
        .cause = .{ .message = @errorName(err), .type = @errorName(err) },
    };
    if (err != error.RpcFailed) return e;
    const r = rejected orelse return e;
    e.message = std.fmt.allocPrint(arena, "{s}: the server rejected the call with gRPC status {t}: {s}", .{ what, r.code, r.message }) catch e.message;
    switch (r.code) {
        .invalid_argument => e.type = .bad_request,
        .already_exists, .failed_precondition, .out_of_range => e.retry = .non_retryable,
        .aborted, .unavailable => e.type = .unavailable,
        .resource_exhausted => e.type = .resource_exhausted,
        .unimplemented => e.type = .not_implemented,
        .deadline_exceeded => e.type = .upstream_timeout,
        .not_found => e.type = .not_found,
        else => {},
    }
    return e;
}

// ---------------------------------------------------------------- dispatch

/// What starting an operation came to. Slices the SDK produced (the result payload, the
/// token, the links, and the messages of errors it mapped) are allocated from the context's
/// `allocator()` or are static. An `operation_error` or `handler_error` the handler set with
/// `Context.fail`, `failCanceled`, or `handlerError` is returned as the handler gave it, so
/// its slices live as long as the handler made them live; those calls require that to be
/// until the response has been sent.
pub const StartResult = union(enum) {
    /// A synchronous operation finished with this output, encoded by the payload converter
    /// and the context's codecs.
    succeeded: Succeeded,
    /// An asynchronous operation started: the caller follows it by `token` and learns its
    /// outcome from the completion callback.
    started: Started,
    /// The operation ran and failed or was canceled.
    operation_error: OperationError,
    /// The request failed: the input didn't decode, the handler returned a handler error or
    /// a plain error, or starting the workflow failed.
    handler_error: HandlerError,

    /// A finished synchronous operation.
    pub const Succeeded = struct {
        /// The output as an encoded payload.
        result: Payload,
    };

    /// A started asynchronous operation.
    pub const Started = struct {
        /// Identifies the operation to `Operation.cancel`.
        token: []const u8,
        /// For a workflow-run operation, a link to the started workflow's start event.
        links: []const Link = &.{},
    };
};

/// What canceling an operation came to. The error's slices are allocated from the context's
/// `allocator()` or are static.
pub const CancelResult = union(enum) {
    /// The cancel was delivered: for a workflow-run operation, the workflow's cancellation
    /// was requested.
    requested,
    /// A token that doesn't parse (`bad_request`), a workflow that doesn't exist
    /// (`not_found`), a synchronous operation (`not_implemented`), or a failed request.
    handler_error: HandlerError,
};

/// An operation as a worker stores it: type-erased calls that decode the input, run the
/// handler (on the receiver for handlers that take one), and encode the result. Both are
/// called on the task that handles the request, never fail outright (every failure is a
/// result), and borrow `ctx` and their other arguments for the call.
pub const Operation = struct {
    name: []const u8,
    kind: Kind,
    /// `input` is the request's payload, still encoded by the caller's codecs; null when the
    /// request carries none. `input` is borrowed for the call.
    start: *const fn (receiver: ?*anyopaque, ctx: *Context, input: ?Payload) StartResult,
    /// `token` is the one `start` returned in `StartResult.started`, borrowed for the call.
    cancel: *const fn (receiver: ?*anyopaque, ctx: *Context, token: []const u8) CancelResult,

    /// `sync` finishes in its start; `workflow_run` starts a workflow and finishes when the
    /// workflow closes.
    pub const Kind = enum { sync, workflow_run };
};

/// A service as a worker registers it; `operations` is static. Take it from
/// `ServiceHandler(...).registered` or `.with(receiver)` rather than building one by hand:
/// those set `receiver` whenever a handler takes one, and an operation whose handler takes a
/// receiver must not be called with a null `receiver`.
pub const Registered = struct {
    name: []const u8,
    operations: []const Operation,
    receiver: ?*anyopaque = null,

    /// The operation called `name`, bound to the receiver, or null.
    pub fn find(r: *const Registered, name: []const u8) ?BoundOperation {
        for (r.operations) |*op| if (std.mem.eql(u8, op.name, name)) return .{ .operation = op, .receiver = r.receiver };
        return null;
    }
};

/// An operation of a registered service, ready to call. Valid while the worker it came from.
pub const BoundOperation = struct {
    operation: *const Operation,
    receiver: ?*anyopaque,

    /// Starts the operation; see `Operation.start`.
    pub fn start(b: BoundOperation, ctx: *Context, input: ?Payload) StartResult {
        return b.operation.start(b.receiver, ctx, input);
    }

    /// Cancels the operation started with `token`; see `Operation.cancel`.
    pub fn cancel(b: BoundOperation, ctx: *Context, token: []const u8) CancelResult {
        return b.operation.cancel(b.receiver, ctx, token);
    }
};

/// Why `Worker.nexusOperation` found nothing. Both are reported to the caller as a
/// `not_found` handler error.
pub const LookupError = error{ ServiceNotFound, OperationNotFound };

fn Implementation(comptime service: []const u8, comptime Op: type, comptime func: anytype) type {
    const shape = handlerShape(service, Op, func).ok;
    const op = Op.operation_name;
    const what = "nexus operation \"" ++ op ++ "\" of service \"" ++ service ++ "\"";
    return struct {
        const kind: Operation.Kind = if (shape.runs_workflow) .workflow_run else .sync;

        fn call(receiver: ?*anyopaque, ctx: *Context, input: Op.Input) shape.Return {
            const args = if (shape.takes_input) .{input} else .{};
            if (shape.bound) {
                const r: @typeInfo(@TypeOf(func)).@"fn".param_types[0].? = @ptrCast(@alignCast(receiver.?));
                return @call(.auto, func, .{ r, ctx } ++ args);
            }
            return @call(.auto, func, .{ctx} ++ args);
        }

        fn start(receiver: ?*anyopaque, ctx: *Context, input: ?Payload) StartResult {
            var failed: HandlerError = undefined;
            const in = decodeInput(Op.Input, what, ctx, input, &failed) catch return .{ .handler_error = failed };
            const value: shape.Value = if (@typeInfo(shape.Return) == .error_union)
                call(receiver, ctx, in) catch |err| return switch (ctx.errorResult(service, op, err)) {
                    .operation_error => |e| .{ .operation_error = e },
                    .handler_error => |e| .{ .handler_error = e },
                }
            else
                call(receiver, ctx, in);
            if (shape.runs_workflow) return startWorkflow(shape.Value.Workflow, what, ctx, value);
            return encodeOutput(ctx, value);
        }

        fn cancel(_: ?*anyopaque, ctx: *Context, token: []const u8) CancelResult {
            if (shape.runs_workflow) return cancelWorkflow(what, ctx, token);
            return .{ .handler_error = .{ .type = .not_implemented, .message = what ++ " is synchronous: it finishes in its start, so there is nothing to cancel" } };
        }

        fn encodeOutput(ctx: *Context, value: shape.Value) StartResult {
            const a = ctx.arena;
            const payload = converter.toPayload(a, value) catch return outOfMemory(what);
            if (ctx.codecs.len == 0) return .{ .succeeded = .{ .result = payload } };
            const encoded = converter.encodePayloadsWithContext(ctx.codecs, a, ctx.io_, &codecContext(ctx), &.{payload}) catch |err| return .{ .handler_error = .{
                .type = .internal,
                .message = std.fmt.allocPrint(a, "{s}: encoding its output with the client's codecs failed: {t}", .{ what, err }) catch what ++ ": encoding its output with the client's codecs failed",
            } };
            return .{ .succeeded = .{ .result = encoded[0] } };
        }
    };
}

fn codecContext(ctx: *const Context) converter.CodecContext {
    return .{ .namespace = ctx.info_.namespace };
}

/// Decodes `input` as `T`: through the context's codecs, then the payload converter. On
/// failure, `failed` says why.
fn decodeInput(comptime T: type, comptime what: []const u8, ctx: *Context, input: ?Payload, failed: *HandlerError) error{Failed}!T {
    if (T == void) return {};
    const a = ctx.arena;
    var payload = input orelse {
        if (@typeInfo(T) == .optional) return null;
        failed.* = .{ .type = .bad_request, .message = what ++ " takes an input of type " ++ @typeName(T) ++ ", but the request carries none", .retry = .non_retryable };
        return error.Failed;
    };
    if (ctx.codecs.len > 0) {
        const decoded = converter.decodePayloadsWithContext(ctx.codecs, a, ctx.io_, &codecContext(ctx), &.{payload}) catch |err| {
            failed.* = .{ .type = .internal, .message = std.fmt.allocPrint(a, "{s}: decoding its input with the client's codecs failed: {t}", .{ what, err }) catch what ++ ": decoding its input with the client's codecs failed" };
            return error.Failed;
        };
        payload = decoded[0];
    }
    return converter.fromPayload(T, a, payload) catch |err| {
        if (err == error.OutOfMemory) {
            failed.* = .{ .type = .internal, .message = what ++ ": out of memory decoding its input" };
            return error.Failed;
        }
        failed.* = .{
            .type = .bad_request,
            .message = std.fmt.allocPrint(a, "{s}: its input doesn't decode as {s}: {t} (encoding \"{s}\")", .{ what, @typeName(T), err, converter.encodingOf(payload) orelse "none" }) catch what ++ ": its input doesn't decode as " ++ @typeName(T),
            .retry = .non_retryable,
        };
        return error.Failed;
    };
}

// ---------------------------------------------------------------- workflow-run operations

const token_header = "nexus-operation-token";

/// Starts the workflow `start` describes for a workflow-run operation, and returns its
/// operation token and a link to its start event.
fn startWorkflow(comptime W: type, comptime what: []const u8, ctx: *Context, start: WorkflowStart(W)) StartResult {
    const a = ctx.arena;
    const c = ctx.client_ orelse return .{ .handler_error = .{
        .type = .internal,
        .message = what ++ " starts workflow \"" ++ W.workflow_name ++ "\", but its context has no client to start it with",
        .retry = .non_retryable,
    } };
    const i = &ctx.info_;
    var options = start.options;
    var request_id = i.request_id;
    if (request_id.len == 0) request_id = client_mod.uuid(a, ctx.io_) catch return outOfMemory(what);
    if (options.id.len == 0) options.id = request_id;
    if (options.task_queue.len == 0) options.task_queue = i.task_queue;
    const token = encodeWorkflowToken(a, c.namespace, options.id) catch return outOfMemory(what);

    var headers: std.ArrayList(Header) = .empty;
    headers.ensureTotalCapacity(a, i.callback_headers.len + 1) catch return outOfMemory(what);
    for (i.callback_headers) |h| if (!std.ascii.eqlIgnoreCase(h.name, token_header)) headers.appendAssumeCapacity(h);
    headers.appendAssumeCapacity(.{ .name = token_header, .value = token });

    var rejected: ?client_mod.RpcRejection = null;
    const started = c.startNexusWorkflow(a, W.workflow_name, options, start.args, .{
        .request_id = request_id,
        .callback_url = i.callback_url,
        .callback_headers = if (i.callback_url.len > 0) headers.items else &.{},
        .links = i.links,
        .rejection = &rejected,
    }) catch |err| return .{ .handler_error = rpcError(a, what ++ ": starting workflow \"" ++ W.workflow_name ++ "\" failed", err, rejected) };
    const links = a.dupe(Link, &.{started.link}) catch return outOfMemory(what);
    return .{ .started = .{ .token = token, .links = links } };
}

fn outOfMemory(comptime what: []const u8) StartResult {
    return .{ .handler_error = .{ .type = .internal, .message = what ++ ": out of memory" } };
}

/// Requests cancellation of the workflow a workflow-run operation's `token` names.
fn cancelWorkflow(comptime what: []const u8, ctx: *Context, token: []const u8) CancelResult {
    const a = ctx.arena;
    var why: []const u8 = "";
    const t = decodeWorkflowToken(a, token, &why) catch |err| return .{ .handler_error = switch (err) {
        error.OutOfMemory => .{ .type = .internal, .message = what ++ ": out of memory decoding its operation token" },
        error.InvalidToken => .{
            .type = .bad_request,
            .message = std.fmt.allocPrint(a, "{s}: the operation token to cancel is not a workflow-run token: {s}", .{ what, why }) catch what ++ ": the operation token to cancel is not a workflow-run token",
            .retry = .non_retryable,
        },
    } };
    const c = ctx.client_ orelse return .{ .handler_error = .{
        .type = .internal,
        .message = what ++ ": canceling its workflow needs a client, and its context has none",
        .retry = .non_retryable,
    } };
    if (!std.mem.eql(u8, t.namespace, c.namespace)) return .{ .handler_error = .{
        .type = .bad_request,
        .message = std.fmt.allocPrint(a, "{s}: the operation token names workflow \"{s}\" in namespace \"{s}\", but this worker's client is in namespace \"{s}\"", .{ what, t.workflow_id, t.namespace, c.namespace }) catch what ++ ": the operation token names another namespace",
        .retry = .non_retryable,
    } };
    var rejected: ?client_mod.RpcRejection = null;
    c.cancelNexusWorkflow(a, t.workflow_id, &rejected) catch |err|
        return .{ .handler_error = rpcError(a, std.fmt.allocPrint(a, "{s}: canceling workflow \"{s}\" failed", .{ what, t.workflow_id }) catch what ++ ": canceling its workflow failed", err, rejected) };
    return .requested;
}

/// A workflow-run operation token: `{"t":1,"ns":<namespace>,"wid":<workflow ID>}` as JSON,
/// base64url-encoded without padding, as other Temporal workers write it, so workers of a
/// mixed task queue cancel each other's operations.
const WorkflowToken = struct {
    namespace: []const u8,
    workflow_id: []const u8,
};

const workflow_token_type = 1;

fn encodeWorkflowToken(a: Allocator, namespace: []const u8, workflow_id: []const u8) error{OutOfMemory}![]const u8 {
    const json = try std.json.Stringify.valueAlloc(a, .{ .t = @as(u8, workflow_token_type), .ns = namespace, .wid = workflow_id }, .{ .escape_unicode = true });
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const out = try a.alloc(u8, encoder.calcSize(json.len));
    return encoder.encode(out, json);
}

/// Parses a workflow-run token. On `error.InvalidToken`, `why` says what is wrong with it.
fn decodeWorkflowToken(a: Allocator, token: []const u8, why: *[]const u8) error{ OutOfMemory, InvalidToken }!WorkflowToken {
    if (token.len == 0) {
        why.* = "the token is empty";
        return error.InvalidToken;
    }
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const text = std.mem.trimEnd(u8, token, "=");
    const json = try a.alloc(u8, decoder.calcSizeForSlice(text) catch {
        why.* = "it is not base64url";
        return error.InvalidToken;
    });
    decoder.decode(json, text) catch {
        why.* = "it is not base64url";
        return error.InvalidToken;
    };
    const Fields = struct { t: i64, ns: []const u8, wid: ?[]const u8 = null, v: ?i64 = null };
    const fields = std.json.parseFromSliceLeaky(Fields, a, json, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        why.* = "it is not a JSON object with an integer \"t\" and a string \"ns\"";
        return error.InvalidToken;
    };
    if (fields.t != workflow_token_type) {
        why.* = std.fmt.allocPrint(a, "its type is {d}, not {d} (workflow run)", .{ fields.t, workflow_token_type }) catch "its type is not workflow run";
        return error.InvalidToken;
    }
    if (fields.v) |v| if (v != 0) {
        why.* = std.fmt.allocPrint(a, "its version is {d}; this worker reads version 0", .{v}) catch "its version is not 0";
        return error.InvalidToken;
    };
    const wid = fields.wid orelse "";
    if (wid.len == 0) {
        why.* = "it names no workflow ID";
        return error.InvalidToken;
    }
    return .{ .namespace = fields.ns, .workflow_id = wid };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const workflow = @import("workflow.zig");

const EchoInput = struct { message: []const u8 };
const EchoOutput = struct { message: []const u8, length: usize };
const Echo = OperationDef("echo", EchoInput, EchoOutput);
const Ping = OperationDef("ping", void, void);
const Shout = OperationDef("shout", []const u8, []const u8);
const Greet = OperationDef("greet", []const u8, []const u8);
const TestService = ServiceDef("test-service", .{ Echo, Ping, Shout, Greet });

const TestHandlers = struct {
    var pings: u32 = 0;

    fn echo(ctx: *Context, input: EchoInput) !EchoOutput {
        return .{ .message = try std.fmt.allocPrint(ctx.allocator(), "{s}/{s}", .{ ctx.info().operation, input.message }), .length = input.message.len };
    }

    fn ping(ctx: *Context) !void {
        _ = ctx;
        pings += 1;
    }

    fn shout(ctx: *Context, input: []const u8) ![]const u8 {
        if (std.mem.eql(u8, input, "fail")) return ctx.fail(.{ .message = "too quiet", .type = "Quiet", .details = &.{} });
        if (std.mem.eql(u8, input, "cancel")) return ctx.failCanceled("shouting stopped");
        if (std.mem.eql(u8, input, "bad")) return ctx.handlerError(.{ .type = .bad_request, .message = "nothing to shout" });
        if (std.mem.eql(u8, input, "retry")) return ctx.handlerError(.{ .type = .bad_request, .message = "try again", .retry = .retryable });
        if (std.mem.eql(u8, input, "plain")) return error.Hoarse;
        if (std.mem.eql(u8, input, "missing")) return error.NotFound;
        if (std.mem.eql(u8, input, "taken")) return error.WorkflowExecutionAlreadyStarted;
        return std.ascii.allocUpperString(ctx.allocator(), input);
    }

    const Greeter = struct {
        greeting: []const u8,
        calls: u32 = 0,
        fn greet(g: *Greeter, ctx: *Context, name: []const u8) ![]const u8 {
            g.calls += 1;
            return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ g.greeting, name });
        }
    };
};

const TestServiceHandler = ServiceHandler(TestService, .{
    OperationHandler(Echo, TestHandlers.echo),
    OperationHandler(Ping, TestHandlers.ping),
    OperationHandler(Shout, TestHandlers.shout),
    OperationHandler(Greet, TestHandlers.Greeter.greet),
});

fn testContext(arena: Allocator, operation: []const u8) Context {
    return .{ .arena = arena, .io_ = testing.io, .info_ = .{ .service = "test-service", .operation = operation, .namespace = "default" } };
}

fn startOp(arena: Allocator, reg: *const Registered, operation: []const u8, input: anytype) !StartResult {
    var ctx = testContext(arena, operation);
    const bound = reg.find(operation) orelse return error.TestUnexpectedResult;
    const payload: ?Payload = if (@TypeOf(input) == @TypeOf(null)) null else try converter.toPayload(arena, input);
    return bound.start(&ctx, payload);
}

test "a contract carries names and types, and a service knows its operations" {
    try testing.expectEqualStrings("echo", Echo.operation_name);
    try testing.expect(Echo.Input == EchoInput and Echo.Output == EchoOutput);
    try testing.expectEqualStrings("test-service", TestService.service_name);
    try testing.expectEqual(4, TestService.operation_defs.len);
    try testing.expect(TestService.hasOperation(Echo));
    try testing.expect(!TestService.hasOperation(OperationDef("echo", EchoInput, []const u8)));
    try testing.expect(!TestService.hasOperation(OperationDef("other", EchoInput, EchoOutput)));
    try testing.expect(TestService.operationNamed("ping").? == Ping);
    try testing.expect(TestServiceHandler.Receiver == TestHandlers.Greeter);
    try testing.expectEqual(Operation.Kind.sync, TestServiceHandler.operations[0].kind);
}

test "a synchronous operation decodes its input, runs, and encodes its output, as a function or a method" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var greeter: TestHandlers.Greeter = .{ .greeting = "Hello" };
    const reg = TestServiceHandler.with(&greeter);

    const echoed = try startOp(a, &reg, "echo", EchoInput{ .message = "hi" });
    const out = try converter.fromPayload(EchoOutput, a, echoed.succeeded.result);
    try testing.expectEqualStrings("echo/hi", out.message);
    try testing.expectEqual(2, out.length);

    const greeted = try startOp(a, &reg, "greet", @as([]const u8, "Temporal"));
    try testing.expectEqualStrings("\"Hello Temporal\"", greeted.succeeded.result.data);
    try testing.expectEqual(1, greeter.calls);
    try testing.expect(reg.find("nope") == null);
}

test "an operation with void input and output takes no payload and returns a null payload" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var greeter: TestHandlers.Greeter = .{ .greeting = "Hello" };
    const reg = TestServiceHandler.with(&greeter);
    const before = TestHandlers.pings;
    const none = try startOp(a, &reg, "ping", null);
    try testing.expectEqualStrings("binary/null", converter.encodingOf(none.succeeded.result).?);
    _ = try startOp(a, &reg, "ping", {});
    try testing.expectEqual(before + 2, TestHandlers.pings);
}

test "operation failures and handler errors come out as the values the handler set" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var greeter: TestHandlers.Greeter = .{ .greeting = "Hello" };
    const reg = TestServiceHandler.with(&greeter);

    const failed = (try startOp(a, &reg, "shout", @as([]const u8, "fail"))).operation_error;
    try testing.expectEqual(OperationError.State.failed, failed.state);
    try testing.expectEqualStrings("too quiet", failed.failure.message);
    try testing.expectEqualStrings("Quiet", failed.failure.type);

    const canceled = (try startOp(a, &reg, "shout", @as([]const u8, "cancel"))).operation_error;
    try testing.expectEqual(OperationError.State.canceled, canceled.state);
    try testing.expectEqualStrings("shouting stopped", canceled.failure.message);

    const bad = (try startOp(a, &reg, "shout", @as([]const u8, "bad"))).handler_error;
    try testing.expectEqual(HandlerErrorType.bad_request, bad.type);
    try testing.expectEqualStrings("nothing to shout", bad.message);
    try testing.expect(!bad.retryable());
    try testing.expect((try startOp(a, &reg, "shout", @as([]const u8, "retry"))).handler_error.retryable());
}

test "a plain Zig error from a handler maps to a handler error as documented" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var greeter: TestHandlers.Greeter = .{ .greeting = "Hello" };
    const reg = TestServiceHandler.with(&greeter);

    const plain = (try startOp(a, &reg, "shout", @as([]const u8, "plain"))).handler_error;
    try testing.expectEqual(HandlerErrorType.internal, plain.type);
    try testing.expect(plain.retryable());
    try testing.expectEqualStrings("nexus operation \"shout\" of service \"test-service\" failed: Hoarse", plain.message);
    try testing.expectEqualStrings("Hoarse", plain.cause.?.type);

    const missing = (try startOp(a, &reg, "shout", @as([]const u8, "missing"))).handler_error;
    try testing.expectEqual(HandlerErrorType.not_found, missing.type);
    try testing.expect(!missing.retryable());

    const taken = (try startOp(a, &reg, "shout", @as([]const u8, "taken"))).handler_error;
    try testing.expectEqual(HandlerErrorType.internal, taken.type);
    try testing.expect(!taken.retryable());

    // Returning the error `fail` gives back without calling it is a plain error too.
    const S = struct {
        fn sneaky(_: *Context, _: []const u8) ![]const u8 {
            return error.OperationFailed;
        }
    };
    const H = ServiceHandler(ServiceDef("s", .{Shout}), .{OperationHandler(Shout, S.sneaky)});
    const sneaky = (try startOp(a, &H.registered, "shout", @as([]const u8, "x"))).handler_error;
    try testing.expectEqual(HandlerErrorType.internal, sneaky.type);
    try testing.expectEqualStrings("OperationFailed", sneaky.cause.?.type);
}

test "an input that doesn't decode, or is missing, is a non-retryable bad request" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var greeter: TestHandlers.Greeter = .{ .greeting = "Hello" };
    const reg = TestServiceHandler.with(&greeter);

    const wrong = (try startOp(a, &reg, "echo", @as(u32, 5))).handler_error;
    try testing.expectEqual(HandlerErrorType.bad_request, wrong.type);
    try testing.expect(!wrong.retryable());
    try testing.expect(std.mem.indexOf(u8, wrong.message, "nexus operation \"echo\" of service \"test-service\": its input doesn't decode as") != null);

    var ctx = testContext(a, "echo");
    const unknown = reg.find("echo").?.start(&ctx, .{ .data = "??" });
    try testing.expectEqual(HandlerErrorType.bad_request, unknown.handler_error.type);

    const none = (try startOp(a, &reg, "echo", null)).handler_error;
    try testing.expectEqual(HandlerErrorType.bad_request, none.type);
    try testing.expect(std.mem.indexOf(u8, none.message, "the request carries none") != null);
}

test "the input and a synchronous result go through the context's codecs" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const zlib: converter.ZlibCodec = .{ .always_encode = true };
    const codecs = [_]converter.PayloadCodec{zlib.codec()};
    var greeter: TestHandlers.Greeter = .{ .greeting = "Hello" };
    const reg = TestServiceHandler.with(&greeter);

    const input = (try converter.encodePayloads(&codecs, a, testing.io, &.{try converter.toPayload(a, @as([]const u8, "Zig"))}))[0];
    var ctx = testContext(a, "greet");
    ctx.codecs = &codecs;
    const result = reg.find("greet").?.start(&ctx, input).succeeded.result;
    try testing.expectEqualStrings(converter.ZlibCodec.encoding, converter.encodingOf(result).?);
    const decoded = (try converter.decodePayloads(&codecs, a, testing.io, &.{result}))[0];
    try testing.expectEqualStrings("\"Hello Zig\"", decoded.data);
}

test "canceling a synchronous operation is not implemented" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var greeter: TestHandlers.Greeter = .{ .greeting = "Hello" };
    const reg = TestServiceHandler.with(&greeter);
    var ctx = testContext(arena.allocator(), "echo");
    const result = reg.find("echo").?.cancel(&ctx, "token");
    try testing.expectEqual(HandlerErrorType.not_implemented, result.handler_error.type);
}

test "handler error types have wire names and default retry behaviors" {
    try testing.expectEqualStrings("BAD_REQUEST", HandlerErrorType.bad_request.wireName());
    try testing.expectEqualStrings("UPSTREAM_TIMEOUT", HandlerErrorType.upstream_timeout.wireName());
    try testing.expectEqual(HandlerErrorType.resource_exhausted, HandlerErrorType.fromWireName("RESOURCE_EXHAUSTED").?);
    try testing.expectEqual(null, HandlerErrorType.fromWireName("bad_request"));
    try testing.expect(HandlerErrorType.internal.retryableByDefault());
    try testing.expect(!HandlerErrorType.not_found.retryableByDefault());
    try testing.expect(!(HandlerError{ .type = .internal, .message = "", .retry = .non_retryable }).retryable());
}

test "a context records the first cancel reason and reads headers case-insensitively" {
    var ctx: Context = .{ .arena = testing.allocator, .io_ = testing.io, .info_ = .{
        .service = "s",
        .operation = "o",
        .headers = &.{.{ .name = "Request-Timeout", .value = "10s" }},
    } };
    try testing.expectEqualStrings("10s", ctx.info().header("request-timeout").?);
    try testing.expectEqual(null, ctx.info().header("other"));
    try testing.expectEqual(null, ctx.cancelReason());
    try testing.expect(ctx.setCancelReason(.timed_out));
    try testing.expect(!ctx.setCancelReason(.worker_shutdown));
    try testing.expectEqual(CancelReason.timed_out, ctx.cancelReason().?);
    try testing.expectError(error.NoClient, ctx.client());
}

test "a workflow-run token has the shared JSON layout, and malformed tokens are rejected" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const token = try encodeWorkflowToken(a, "default", "order-1");
    // base64url of {"t":1,"ns":"default","wid":"order-1"}, no padding.
    try testing.expectEqualStrings("eyJ0IjoxLCJucyI6ImRlZmF1bHQiLCJ3aWQiOiJvcmRlci0xIn0", token);
    var why: []const u8 = "";
    const t = try decodeWorkflowToken(a, token, &why);
    try testing.expectEqualStrings("default", t.namespace);
    try testing.expectEqualStrings("order-1", t.workflow_id);
    // Padded, and with a version 0 and unknown fields, as other writers may send.
    _ = try decodeWorkflowToken(a, "eyJ0IjoxLCJucyI6ImRlZmF1bHQiLCJ3aWQiOiJvcmRlci0xIn0=", &why);
    const extra = try encodeBase64(a, "{\"v\":0,\"t\":1,\"ns\":\"\",\"wid\":\"w\",\"x\":true}");
    try testing.expectEqualStrings("w", (try decodeWorkflowToken(a, extra, &why)).workflow_id);
    // Non-ASCII survives the round trip.
    const unicode = try encodeWorkflowToken(a, "default", "bestellung-\u{fc}");
    try testing.expectEqualStrings("bestellung-\u{fc}", (try decodeWorkflowToken(a, unicode, &why)).workflow_id);

    for ([_][]const u8{
        "",
        "not base64!",
        try encodeBase64(a, "[1,2]"),
        try encodeBase64(a, "{\"t\":2,\"ns\":\"default\",\"wid\":\"w\"}"),
        try encodeBase64(a, "{\"t\":1,\"ns\":\"default\"}"),
        try encodeBase64(a, "{\"t\":1,\"ns\":\"default\",\"wid\":\"\"}"),
        try encodeBase64(a, "{\"t\":1,\"wid\":\"w\"}"),
        try encodeBase64(a, "{\"t\":1,\"ns\":\"default\",\"wid\":\"w\",\"v\":1}"),
    }) |bad| {
        why = "";
        try testing.expectError(error.InvalidToken, decodeWorkflowToken(a, bad, &why));
        try testing.expect(why.len > 0);
    }
}

fn encodeBase64(a: Allocator, text: []const u8) ![]const u8 {
    const encoder = std.base64.url_safe_no_pad.Encoder;
    return encoder.encode(try a.alloc(u8, encoder.calcSize(text.len)), text);
}

const WaitingWorkflow = struct {
    fn wait(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        try ctx.sleep(.fromSeconds(3600));
        return name;
    }
    const Def = workflow.Def("NexusWaiting", wait);

    const Run = OperationDef("run", []const u8, []const u8);
    const Attach = OperationDef("attach", []const u8, []const u8);
    const Service = ServiceDef("run-service", .{ Run, Attach });

    fn run(ctx: *Context, input: []const u8) !WorkflowStart(Def) {
        return .{ .options = .{ .id = ctx.info().request_id }, .args = .{input} };
    }
    fn attach(_: *Context, input: []const u8) !WorkflowStart(Def) {
        return .{ .options = .{ .id = input, .id_conflict_policy = .use_existing }, .args = .{input} };
    }
    const Handler = ServiceHandler(Service, .{ OperationHandler(Run, run), OperationHandler(Attach, attach) });
};

test "a workflow-run operation's garbage token is a bad request, before any client call" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var ctx = testContext(arena.allocator(), "run");
    const op = WaitingWorkflow.Handler.registered.find("run").?;
    try testing.expectEqual(Operation.Kind.workflow_run, op.operation.kind);
    const result = op.cancel(&ctx, "garbage");
    try testing.expectEqual(HandlerErrorType.bad_request, result.handler_error.type);
    try testing.expect(std.mem.indexOf(u8, result.handler_error.message, "not a workflow-run token") != null);
    // Starting needs a client.
    const started = op.start(&ctx, try converter.toPayload(arena.allocator(), @as([]const u8, "x")));
    try testing.expectEqual(HandlerErrorType.internal, started.handler_error.type);
}

test "a workflow-run operation starts its workflow with the request ID, callback and links, idempotently, and cancels it" {
    const testing_mod = @import("testing.zig");
    const env = try testing_mod.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const caller_link: Link = .{ .variant = .{ .workflow_event = .{
        .namespace = "default",
        .workflow_id = "caller-workflow",
        .run_id = "8b2f1c5e-6a3d-4f7e-9c1b-2d4e6f8a0b1c",
        .reference = .{ .event_ref = .{ .event_id = 5, .event_type = .EVENT_TYPE_NEXUS_OPERATION_SCHEDULED } },
    } } };
    var ctx: Context = .{ .arena = a, .io_ = testing.io, .client_ = &env.client, .info_ = .{
        .service = "run-service",
        .operation = "run",
        .request_id = "nexus-request-1",
        .callback_url = "temporal://system",
        .callback_headers = &.{.{ .name = "caller-header", .value = "caller-value" }},
        .links = &.{caller_link},
        .namespace = "default",
        .task_queue = env.task_queue,
    } };
    const op = WaitingWorkflow.Handler.registered.find("run").?;
    const input = try converter.toPayload(a, @as([]const u8, "Temporal"));

    const first = switch (op.start(&ctx, input)) {
        .started => |s| s,
        .handler_error => |e| {
            std.debug.print("unexpected handler error: {t}: {s}\n", .{ e.type, e.message });
            return error.TestUnexpectedResult;
        },
        else => return error.TestUnexpectedResult,
    };
    var why: []const u8 = "";
    const token = try decodeWorkflowToken(a, first.token, &why);
    try testing.expectEqualStrings("default", token.namespace);
    try testing.expectEqualStrings("nexus-request-1", token.workflow_id);
    try testing.expectEqual(1, first.links.len);
    const started_link = first.links[0].variant.?.workflow_event;
    try testing.expectEqualStrings("nexus-request-1", started_link.workflow_id);
    try testing.expect(started_link.run_id.len > 0);

    // The server recorded the callback (with the token), the links, and the request ID.
    const described = try env.client.describeWorkflow(a, token.workflow_id, "");
    try testing.expectEqualStrings(started_link.run_id, described.run_id);
    try testing.expectEqualStrings(env.task_queue, described.task_queue);
    const callbacks = described.raw.callbacks.items;
    try testing.expectEqual(1, callbacks.len);
    const nexus_callback = callbacks[0].callback.?.variant.?.nexus;
    try testing.expectEqualStrings("temporal://system", nexus_callback.url);
    var saw_token = false;
    var saw_caller_header = false;
    for (nexus_callback.header.items) |h| {
        if (std.ascii.eqlIgnoreCase(h.key, token_header)) saw_token = std.mem.eql(u8, h.value, first.token);
        if (std.ascii.eqlIgnoreCase(h.key, "caller-header")) saw_caller_header = std.mem.eql(u8, h.value, "caller-value");
    }
    try testing.expect(saw_token and saw_caller_header);
    var saw_request_id = false;
    if (described.raw.workflow_extended_info) |ext| for (ext.request_id_infos.items) |entry| {
        if (std.mem.eql(u8, entry.key, "nexus-request-1")) saw_request_id = true;
    };
    try testing.expect(saw_request_id);
    // The caller's links ride on the callback, which the server records with the start
    // event; the start request carries them too, for servers that read them there.
    const history = try env.client.fetchHistory(a, token.workflow_id, "");
    const recorded = history.events.items[0].attributes.?.workflow_execution_started_event_attributes.completion_callbacks.items;
    try testing.expectEqual(1, recorded.len);
    try testing.expectEqualStrings("temporal://system", recorded[0].variant.?.nexus.url);
    for ([_][]const Link{ recorded[0].links.items, callbacks[0].callback.?.links.items }) |links| {
        try testing.expectEqual(1, links.len);
        try testing.expectEqualStrings("caller-workflow", links[0].variant.?.workflow_event.workflow_id);
        try testing.expectEqual(5, links[0].variant.?.workflow_event.reference.?.event_ref.event_id);
    }

    // A retried start with the same request ID finds the same workflow.
    var retry_ctx = ctx;
    retry_ctx.pending = null;
    const second = op.start(&retry_ctx, input).started;
    try testing.expectEqualStrings(first.token, second.token);
    try testing.expectEqualStrings(started_link.run_id, second.links[0].variant.?.workflow_event.run_id);

    // Cancel requests cancellation of that workflow.
    var cancel_ctx: Context = .{ .arena = a, .io_ = testing.io, .client_ = &env.client, .info_ = .{ .service = "run-service", .operation = "run", .namespace = "default" } };
    try testing.expectEqual(CancelResult.requested, op.cancel(&cancel_ctx, first.token));
    const after = try env.client.fetchHistory(a, token.workflow_id, "");
    var cancel_requested = false;
    for (after.events.items) |e| {
        if (e.event_type == .EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED) cancel_requested = true;
    }
    try testing.expect(cancel_requested);

    // A token for a workflow that doesn't exist is not found; a garbage one a bad request.
    const missing = op.cancel(&cancel_ctx, try encodeWorkflowToken(a, "default", "no-such-workflow"));
    try testing.expectEqual(HandlerErrorType.not_found, missing.handler_error.type);
    const foreign = op.cancel(&cancel_ctx, try encodeWorkflowToken(a, "elsewhere", "w"));
    try testing.expectEqual(HandlerErrorType.bad_request, foreign.handler_error.type);
    try testing.expectEqual(HandlerErrorType.bad_request, op.cancel(&cancel_ctx, "garbage").handler_error.type);

    // Under `.use_existing`, a start that finds the workflow running attaches the operation's
    // callback to it.
    var existing = try env.client.executeWorkflow(WaitingWorkflow.Def, .{ .id = "already-running", .task_queue = env.task_queue }, .{"x"});
    defer existing.deinit();
    var attach_ctx = ctx;
    attach_ctx.info_.request_id = "nexus-request-2";
    attach_ctx.info_.operation = "attach";
    const attached = WaitingWorkflow.Handler.registered.find("attach").?.start(&attach_ctx, try converter.toPayload(a, @as([]const u8, "already-running"))).started;
    try testing.expectEqualStrings(existing.run_id, attached.links[0].variant.?.workflow_event.run_id);
    try testing.expectEqualStrings("already-running", (try decodeWorkflowToken(a, attached.token, &why)).workflow_id);
    const with_callback = try env.client.describeWorkflow(a, "already-running", "");
    try testing.expectEqual(1, with_callback.raw.callbacks.items.len);
    try testing.expectEqualStrings("temporal://system", with_callback.raw.callbacks.items[0].callback.?.variant.?.nexus.url);
}

test "a rejected workflow start or cancel maps to a handler error by its gRPC status" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Case = struct { code: ?client_mod.RpcCode, type: HandlerErrorType, retryable: bool };
    const cases = [_]Case{
        .{ .code = .invalid_argument, .type = .bad_request, .retryable = false },
        .{ .code = .already_exists, .type = .internal, .retryable = false },
        .{ .code = .failed_precondition, .type = .internal, .retryable = false },
        .{ .code = .out_of_range, .type = .internal, .retryable = false },
        .{ .code = .aborted, .type = .unavailable, .retryable = true },
        .{ .code = .unavailable, .type = .unavailable, .retryable = true },
        .{ .code = .resource_exhausted, .type = .resource_exhausted, .retryable = true },
        .{ .code = .unimplemented, .type = .not_implemented, .retryable = false },
        .{ .code = .deadline_exceeded, .type = .upstream_timeout, .retryable = true },
        .{ .code = .not_found, .type = .not_found, .retryable = false },
        .{ .code = .internal, .type = .internal, .retryable = true },
        .{ .code = .permission_denied, .type = .internal, .retryable = true },
        .{ .code = null, .type = .internal, .retryable = true },
    };
    for (cases) |case| {
        const rejected: ?client_mod.RpcRejection = if (case.code) |code| .{ .code = code, .message = "workflow ID exceeds the length limit" } else null;
        const e = rpcError(a, "starting workflow \"W\" failed", error.RpcFailed, rejected);
        try testing.expectEqual(case.type, e.type);
        try testing.expectEqual(case.retryable, e.retryable());
        if (case.code) |code| {
            try testing.expect(std.mem.indexOf(u8, e.message, @tagName(code)) != null);
            try testing.expect(std.mem.indexOf(u8, e.message, "workflow ID exceeds the length limit") != null);
        }
    }
    // Errors from before the call aren't retried; the status only matters for RpcFailed.
    for ([_]anyerror{ error.InvalidOptions, error.InvalidSearchAttribute, error.WorkflowExecutionAlreadyStarted }) |err| {
        const e = rpcError(a, "w", err, .{ .code = .invalid_argument, .message = "" });
        try testing.expectEqual(HandlerErrorType.internal, e.type);
        try testing.expect(!e.retryable());
    }
    try testing.expectEqual(HandlerErrorType.not_found, rpcError(a, "w", error.NotFound, null).type);
    try testing.expect(rpcError(a, "w", error.Canceled, null).retryable());
}

test "compile errors name the operation, the service, and what to fix" {
    const expectMessage = struct {
        fn f(comptime message: ?[]const u8, comptime parts: []const []const u8) !void {
            const m = message orelse return error.TestExpectedCompileError;
            inline for (parts) |part| if (std.mem.indexOf(u8, m, part) == null) {
                std.debug.print("message {s}\n  lacks {s}\n", .{ m, part });
                return error.TestUnexpectedMessage;
            };
        }
    }.f;
    // Contracts.
    try expectMessage(comptime operationDefError("", void, void), &.{ "OperationDef(\"\"", "operation name is empty" });
    try expectMessage(comptime operationDefError("op", *anyopaque, void), &.{ "OperationDef(\"op\"", "input type", "payload converter" });
    try expectMessage(comptime operationDefError("op", void, error{A}), &.{ "OperationDef(\"op\"", "output type", "decoded" });
    try testing.expect(comptime operationDefError("op", EchoInput, EchoOutput) == null);
    try expectMessage(comptime serviceDefError("", .{Echo}), &.{"service name is empty"});
    try expectMessage(comptime serviceDefError("svc", .{}), &.{ "nexus service \"svc\" has no operations", "OperationDef" });
    try expectMessage(comptime serviceDefError("svc", .{ Echo, OperationDef("echo", void, void) }), &.{ "nexus service \"svc\"", "two operations named \"echo\"" });
    try expectMessage(comptime serviceDefError("svc", .{ Echo, u32 }), &.{ "nexus service \"svc\"", "element 1", "not a nexus.OperationDef" });
    try expectMessage(comptime serviceDefError("svc", Echo), &.{ "nexus service \"svc\"", "tuple" });
    try testing.expect(comptime serviceDefError("svc", .{ Echo, Ping }) == null);

    // Handlers.
    const Svc = ServiceDef("svc", .{ Echo, Ping });
    const H = struct {
        fn echo(_: *Context, i: EchoInput) !EchoOutput {
            return .{ .message = i.message, .length = 0 };
        }
        fn ping(_: *Context) !void {}
        fn wrongInput(_: *Context, _: []const u8) !EchoOutput {
            return undefined;
        }
        fn wrongOutput(_: *Context, _: EchoInput) ![]const u8 {
            return "";
        }
        fn noInput(_: *Context) !EchoOutput {
            return undefined;
        }
        fn noContext(_: EchoInput) !EchoOutput {
            return undefined;
        }
        fn wrongWorkflow(_: *Context, _: EchoInput) !WorkflowStart(WaitingWorkflow.Def) {
            return undefined;
        }
        const A = struct {
            fn echo(_: *@This(), _: *Context, i: EchoInput) !EchoOutput {
                return .{ .message = i.message, .length = 0 };
            }
        };
        const B = struct {
            fn ping(_: *@This(), _: *Context) !void {}
        };
    };
    try testing.expect(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.echo), OperationHandler(Ping, H.ping) }) == null);
    try testing.expect(comptime serviceHandlerError(Svc, .{ OperationHandler(Ping, H.ping), OperationHandler(Echo, H.A.echo) }) == null);
    try expectMessage(comptime serviceHandlerError(Echo, .{}), &.{ "ServiceHandler takes a nexus.ServiceDef", "nexus.OperationDef" });
    try expectMessage(comptime serviceHandlerError(Svc, .{OperationHandler(Echo, H.echo)}), &.{ "nexus service \"svc\" operation \"ping\" has no handler", "OperationHandler(" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.echo), OperationHandler(Ping, H.ping), OperationHandler(Shout, TestHandlers.shout) }), &.{ "nexus service \"svc\" has no operation \"shout\"", "\"echo\", \"ping\"" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.echo), OperationHandler(Ping, H.ping), OperationHandler(Ping, H.ping) }), &.{"nexus service \"svc\" operation \"ping\" has two handlers"});
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(OperationDef("echo", []const u8, EchoOutput), H.wrongInput), OperationHandler(Ping, H.ping) }), &.{ "nexus service \"svc\" operation \"echo\" takes", "bound to an OperationDef of that name" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.wrongInput), OperationHandler(Ping, H.ping) }), &.{ "nexus service \"svc\" operation \"echo\"", "input type []const u8 is not the operation's input type" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.wrongOutput), OperationHandler(Ping, H.ping) }), &.{ "nexus service \"svc\" operation \"echo\"", "returns []const u8, not the operation's output type" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.noInput), OperationHandler(Ping, H.ping) }), &.{ "nexus service \"svc\" operation \"echo\"", "takes no input" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.noContext), OperationHandler(Ping, H.ping) }), &.{ "nexus service \"svc\" operation \"echo\"", "*nexus.Context first" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.wrongWorkflow), OperationHandler(Ping, H.ping) }), &.{ "nexus service \"svc\" operation \"echo\"", "starts workflow \"NexusWaiting\"", "result type []const u8", "output type" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.A.echo), OperationHandler(Ping, H.B.ping) }), &.{ "nexus service \"svc\"", "one service handler binds one receiver" });
    try expectMessage(comptime serviceHandlerError(Svc, .{ OperationHandler(Echo, H.echo), u8 }), &.{ "nexus service \"svc\"", "element 1", "not a nexus.OperationHandler" });
    try expectMessage(comptime serviceHandlerError(Svc, OperationHandler(Echo, H.echo)), &.{ "nexus service \"svc\"", "tuple" });
}

test {
    _ = @import("nexus/wire.zig");
}
