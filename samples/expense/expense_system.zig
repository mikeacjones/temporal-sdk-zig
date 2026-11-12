//! A sample expense system: lists expenses, creates them, records decisions and payments, and
//! reports each expense's state. When a person approves or rejects an expense whose decision
//! an activity is waiting for, it completes that activity through the Temporal client.
//!
//! Run as a program, it serves the expense system with a page at
//! http://127.0.0.1:8099/list for approving and rejecting expenses by hand. The starter also
//! hosts it in-process, so a sample run needs no third program.
//!
//! Endpoints (all GET except registerCallback, which takes a POSTed form):
//!   /, /list                         the expenses, with approve and reject buttons
//!   /create?id=ID                    creates an expense
//!   /action?type=approve|reject|payment&id=ID
//!   /status?id=ID                    the expense's state
//!   /registerCallback?id=ID          form field task_token: the activity to complete with
//!                                    the decision
//! `is_api_call=true` makes /create and /action answer "SUCCEED" instead of the list page.
//! Errors are answered with "ERROR:..." bodies.

const std = @import("std");
const temporal = @import("temporal");
const expense = @import("expense.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const State = enum {
    created,
    approved,
    rejected,
    completed,

    pub fn label(s: State) []const u8 {
        return switch (s) {
            .created => "CREATED",
            .approved => "APPROVED",
            .rejected => "REJECTED",
            .completed => "COMPLETED",
        };
    }
};

pub const Response = struct {
    content_type: []const u8 = "text/plain",
    body: []const u8,
};

/// The expense system's in-memory state. Safe to use from several tasks at once: each
/// connection `serve` accepts is handled in its own task.
pub const ExpenseSystem = struct {
    gpa: Allocator,
    io: Io,
    /// Completes the activities waiting for decisions. Must outlive the system.
    client: *temporal.client.Client,
    mutex: Io.Mutex = .init,
    /// Broadcast whenever a callback is registered.
    callback_registered: Io.Condition = .init,
    /// Keys and token values are owned by `gpa`.
    expenses: std.StringArrayHashMapUnmanaged(State) = .empty,
    tokens: std.StringArrayHashMapUnmanaged([]u8) = .empty,

    pub fn init(gpa: Allocator, io: Io, client: *temporal.client.Client) ExpenseSystem {
        return .{ .gpa = gpa, .io = io, .client = client };
    }

    pub fn deinit(s: *ExpenseSystem) void {
        for (s.expenses.keys()) |id| s.gpa.free(id);
        s.expenses.deinit(s.gpa);
        for (s.tokens.keys(), s.tokens.values()) |id, token| {
            s.gpa.free(id);
            s.gpa.free(token);
        }
        s.tokens.deinit(s.gpa);
    }

    /// Answers one request: `target` is the request's path and query, `form` its
    /// form-encoded body ("" for none). The response is allocated from `arena`.
    pub fn handle(s: *ExpenseSystem, arena: Allocator, target: []const u8, form: []const u8) !Response {
        const path_end = std.mem.findScalar(u8, target, '?') orelse target.len;
        const path = target[0..path_end];
        const query = if (path_end < target.len) target[path_end + 1 ..] else "";

        if (std.mem.eql(u8, path, "/") or std.mem.eql(u8, path, "/list")) return s.list(arena);
        if (std.mem.eql(u8, path, "/create")) return s.create(arena, query);
        if (std.mem.eql(u8, path, "/action")) return s.action(arena, query);
        if (std.mem.eql(u8, path, "/status")) return s.status(arena, query);
        if (std.mem.eql(u8, path, "/registerCallback")) return s.registerCallback(arena, query, form);
        return .{ .body = "ERROR:NOT_FOUND" };
    }

    /// Blocks until a callback is registered for expense `id`, that is, until an activity is
    /// waiting for its decision.
    pub fn waitForCallback(s: *ExpenseSystem, id: []const u8) Io.Cancelable!void {
        try s.mutex.lock(s.io);
        defer s.mutex.unlock(s.io);
        while (!s.tokens.contains(id)) try s.callback_registered.wait(s.io, &s.mutex);
    }

    /// Serves HTTP on `listener` until canceled, handling each connection in its own task.
    pub fn serve(s: *ExpenseSystem, listener: *Io.net.Server) Io.net.Server.AcceptError!void {
        var group: Io.Group = .init;
        defer group.cancel(s.io);
        while (true) {
            const stream = try listener.accept(s.io);
            // Without a task of its own, a kept-alive connection would hold up the others.
            group.concurrent(s.io, serveConnection, .{ s, stream }) catch try serveConnection(s, stream);
        }
    }

    fn serveConnection(s: *ExpenseSystem, stream: Io.net.Stream) Io.Cancelable!void {
        defer stream.close(s.io);
        var recv_buffer: [8192]u8 = undefined;
        var send_buffer: [4096]u8 = undefined;
        var conn_reader = stream.reader(s.io, &recv_buffer);
        var conn_writer = stream.writer(s.io, &send_buffer);
        var server = std.http.Server.init(&conn_reader.interface, &conn_writer.interface);
        while (server.reader.state == .ready) {
            var request = server.receiveHead() catch |err| switch (err) {
                error.HttpConnectionClosing => return,
                else => return std.log.warn("closing an expense system connection: {t}", .{err}),
            };
            s.serveRequest(&request) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return std.log.warn("unable to answer an expense system request: {t}", .{err}),
            };
        }
    }

    fn serveRequest(s: *ExpenseSystem, request: *std.http.Server.Request) !void {
        var arena_state: std.heap.ArenaAllocator = .init(s.gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        // Reading the body invalidates the head's strings.
        const target = try arena.dupe(u8, request.head.target);
        var form: []const u8 = "";
        if (request.head.method == .POST) {
            var body_buffer: [1024]u8 = undefined;
            const body = try request.readerExpectContinue(&body_buffer);
            form = try body.allocRemaining(arena, .limited(64 * 1024));
        }
        const response = try s.handle(arena, target, form);
        try request.respond(response.body, .{ .extra_headers = &.{.{ .name = "content-type", .value = response.content_type }} });
    }

    fn list(s: *ExpenseSystem, arena: Allocator) !Response {
        try s.mutex.lock(s.io);
        defer s.mutex.unlock(s.io);
        var page: Io.Writer.Allocating = .init(arena);
        const w = &page.writer;
        try w.writeAll("<h1>SAMPLE EXPENSE SYSTEM</h1><a href=\"/list\">HOME</a>" ++
            "<h3>All expense requests:</h3><table border=1><tr><th>Expense ID</th><th>Status</th><th>Action</th>");
        const ids = try arena.dupe([]const u8, s.expenses.keys());
        std.mem.sort([]const u8, ids, {}, lessThan);
        for (ids) |id| {
            const state = s.expenses.get(id).?;
            try w.print("<tr><td>{s}</td><td>{s}</td><td>", .{ id, state.label() });
            if (state == .created) try w.print("<a href=\"/action?type=approve&id={s}\">" ++
                "<button style=\"background-color:#4CAF50;\">APPROVE</button></a>" ++
                "&nbsp;&nbsp;<a href=\"/action?type=reject&id={s}\">" ++
                "<button style=\"background-color:#f44336;\">REJECT</button></a>", .{ id, id });
            try w.writeAll("</td></tr>");
        }
        try w.writeAll("</table>");
        return .{ .content_type = "text/html", .body = page.written() };
    }

    fn lessThan(_: void, a: []const u8, b: []const u8) bool {
        return std.mem.lessThan(u8, a, b);
    }

    fn create(s: *ExpenseSystem, arena: Allocator, query: []const u8) !Response {
        const id = try formValue(arena, query, "id") orelse "";
        {
            try s.mutex.lock(s.io);
            defer s.mutex.unlock(s.io);
            const entry = try s.expenses.getOrPut(s.gpa, id);
            if (entry.found_existing) return .{ .body = "ERROR:ID_ALREADY_EXISTS" };
            entry.key_ptr.* = s.gpa.dupe(u8, id) catch |err| {
                s.expenses.swapRemoveAt(entry.index);
                return err;
            };
            entry.value_ptr.* = .created;
        }
        std.log.info("Created new expense id:{s}.", .{id});
        if (try isApiCall(arena, query)) return .{ .body = "SUCCEED" };
        return s.list(arena);
    }

    fn action(s: *ExpenseSystem, arena: Allocator, query: []const u8) !Response {
        const id = try formValue(arena, query, "id") orelse "";
        const action_type = try formValue(arena, query, "type") orelse "";
        var old_state: State = undefined;
        var new_state: State = undefined;
        {
            try s.mutex.lock(s.io);
            defer s.mutex.unlock(s.io);
            const state = s.expenses.getPtr(id) orelse return .{ .body = "ERROR:INVALID_ID" };
            old_state = state.*;
            if (std.mem.eql(u8, action_type, "approve")) {
                state.* = .approved;
            } else if (std.mem.eql(u8, action_type, "reject")) {
                state.* = .rejected;
            } else if (std.mem.eql(u8, action_type, "payment")) {
                state.* = .completed;
            }
            new_state = state.*;
        }
        if (old_state == .created and (new_state == .approved or new_state == .rejected)) {
            try s.notifyExpenseStateChange(arena, id, new_state);
        }
        std.log.info("Set state for {s} from {s} to {s}.", .{ id, old_state.label(), new_state.label() });
        if (try isApiCall(arena, query)) return .{ .body = "SUCCEED" };
        return s.list(arena);
    }

    fn status(s: *ExpenseSystem, arena: Allocator, query: []const u8) !Response {
        const id = try formValue(arena, query, "id") orelse "";
        try s.mutex.lock(s.io);
        defer s.mutex.unlock(s.io);
        const state = s.expenses.get(id) orelse return .{ .body = "ERROR:INVALID_ID" };
        std.log.info("Checking status for {s}: {s}", .{ id, state.label() });
        return .{ .body = state.label() };
    }

    fn registerCallback(s: *ExpenseSystem, arena: Allocator, query: []const u8, form: []const u8) !Response {
        const id = try formValue(arena, query, "id") orelse "";
        const token = try formValue(arena, form, "task_token") orelse return .{ .body = "ERROR:INVALID_FORM_DATA" };
        {
            try s.mutex.lock(s.io);
            defer s.mutex.unlock(s.io);
            const state = s.expenses.get(id) orelse return .{ .body = "ERROR:INVALID_ID" };
            if (state != .created) return .{ .body = "ERROR:INVALID_STATE" };

            const owned_token = try s.gpa.dupe(u8, token);
            errdefer s.gpa.free(owned_token);
            const entry = try s.tokens.getOrPut(s.gpa, id);
            if (entry.found_existing) {
                s.gpa.free(entry.value_ptr.*);
            } else {
                entry.key_ptr.* = s.gpa.dupe(u8, id) catch |err| {
                    s.tokens.swapRemoveAt(entry.index);
                    return err;
                };
            }
            entry.value_ptr.* = owned_token;
            s.callback_registered.broadcast(s.io);
        }
        std.log.info("Registered callback for ID={s}", .{id});
        return .{ .body = "SUCCEED" };
    }

    /// Completes the activity waiting for expense `id`'s decision with `state`'s label. A
    /// failure to complete it is logged, not returned: the decision itself is recorded.
    fn notifyExpenseStateChange(s: *ExpenseSystem, arena: Allocator, id: []const u8, state: State) !void {
        const token = blk: {
            try s.mutex.lock(s.io);
            defer s.mutex.unlock(s.io);
            const token = s.tokens.get(id) orelse {
                std.log.info("No activity waits for a decision on expense id:{s}", .{id});
                return;
            };
            break :blk try arena.dupe(u8, token);
        };
        s.client.completeActivity(token, state.label()) catch |err| {
            // Warn rather than error: a stale or unknown token is the caller's input, not a
            // failure of the expense system.
            std.log.warn("Failed to complete the activity for expense {s} with {s}: {t}", .{ id, state.label(), err });
            return;
        };
        std.log.info("Successfully completed the activity for expense {s} with {s}", .{ id, state.label() });
    }
};

fn isApiCall(arena: Allocator, query: []const u8) !bool {
    const value = try formValue(arena, query, "is_api_call") orelse return false;
    return std.mem.eql(u8, value, "true");
}

/// The decoded value of the first `name` field in form-encoded `fields`, or null if there is
/// none. Allocated from `arena`.
fn formValue(arena: Allocator, fields: []const u8, name: []const u8) !?[]const u8 {
    var it = std.mem.splitScalar(u8, fields, '&');
    while (it.next()) |field| {
        const eq = std.mem.findScalar(u8, field, '=') orelse field.len;
        if (!std.mem.eql(u8, field[0..eq], name)) continue;
        const value = try arena.dupe(u8, if (eq < field.len) field[eq + 1 ..] else "");
        std.mem.replaceScalar(u8, value, '+', ' ');
        return std.Uri.percentDecodeInPlace(value);
    }
    return null;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var system: ExpenseSystem = .init(gpa, io, &client);
    defer system.deinit();
    const address = try Io.net.IpAddress.parse(expense.expense_server_host, expense.expense_server_port);
    var listener = address.listen(io, .{ .reuse_address = true }) catch |err| {
        std.log.err("Unable to listen on {s}:{d}: {t}", .{ expense.expense_server_host, expense.expense_server_port, err });
        std.process.exit(1);
    };
    defer listener.deinit(io);
    std.log.info("Expense system UI available at http://{s}:{d}/list", .{ expense.expense_server_host, expense.expense_server_port });
    system.serve(&listener) catch |err| {
        std.log.err("Expense system stopped serving: {t}", .{err});
        std.process.exit(1);
    };
}
