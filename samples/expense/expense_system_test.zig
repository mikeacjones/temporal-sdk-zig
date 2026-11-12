//! Tests for the expense system's handlers, called directly and over HTTP.

const std = @import("std");
const temporal = @import("temporal");
const expense_system = @import("expense_system.zig");
const ExpenseSystem = expense_system.ExpenseSystem;

const testing = std.testing;

test "the expense system's handlers" {
    const io = testing.io;
    // The system needs a client to complete activities, though no activity waits here.
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, io);
    defer env.deinit();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var system: ExpenseSystem = .init(testing.allocator, io, &env.client);
    defer system.deinit();

    try testing.expectEqualStrings("SUCCEED", (try system.handle(arena, "/create?is_api_call=true&id=b", "")).body);
    try testing.expectEqualStrings("ERROR:ID_ALREADY_EXISTS", (try system.handle(arena, "/create?is_api_call=true&id=b", "")).body);
    // Without is_api_call, a person's request answers with the list page.
    const page = try system.handle(arena, "/create?id=a", "");
    try testing.expectEqualStrings("text/html", page.content_type);
    const a_row = std.mem.find(u8, page.body, "<td>a</td><td>CREATED</td>").?;
    try testing.expect(a_row < std.mem.find(u8, page.body, "<td>b</td><td>CREATED</td>").?);
    try testing.expect(std.mem.find(u8, page.body, "/action?type=approve&id=a") != null);

    try testing.expectEqualStrings("CREATED", (try system.handle(arena, "/status?id=a", "")).body);
    try testing.expectEqualStrings("ERROR:INVALID_ID", (try system.handle(arena, "/status?id=missing", "")).body);
    try testing.expectEqualStrings("ERROR:INVALID_ID", (try system.handle(arena, "/action?is_api_call=true&type=approve&id=missing", "")).body);
    try testing.expectEqualStrings("ERROR:INVALID_ID", (try system.handle(arena, "/registerCallback?id=missing", "task_token=t")).body);
    try testing.expectEqualStrings("ERROR:INVALID_FORM_DATA", (try system.handle(arena, "/registerCallback?id=a", "")).body);

    // The form's task token is percent-decoded; tokens are arbitrary bytes.
    try testing.expectEqualStrings("SUCCEED", (try system.handle(arena, "/registerCallback?id=a", "task_token=%00%FFtoken%2B")).body);
    try testing.expectEqualStrings("\x00\xfftoken+", system.tokens.get("a").?);
    try system.waitForCallback("a");

    // Deciding an expense no activity waits for only records the decision.
    try testing.expectEqualStrings("SUCCEED", (try system.handle(arena, "/action?is_api_call=true&type=approve&id=b", "")).body);
    try testing.expectEqualStrings("APPROVED", (try system.handle(arena, "/status?id=b", "")).body);
    try testing.expect(std.mem.find(u8, (try system.handle(arena, "/list", "")).body, "/action?type=approve&id=b") == null);
    try testing.expectEqualStrings("ERROR:INVALID_STATE", (try system.handle(arena, "/registerCallback?id=b", "task_token=t")).body);
    try testing.expectEqualStrings("SUCCEED", (try system.handle(arena, "/action?is_api_call=true&type=payment&id=b", "")).body);
    try testing.expectEqualStrings("COMPLETED", (try system.handle(arena, "/status?id=b", "")).body);
}

test "the expense system serves its handlers over HTTP" {
    const io = testing.io;
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, io);
    defer env.deinit();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var system: ExpenseSystem = .init(testing.allocator, io, &env.client);
    defer system.deinit();

    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var serving = try io.concurrent(ExpenseSystem.serve, .{ &system, &listener });
    defer serving.cancel(io) catch {};
    const port = listener.socket.address.getPort();

    var http: std.http.Client = .{ .allocator = testing.allocator, .io = io };
    defer http.deinit();
    const Fetch = struct {
        fn body(client: *std.http.Client, a: std.mem.Allocator, url: []const u8, form: ?[]const u8) ![]const u8 {
            var out: std.Io.Writer.Allocating = .init(a);
            const response = try client.fetch(.{ .location = .{ .url = url }, .payload = form, .response_writer = &out.writer });
            try testing.expectEqual(.ok, response.status);
            return out.written();
        }
    };

    const base = try std.fmt.allocPrint(arena, "http://127.0.0.1:{d}", .{port});
    try testing.expectEqualStrings("SUCCEED", try Fetch.body(&http, arena, try std.fmt.allocPrint(arena, "{s}/create?is_api_call=true&id=x", .{base}), null));
    try testing.expectEqualStrings("SUCCEED", try Fetch.body(&http, arena, try std.fmt.allocPrint(arena, "{s}/registerCallback?id=x", .{base}), "task_token=abc%20def"));
    try system.waitForCallback("x");
    try testing.expectEqualStrings("abc def", system.tokens.get("x").?);
    try testing.expectEqualStrings("CREATED", try Fetch.body(&http, arena, try std.fmt.allocPrint(arena, "{s}/status?id=x", .{base}), null));
    const page = try Fetch.body(&http, arena, try std.fmt.allocPrint(arena, "{s}/list", .{base}), null);
    try testing.expect(std.mem.find(u8, page, "<td>x</td><td>CREATED</td>") != null);
}

test "the expense system program compiles" {
    _ = &expense_system.main;
}
