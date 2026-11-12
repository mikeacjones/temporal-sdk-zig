//! Starts the search attributes workflow with CustomIntField set to 1 and waits for it. It
//! then describes the workflow to print the search attributes the server stores after the
//! workflow's upserts, and finds the workflow with a visibility query on them.

const std = @import("std");
const temporal = @import("temporal");
const searchattributes = @import("searchattributes.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(searchattributes.SearchAttributesWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "search_attributes_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "search-attributes",
        .search_attributes = &.{searchattributes.custom_int_field.set(1)},
        .memo = try temporal.converter.toFields(arena, .{ .description = "Test search attributes workflow" }),
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow completed", .{});

    const description = client.describeWorkflow(arena, run.id, run.run_id) catch |err| {
        std.log.err("Unable to describe workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    for (description.search_attributes.fields) |field| {
        std.log.info("Described search attribute: {s}={s}", .{ field.name, field.value.data });
    }
    const double = description.search_attributes.get(arena, searchattributes.custom_double_field) catch |err| {
        std.log.err("Unable to decode CustomDoubleField of workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    if (double) |value| {
        std.log.err("CustomDoubleField of workflow {s} is still {d}; the workflow unsets it", .{ run.id, value });
        std.process.exit(1);
    }
    std.log.info("Described search attribute: CustomDoubleField is unset", .{});

    // Visibility catches up with the workflow's last changes a moment after it closes.
    const query = try std.fmt.allocPrint(arena, "WorkflowId = '{s}' AND {s} = 2 AND ExecutionStatus = 'Completed'", .{
        run.id,
        searchattributes.custom_int_field.name,
    });
    var tries: usize = 0;
    while (true) : (tries += 1) {
        const count = client.countWorkflows(query) catch |err| {
            std.log.err("Unable to count workflows matching \"{s}\": {t}", .{ query, err });
            std.process.exit(1);
        };
        if (count == 1) break;
        if (tries == 100) {
            std.log.err("No workflow matched \"{s}\" after {d} tries", .{ query, tries });
            std.process.exit(1);
        }
        try io.sleep(.fromMilliseconds(100), .awake);
    }
    std.log.info("Visibility query found the completed workflow with CustomIntField=2", .{});
}
