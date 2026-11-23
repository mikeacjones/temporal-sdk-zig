//! Draws, in order:
//! - a slice into a 512-byte buffer, the link's URL;
//! - an `eos` (1 byte): false gives the link the type of a workflow-event link, true
//!   draws the type next;
//! - when the `eos` was true, a slice into a 64-byte buffer, the link's type.
//!
//! A link that `fromNexusLink` rejects has a reason. One that converts holds UTF-8 in its
//! string fields, and goes through `toNexusLink` and `fromNexusLink` again to an equal link.

const std = @import("std");
const wire = @import("../nexus/wire.zig");
const proto = @import("../proto.zig");
const Link = proto.temporal.api.common.v1.Link;

pub fn testOne(_: void, smith: *std.testing.Smith) anyerror!void {
    var url: [512]u8 = undefined;
    var link_type: [64]u8 = undefined;
    const nexus_link: proto.temporal.api.nexus.v1.Link = .{
        .url = url[0..smith.slice(&url)],
        .type = if (smith.eos()) link_type[0..smith.slice(&link_type)] else wire.workflow_event_link_type,
    };

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var why: []const u8 = "";
    const result: wire.LinkError!Link = wire.fromNexusLink(a, nexus_link, &why);
    const first = result catch |err| switch (err) {
        error.OutOfMemory => return,
        error.InvalidLink => {
            if (why.len == 0) return error.InvalidLinkWithoutReason;
            return;
        },
    };
    if (!stringsAreUtf8(first)) return error.ConvertedLinkNotUtf8;

    const back = try wire.toNexusLink(a, first, &why);
    try std.testing.expectEqualStrings(wire.workflow_event_link_type, back.type);
    try std.testing.expectEqualDeep(first, try wire.fromNexusLink(a, back, &why));
}

/// Whether the namespace, workflow ID, run ID and request ID of a workflow-event link are
/// UTF-8, as protobuf string fields must be.
fn stringsAreUtf8(link: Link) bool {
    const event = link.variant.?.workflow_event;
    for ([_][]const u8{ event.namespace, event.workflow_id, event.run_id }) |s| {
        if (!std.unicode.utf8ValidateSlice(s)) return false;
    }
    const ref = event.reference orelse return true;
    return switch (ref) {
        .event_ref => true,
        .request_id_ref => |r| std.unicode.utf8ValidateSlice(r.request_id),
    };
}
