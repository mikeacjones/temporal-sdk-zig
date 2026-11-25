//! Draws, in order:
//! - a slice into a 512-byte buffer, the link's URL;
//! - an `eos` (1 byte): false gives the link the type of a workflow-event link, true
//!   draws the type next;
//! - when the `eos` was true, a slice into a 64-byte buffer, the link's type (the seeds name
//!   the activity and Nexus-operation link types there).
//!
//! A link that `fromNexusLink` rejects has a reason. One that converts holds UTF-8 in its
//! string fields, and goes through `toNexusLink` and `fromNexusLink` again to an equal link of
//! the same Nexus type.

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
    try std.testing.expectEqualStrings(nexus_link.type, back.type);
    try std.testing.expectEqualDeep(first, try wire.fromNexusLink(a, back, &why));
}

/// Whether the string fields of a converted link are UTF-8, as protobuf string fields must
/// be.
fn stringsAreUtf8(link: Link) bool {
    const strings: [3][]const u8, const request_id: []const u8 = switch (link.variant.?) {
        .workflow_event => |event| .{ .{ event.namespace, event.workflow_id, event.run_id }, if (event.reference) |ref| switch (ref) {
            .event_ref => "",
            .request_id_ref => |r| r.request_id,
        } else "" },
        .activity => |l| .{ .{ l.namespace, l.activity_id, l.run_id }, "" },
        .nexus_operation => |l| .{ .{ l.namespace, l.operation_id, l.run_id }, "" },
        // `fromNexusLink` makes no other kind.
        else => return false,
    };
    for (strings ++ [_][]const u8{request_id}) |s| {
        if (!std.unicode.utf8ValidateSlice(s)) return false;
    }
    return true;
}
