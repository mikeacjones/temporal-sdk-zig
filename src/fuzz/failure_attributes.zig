//! Draws, in order, for each failure of a chain of at most 4, the root first:
//! - before every failure but the root, an `eos` (1 byte; true ends the chain);
//! - a slice into a 64-byte buffer, the message;
//! - a slice into a 64-byte buffer, the stack trace;
//! - an `eos` (1 byte; true means the failure has no encoded attributes) and, when false:
//!   - up to 2 metadata entries, each an `eos` (1 byte; true ends the list) followed by a
//!     key slice into a 16-byte buffer and a value slice into a 32-byte buffer; after the
//!     second entry no `eos` is drawn;
//!   - a slice into a 1024-byte buffer, the attributes' data.
//!
//! `failure.decodeAttributes` must not write the chain it is given. In the chain it leaves,
//! a failure that had no attributes is unchanged, and one whose attributes are still there
//! is left as received.

const std = @import("std");
const failure = @import("../failure.zig");
const converter = @import("../converter.zig");
const Failure = failure.Failure;
const Payload = converter.Payload;

const max_chain = 4;
const max_metadata = 2;

const Buffers = struct {
    message: [64]u8,
    stack_trace: [64]u8,
    keys: [max_metadata][16]u8,
    values: [max_metadata][32]u8,
    entries: [max_metadata]Payload.MetadataEntry,
    data: [1024]u8,
};

pub fn testOne(_: void, smith: *std.testing.Smith) anyerror!void {
    var buffers: [max_chain]Buffers = undefined;
    var chain: [max_chain]Failure = undefined;
    var len: usize = 0;
    while (len < max_chain and (len == 0 or !smith.eos())) : (len += 1) {
        chain[len] = draw(smith, &buffers[len]);
        if (len > 0) chain[len - 1].cause = &chain[len];
    }
    // Shallow copies: `cause` still points into `chain`, so comparing them deeply checks
    // every level as received.
    var received: [max_chain]Failure = undefined;
    @memcpy(received[0..len], chain[0..len]);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    var root = chain[0];
    const result: error{OutOfMemory}!void = failure.decodeAttributes(Failure, &root, arena.allocator());
    result catch |err| switch (err) {
        error.OutOfMemory => return,
    };

    for (chain[0..len], received[0..len]) |*original, *before| try std.testing.expectEqualDeep(before, original);

    var cur: ?*const Failure = &root;
    for (received[0..len]) |before| {
        const after = cur orelse return error.ChainShortened;
        if (before.encoded_attributes == null or after.encoded_attributes != null) {
            try std.testing.expectEqualStrings(before.message, after.message);
            try std.testing.expectEqualStrings(before.stack_trace, after.stack_trace);
            try std.testing.expectEqualDeep(before.encoded_attributes, after.encoded_attributes);
        }
        cur = after.cause;
    }
    if (cur != null) return error.ChainLengthened;
}

fn draw(smith: *std.testing.Smith, b: *Buffers) Failure {
    var f: Failure = .{
        .message = b.message[0..smith.slice(&b.message)],
        .stack_trace = b.stack_trace[0..smith.slice(&b.stack_trace)],
    };
    if (smith.eos()) return f;
    var count: usize = 0;
    while (count < max_metadata and !smith.eos()) : (count += 1) {
        b.entries[count] = .{
            .key = b.keys[count][0..smith.slice(&b.keys[count])],
            .value = b.values[count][0..smith.slice(&b.values[count])],
        };
    }
    f.encoded_attributes = .{
        .metadata = .fromOwnedSlice(b.entries[0..count]),
        .data = b.data[0..smith.slice(&b.data)],
    };
    return f;
}
