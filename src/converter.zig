//! Payload conversion.
//!
//! Values cross the wire as `temporal.api.common.v1.Payload`: bytes plus an `encoding`
//! metadata entry. The default converter uses Temporal's standard encodings, so workers and
//! clients in any language understand each other:
//!
//! | Zig value                         | encoding      | data                    |
//! |-----------------------------------|---------------|-------------------------|
//! | `void`, `null`                    | `binary/null` | empty                   |
//! | `converter.Bytes`                 | `binary/plain`| the bytes               |
//! | anything else (strings included)  | `json/plain`  | JSON from `std.json`    |
//!
//! Strings are JSON strings; use `Bytes` for raw binary data. Decoding
//! accepts any of the three encodings into a compatible type. Decoded slices and strings are
//! allocated from the arena passed in.

const std = @import("std");
const proto = @import("proto.zig");
const Allocator = std.mem.Allocator;

pub const Payload = proto.temporal.api.common.v1.Payload;
pub const Payloads = proto.temporal.api.common.v1.Payloads;

/// Raw bytes, encoded as `binary/plain` instead of a JSON string.
pub const Bytes = struct { data: []const u8 };

pub const encoding_key = "encoding";

pub const Encoding = enum {
    null,
    binary,
    json,

    pub fn text(e: Encoding) []const u8 {
        return switch (e) {
            .null => "binary/null",
            .binary => "binary/plain",
            .json => "json/plain",
        };
    }
};

pub const DecodeError = error{ OutOfMemory, UnknownEncoding, InvalidPayload, WrongArgumentCount };

/// The payload's `encoding` metadata value, or null if it has none.
pub fn encodingOf(p: Payload) ?[]const u8 {
    for (p.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, encoding_key)) return entry.value;
    }
    return null;
}

/// Encodes one value. Memory comes from `arena`.
pub fn toPayload(arena: Allocator, value: anytype) error{OutOfMemory}!Payload {
    const T = @TypeOf(value);
    if (T == void or T == @TypeOf(null)) return payloadWith(arena, .null, "");
    if (@typeInfo(T) == .optional) {
        if (value) |v| return toPayload(arena, v);
        return payloadWith(arena, .null, "");
    }
    if (T == Bytes) return payloadWith(arena, .binary, value.data);
    const json = try std.json.Stringify.valueAlloc(arena, value, .{});
    return payloadWith(arena, .json, json);
}

/// Decodes one value of type `T`. Memory comes from `arena`.
pub fn fromPayload(comptime T: type, arena: Allocator, p: Payload) DecodeError!T {
    const enc = encodingOf(p) orelse return error.UnknownEncoding;
    if (std.mem.eql(u8, enc, Encoding.null.text())) {
        if (T == void) return {};
        if (@typeInfo(T) == .optional) return null;
        return error.InvalidPayload;
    }
    if (std.mem.eql(u8, enc, Encoding.binary.text())) {
        if (T == Bytes) return .{ .data = try arena.dupe(u8, p.data) };
        if (T == []const u8 or T == []u8) return arena.dupe(u8, p.data);
        return error.InvalidPayload;
    }
    if (std.mem.eql(u8, enc, Encoding.json.text())) {
        if (T == void) return {};
        return std.json.parseFromSliceLeaky(T, arena, p.data, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.InvalidPayload,
        };
    }
    return error.UnknownEncoding;
}

/// Encodes each element of the tuple `args` (workflow and activity arguments).
pub fn toPayloads(arena: Allocator, args: anytype) error{OutOfMemory}!std.ArrayList(Payload) {
    var out: std.ArrayList(Payload) = .empty;
    inline for (args) |arg| try out.append(arena, try toPayload(arena, arg));
    return out;
}

/// Decodes payloads into a tuple of type `Args`. Missing trailing optional arguments decode
/// as null; extra payloads are ignored.
pub fn fromPayloads(comptime Args: type, arena: Allocator, payloads: []const Payload) DecodeError!Args {
    var out: Args = undefined;
    inline for (@typeInfo(Args).@"struct".field_types, 0..) |F, i| {
        if (i < payloads.len) {
            out[i] = try fromPayload(F, arena, payloads[i]);
        } else if (@typeInfo(F) == .optional) {
            out[i] = null;
        } else {
            return error.WrongArgumentCount;
        }
    }
    return out;
}

/// A named value, such as one entry of a workflow memo.
pub const Field = struct {
    name: []const u8,
    value: Payload,
};

/// Encodes each field of the struct `values` as a `Field`, in declaration order, so the
/// result is deterministic and usable from workflow code:
/// `toFields(arena, .{ .description = "nightly import", .owner = @as(?[]const u8, null) })`.
/// Null optional fields encode as `binary/null`. The slice and payload memory come from
/// `arena`, and the fields borrow it; names are the comptime field names.
pub fn toFields(arena: Allocator, values: anytype) error{OutOfMemory}![]Field {
    const info = @typeInfo(@TypeOf(values));
    if (info != .@"struct" or (info.@"struct".is_tuple and info.@"struct".field_names.len > 0))
        @compileError("toFields takes a struct with named fields, got " ++ @typeName(@TypeOf(values)));
    const names = info.@"struct".field_names;
    const out = try arena.alloc(Field, names.len);
    inline for (names, 0..) |name, i| {
        out[i] = .{ .name = name, .value = try toPayload(arena, @field(values, name)) };
    }
    return out;
}

/// Decodes the value of the field called `name` as `T`, or returns null if no field has that
/// name. If several fields share the name, the first wins. Memory comes from `arena`.
pub fn fieldValue(comptime T: type, arena: Allocator, fields: []const Field, name: []const u8) DecodeError!?T {
    for (fields) |field| {
        if (std.mem.eql(u8, field.name, name)) return try fromPayload(T, arena, field.value);
    }
    return null;
}

fn payloadWith(arena: Allocator, enc: Encoding, data: []const u8) error{OutOfMemory}!Payload {
    var p: Payload = .{ .data = data };
    try p.metadata.append(arena, .{ .key = encoding_key, .value = enc.text() });
    return p;
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

/// A point in time that encodes in JSON as an RFC 3339 string with up to nanosecond
/// precision, such as "2018-08-22T16:30:00Z", the common form for times in payloads.
pub const Time = struct {
    /// Nanoseconds since the Unix epoch.
    unix_ns: i96,

    /// The longest text `format` writes: "-YYYYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ" fits easily.
    pub const max_len = 40;

    pub fn fromTimestamp(ts: std.Io.Timestamp) Time {
        return .{ .unix_ns = ts.nanoseconds };
    }

    pub fn toTimestamp(t: Time) std.Io.Timestamp {
        return .{ .nanoseconds = t.unix_ns };
    }

    /// Writes `t` in UTC, with trailing zeros of the fraction dropped.
    pub fn format(t: Time, buf: *[max_len]u8) []const u8 {
        const secs: i64 = @intCast(@divFloor(t.unix_ns, std.time.ns_per_s));
        const nanos: u32 = @intCast(@mod(t.unix_ns, std.time.ns_per_s));
        const days = @divFloor(secs, std.time.s_per_day);
        const sod: u32 = @intCast(@mod(secs, std.time.s_per_day));
        const date = civilFromDays(days);
        var w: std.Io.Writer = .fixed(buf);
        w.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}", .{
            @as(u64, @intCast(@max(date.year, 0))), date.month, date.day, sod / 3600, sod / 60 % 60, sod % 60,
        }) catch unreachable;
        if (nanos != 0) {
            var frac: [9]u8 = undefined;
            _ = std.fmt.bufPrint(&frac, "{d:0>9}", .{nanos}) catch unreachable;
            const digits = std.mem.trimEnd(u8, &frac, "0");
            w.print(".{s}", .{digits}) catch unreachable;
        }
        w.writeByte('Z') catch unreachable;
        return w.buffered();
    }

    /// Parses an RFC 3339 time ("2006-01-02T15:04:05.999999999Z07:00").
    pub fn parse(text: []const u8) error{InvalidTime}!Time {
        if (text.len < 20 or text[4] != '-' or text[7] != '-' or (text[10] != 'T' and text[10] != 't') or text[13] != ':' or text[16] != ':')
            return error.InvalidTime;
        const year = try digitsOf(text[0..4]);
        const month = try digitsOf(text[5..7]);
        const day = try digitsOf(text[8..10]);
        const hour = try digitsOf(text[11..13]);
        const minute = try digitsOf(text[14..16]);
        const second = try digitsOf(text[17..19]);
        if (month < 1 or month > 12 or day < 1 or day > 31 or hour > 23 or minute > 59 or second > 60) return error.InvalidTime;
        var i: usize = 19;
        var nanos: i96 = 0;
        if (text[i] == '.') {
            i += 1;
            var scale: i96 = 100_000_000;
            const start = i;
            while (i < text.len and std.ascii.isDigit(text[i])) : (i += 1) {
                nanos += (text[i] - '0') * scale;
                scale = @divTrunc(scale, 10);
            }
            if (i == start) return error.InvalidTime;
        }
        if (i >= text.len) return error.InvalidTime;
        var offset_s: i64 = 0;
        switch (text[i]) {
            'Z', 'z' => i += 1,
            '+', '-' => {
                if (text.len < i + 6 or text[i + 3] != ':') return error.InvalidTime;
                const off = @as(i64, try digitsOf(text[i + 1 .. i + 3])) * 3600 + @as(i64, try digitsOf(text[i + 4 .. i + 6])) * 60;
                offset_s = if (text[i] == '-') -off else off;
                i += 6;
            },
            else => return error.InvalidTime,
        }
        if (i != text.len) return error.InvalidTime;
        const days = daysFromCivil(year, month, day);
        const secs = days * std.time.s_per_day + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + second - offset_s;
        return .{ .unix_ns = @as(i96, secs) * std.time.ns_per_s + nanos };
    }

    pub fn jsonStringify(t: Time, jws: anytype) !void {
        var buf: [max_len]u8 = undefined;
        try jws.write(t.format(&buf));
    }

    pub fn jsonParse(allocator: Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!Time {
        const text = try std.json.innerParse([]const u8, allocator, source, options);
        return parse(text) catch error.UnexpectedToken;
    }

    pub fn jsonParseFromValue(allocator: Allocator, source: std.json.Value, options: std.json.ParseOptions) std.json.ParseFromValueError!Time {
        _ = allocator;
        _ = options;
        if (source != .string) return error.UnexpectedToken;
        return parse(source.string) catch error.UnexpectedToken;
    }

    fn digitsOf(s: []const u8) error{InvalidTime}!u32 {
        return std.fmt.parseUnsigned(u32, s, 10) catch error.InvalidTime;
    }

    // Howard Hinnant's days_from_civil / civil_from_days, valid for the proleptic Gregorian
    // calendar in both directions from the epoch.
    fn daysFromCivil(y0: i64, m: u32, d: u32) i64 {
        const y = if (m <= 2) y0 - 1 else y0;
        const era = @divFloor(y, 400);
        const yoe = y - era * 400;
        const mp: i64 = if (m > 2) m - 3 else m + 9;
        const doy = @divFloor(153 * mp + 2, 5) + d - 1;
        const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
        return era * 146097 + doe - 719468;
    }

    fn civilFromDays(z0: i64) struct { year: i64, month: u32, day: u32 } {
        const z = z0 + 719468;
        const era = @divFloor(z, 146097);
        const doe = z - era * 146097;
        const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
        const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
        const mp = @divFloor(5 * doy + 2, 153);
        const day: u32 = @intCast(doy - @divFloor(153 * mp + 2, 5) + 1);
        const month: u32 = @intCast(if (mp < 10) mp + 3 else mp - 9);
        return .{ .year = yoe + era * 400 + @intFromBool(month <= 2), .month = month, .day = day };
    }
};

test "strings, structs and numbers encode as JSON" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const s = try toPayload(a, @as([]const u8, "Temporal"));
    try testing.expectEqualStrings("json/plain", encodingOf(s).?);
    try testing.expectEqualStrings("\"Temporal\"", s.data);
    try testing.expectEqualStrings("Temporal", try fromPayload([]const u8, a, s));

    const Order = struct { id: u32, items: []const []const u8 };
    const o = try toPayload(a, Order{ .id = 7, .items = &.{ "a", "b" } });
    try testing.expectEqualStrings("{\"id\":7,\"items\":[\"a\",\"b\"]}", o.data);
    const back = try fromPayload(Order, a, o);
    try testing.expectEqual(@as(u32, 7), back.id);
    try testing.expectEqualStrings("b", back.items[1]);
}

test "null, void and raw bytes use the binary encodings" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const n = try toPayload(a, {});
    try testing.expectEqualStrings("binary/null", encodingOf(n).?);
    try testing.expectEqual(@as(?u32, null), try fromPayload(?u32, a, n));
    try fromPayload(void, a, n);

    const b = try toPayload(a, Bytes{ .data = "\x00\x01" });
    try testing.expectEqualStrings("binary/plain", encodingOf(b).?);
    try testing.expectEqualSlices(u8, "\x00\x01", (try fromPayload(Bytes, a, b)).data);
    try testing.expectEqualSlices(u8, "\x00\x01", try fromPayload([]const u8, a, b));
}

test "argument tuples round-trip, with optional trailing arguments" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ps = try toPayloads(a, .{ @as([]const u8, "x"), @as(i64, 42) });
    const args = try fromPayloads(struct { []const u8, i64, ?bool }, a, ps.items);
    try testing.expectEqualStrings("x", args[0]);
    try testing.expectEqual(@as(i64, 42), args[1]);
    try testing.expectEqual(@as(?bool, null), args[2]);
    try testing.expectError(error.WrongArgumentCount, fromPayloads(struct { []const u8, i64, u8 }, a, ps.items));
}

test "bad payloads are reported, not crashed on" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectError(error.UnknownEncoding, fromPayload(u32, a, .{ .data = "1" }));
    const bad = try payloadWith(a, .json, "{not json");
    try testing.expectError(error.InvalidPayload, fromPayload(u32, a, bad));
    var odd: Payload = .{ .data = "x" };
    try odd.metadata.append(a, .{ .key = "encoding", .value = "json/protobuf" });
    try testing.expectError(error.UnknownEncoding, fromPayload(u32, a, odd));
}

test "Time encodes as RFC 3339 text" {
    var buf: [Time.max_len]u8 = undefined;
    try std.testing.expectEqualStrings("0001-01-01T00:00:00Z", (try Time.parse("0001-01-01T00:00:00Z")).format(&buf));
    const t = try Time.parse("2018-08-22T16:30:00Z");
    try std.testing.expectEqualStrings("2018-08-22T16:30:00Z", t.format(&buf));
    try std.testing.expectEqual(@as(i96, 1534955400) * std.time.ns_per_s, t.unix_ns);
    const frac = try Time.parse("2018-08-22T09:30:00.12-07:00");
    try std.testing.expectEqualStrings("2018-08-22T16:30:00.12Z", frac.format(&buf));
    try std.testing.expectError(error.InvalidTime, Time.parse("2018-08-22 16:30:00"));

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const S = struct { run_time: Time };
    const p = try toPayload(arena.allocator(), S{ .run_time = t });
    try std.testing.expectEqualStrings("{\"run_time\":\"2018-08-22T16:30:00Z\"}", p.data);
    try std.testing.expectEqual(t, (try fromPayload(S, arena.allocator(), p)).run_time);
}

test "fields encode in declaration order and decode by name" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const fields = try toFields(a, .{
        .description = "nightly import",
        .priority = 3,
        .owner = @as(?[]const u8, null),
        .raw = Bytes{ .data = "\x01" },
    });
    try testing.expectEqual(@as(usize, 4), fields.len);
    for (fields, [_][]const u8{ "description", "priority", "owner", "raw" }) |f, name|
        try testing.expectEqualStrings(name, f.name);
    try testing.expectEqualStrings("binary/null", encodingOf(fields[2].value).?);

    try testing.expectEqualStrings("nightly import", (try fieldValue([]const u8, a, fields, "description")).?);
    try testing.expectEqual(@as(?u8, 3), try fieldValue(u8, a, fields, "priority"));
    try testing.expectEqual(@as(??[]const u8, @as(?[]const u8, null)), try fieldValue(?[]const u8, a, fields, "owner"));
    try testing.expectEqualSlices(u8, "\x01", (try fieldValue(Bytes, a, fields, "raw")).?.data);
    try testing.expectEqual(@as(?u8, null), try fieldValue(u8, a, fields, "missing"));
    try testing.expectError(error.InvalidPayload, fieldValue(u8, a, fields, "description"));
    try testing.expectEqual(@as(usize, 0), (try toFields(a, .{})).len);
}
