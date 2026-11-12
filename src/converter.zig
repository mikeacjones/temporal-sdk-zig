//! Payload conversion.
//!
//! Values cross the wire as `temporal.api.common.v1.Payload`: bytes plus an `encoding`
//! metadata entry. The default converter uses Temporal's standard encodings, so workers and
//! clients in any language understand each other:
//!
//! | Zig value                         | encoding          | data                       |
//! |-----------------------------------|-------------------|----------------------------|
//! | `void`, `null`                    | `binary/null`     | empty                      |
//! | `converter.Bytes`                 | `binary/plain`    | the bytes                  |
//! | a generated protobuf message      | `json/protobuf`   | ProtoJSON                  |
//! | `converter.ProtoBinary(T)`        | `binary/protobuf` | the message's wire format  |
//! | anything else (strings included)  | `json/plain`      | JSON from `std.json`       |
//!
//! Strings are JSON strings; use `Bytes` for raw binary data. Decoding
//! accepts any of these encodings into a compatible type: a protobuf message decodes from
//! `json/protobuf` and `binary/protobuf`. Protobuf payloads also carry a `messageType`
//! metadata entry with the message's full name (see `Proto`). Decoded slices and strings are
//! allocated from the arena passed in.
//!
//! A type converts itself by declaring both of
//!
//!     pub fn toPayload(self: T, arena: Allocator) error{OutOfMemory}!Payload
//!     pub fn fromPayload(arena: Allocator, p: Payload) DecodeError!T
//!
//! which take precedence over the defaults, for the type and for optionals of it. For
//! example, a point sent as 8 little-endian bytes:
//!
//!     const Point = struct {
//!         x: i32,
//!         y: i32,
//!
//!         pub fn toPayload(p: Point, arena: Allocator) error{OutOfMemory}!converter.Payload {
//!             const data = try arena.alloc(u8, 8);
//!             std.mem.writeInt(i32, data[0..4], p.x, .little);
//!             std.mem.writeInt(i32, data[4..8], p.y, .little);
//!             return converter.payloadWithEncoding(arena, "binary/point", data);
//!         }
//!
//!         pub fn fromPayload(_: Allocator, p: converter.Payload) converter.DecodeError!Point {
//!             const enc = converter.encodingOf(p) orelse return error.UnknownEncoding;
//!             if (!std.mem.eql(u8, enc, "binary/point")) return error.UnknownEncoding;
//!             if (p.data.len != 8) return error.InvalidPayload;
//!             return .{
//!                 .x = std.mem.readInt(i32, p.data[0..4], .little),
//!                 .y = std.mem.readInt(i32, p.data[4..8], .little),
//!             };
//!         }
//!     };
//!
//! Conversion turns values into payloads; payload codecs (`PayloadCodec`) then transform
//! the payloads themselves, for example to compress or encrypt them. `codecHandler` serves
//! codecs over HTTP, so the Web UI and CLI can show payloads only the codecs can decode.

const std = @import("std");
const proto = @import("proto.zig");
const Allocator = std.mem.Allocator;

pub const Payload = proto.temporal.api.common.v1.Payload;
pub const Payloads = proto.temporal.api.common.v1.Payloads;

const codec = @import("converter/codec.zig");
pub const CodecError = codec.CodecError;
pub const PayloadCodec = codec.PayloadCodec;
pub const ZlibCodec = codec.ZlibCodec;
pub const encodePayloads = codec.encodePayloads;
pub const decodePayloads = codec.decodePayloads;
pub const encodeMessage = codec.encodeMessage;
pub const decodeMessage = codec.decodeMessage;
pub const visitPayloads = codec.visitPayloads;
const codec_handler = @import("converter/codec_handler.zig");
pub const CodecHandler = codec_handler.CodecHandler;
pub const CodecHandlerOptions = codec_handler.CodecHandlerOptions;
pub const codecHandler = codec_handler.codecHandler;

/// Raw bytes, encoded as `binary/plain` instead of a JSON string.
pub const Bytes = struct { data: []const u8 };

pub const encoding_key = "encoding";
/// Metadata key of a protobuf payload's full message name, such as
/// "temporal.api.common.v1.WorkflowExecution".
pub const message_type_key = "messageType";

pub const Encoding = enum {
    null,
    binary,
    json,
    proto_json,
    proto_binary,

    pub fn text(e: Encoding) []const u8 {
        return switch (e) {
            .null => "binary/null",
            .binary => "binary/plain",
            .json => "json/plain",
            .proto_json => "json/protobuf",
            .proto_binary => "binary/protobuf",
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
    if (comptime hasPayloadHooks(T)) return value.toPayload(arena);
    if (T == Bytes) return payloadWith(arena, .binary, value.data);
    if (comptime isProtoMessage(T)) return Proto(T, .{}).encode(arena, value);
    const json = try std.json.Stringify.valueAlloc(arena, value, .{});
    return payloadWith(arena, .json, json);
}

/// Decodes one value of type `T`. Memory comes from `arena`.
pub fn fromPayload(comptime T: type, arena: Allocator, p: Payload) DecodeError!T {
    if (comptime hasPayloadHooks(T)) return T.fromPayload(arena, p);
    const enc = encodingOf(p) orelse return error.UnknownEncoding;
    if (std.mem.eql(u8, enc, Encoding.null.text())) {
        if (T == void) return {};
        if (@typeInfo(T) == .optional) return null;
        return error.InvalidPayload;
    }
    if (@typeInfo(T) == .optional) {
        const Child = @typeInfo(T).optional.child;
        if (comptime hasPayloadHooks(Child) or isProtoMessage(Child)) return try fromPayload(Child, arena, p);
    }
    if (comptime isProtoMessage(T)) return Proto(T, .{}).decode(arena, p);
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

/// Arguments not yet decoded, as a dynamic workflow, activity, or signal handler receives
/// them: the handler reads each one as the type it expects. A view: the payloads are owned by
/// the run or task that delivered them and live as long as its memory (`ctx.allocator()`);
/// decoded values come from the arena passed in.
pub const EncodedValues = struct {
    payloads: []const Payload,

    pub fn len(v: EncodedValues) usize {
        return v.payloads.len;
    }

    /// Decodes the argument at `index` as `T`. Past the last argument, an optional `T`
    /// decodes as null and anything else returns `error.WrongArgumentCount`, as in
    /// `decode`.
    pub fn get(v: EncodedValues, comptime T: type, arena: Allocator, index: usize) DecodeError!T {
        if (index < v.payloads.len) return fromPayload(T, arena, v.payloads[index]);
        if (@typeInfo(T) == .optional) return null;
        return error.WrongArgumentCount;
    }

    /// Decodes every argument into the tuple `Args`, by the rules for typed arguments:
    /// missing trailing optionals are null, extra arguments are ignored.
    pub fn decode(v: EncodedValues, comptime Args: type, arena: Allocator) DecodeError!Args {
        return fromPayloads(Args, arena, v.payloads);
    }
};

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

/// Turns the entries of a proto map of payloads, such as `Memo.fields` or
/// `SearchAttributes.indexed_fields` (each entry has `key` and `value: ?Payload`), into
/// fields in wire order. An entry without a value becomes an empty payload. The slice comes
/// from `arena`; names and payloads borrow the entries.
pub fn fieldsFromMap(arena: Allocator, entries: anytype) error{OutOfMemory}![]Field {
    const out = try arena.alloc(Field, entries.len);
    for (entries, out) |entry, *field| field.* = .{ .name = entry.key, .value = entry.value orelse .{} };
    return out;
}

fn payloadWith(arena: Allocator, enc: Encoding, data: []const u8) error{OutOfMemory}!Payload {
    return payloadWithEncoding(arena, enc.text(), data);
}

/// A payload of `data` with the `encoding` metadata entry `encoding`, for `toPayload` hooks
/// and payload codecs. Borrows `data` and `encoding`; the metadata list comes from `arena`.
pub fn payloadWithEncoding(arena: Allocator, encoding: []const u8, data: []const u8) error{OutOfMemory}!Payload {
    var p: Payload = .{ .data = data };
    try p.metadata.append(arena, .{ .key = encoding_key, .value = encoding });
    return p;
}

fn hasPayloadHooks(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union", .@"enum", .@"opaque" => std.meta.hasFn(T, "toPayload") and std.meta.hasFn(T, "fromPayload"),
        else => false,
    };
}

/// Whether `T` is a message type generated by zig-protobuf (detected by its descriptor table
/// and generated codec functions).
pub fn isProtoMessage(comptime T: type) bool {
    return @typeInfo(T) == .@"struct" and @hasDecl(T, "_desc_table") and
        std.meta.hasFn(T, "encode") and std.meta.hasFn(T, "decode") and std.meta.hasFn(T, "jsonEncode");
}

/// The full protobuf name of a generated message type, derived from where the generator put
/// it: generated files mirror the proto package (`temporal/api/common/v1.pb.zig` for
/// `temporal.api.common.v1`), so the name is the type's Zig name without its first segment
/// (the module or directory the generated files are imported through) and without the file's
/// `pb` segment: `proto.temporal.api.common.v1.pb.Payload` is
/// "temporal.api.common.v1.Payload". When the generated files are imported some other way, set
/// `ProtoOptions.message_type`.
pub fn protoMessageType(comptime T: type) []const u8 {
    return comptime blk: {
        const name = @typeName(T);
        const pb = std.mem.indexOf(u8, name, ".pb.") orelse break :blk name;
        const first = std.mem.indexOfScalar(u8, name, '.').?;
        if (first == pb) break :blk name[pb + ".pb.".len ..];
        break :blk name[first + 1 .. pb] ++ name[pb + ".pb".len ..];
    };
}

pub const ProtoOptions = struct {
    encoding: enum { json, binary } = .json,
    /// The `messageType` metadata value; null derives it with `protoMessageType`.
    message_type: ?[]const u8 = null,
};

/// A protobuf message `T` converted with `options`: wrap a message to choose its encoding or
/// name, `Proto(Order, .{ .encoding = .binary }){ .message = order }`, and take the wrapper
/// type as a parameter or result to decode one. Decoding accepts both protobuf encodings
/// whatever `options` says. Unwrapped messages convert as `Proto(T, .{})`.
pub fn Proto(comptime T: type, comptime options: ProtoOptions) type {
    if (!isProtoMessage(T)) @compileError("Proto takes a message type generated by zig-protobuf, not " ++ @typeName(T));
    return struct {
        message: T,

        const Self = @This();
        const message_type = options.message_type orelse protoMessageType(T);

        pub fn toPayload(self: Self, arena: Allocator) error{OutOfMemory}!Payload {
            return encode(arena, self.message);
        }

        pub fn fromPayload(arena: Allocator, p: Payload) DecodeError!Self {
            return .{ .message = try decode(arena, p) };
        }

        fn encode(arena: Allocator, message: T) error{OutOfMemory}!Payload {
            const enc: Encoding, const data = switch (options.encoding) {
                // Standard ProtoJSON writes a oneof's fields flat, without the oneof's name.
                .json => .{ .proto_json, message.jsonEncode(.{}, .{ .emit_oneof_field_name = false }, arena) catch return error.OutOfMemory },
                .binary => blk: {
                    var out: std.Io.Writer.Allocating = .init(arena);
                    message.encode(&out.writer, arena) catch return error.OutOfMemory;
                    break :blk .{ .proto_binary, out.written() };
                },
            };
            var p = try payloadWith(arena, enc, data);
            try p.metadata.append(arena, .{ .key = message_type_key, .value = message_type });
            return p;
        }

        fn decode(arena: Allocator, p: Payload) DecodeError!T {
            const enc = encodingOf(p) orelse return error.UnknownEncoding;
            if (std.mem.eql(u8, enc, Encoding.proto_json.text())) {
                return std.json.parseFromSliceLeaky(T, arena, p.data, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                }) catch |err| switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                    else => error.InvalidPayload,
                };
            }
            if (std.mem.eql(u8, enc, Encoding.proto_binary.text())) {
                // Decoded messages may borrow the input, so it is copied into the arena.
                var reader: std.Io.Reader = .fixed(try arena.dupe(u8, p.data));
                return T.decode(&reader, arena) catch |err| switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                    else => error.InvalidPayload,
                };
            }
            return error.UnknownEncoding;
        }
    };
}

/// `Proto(T, .{ .encoding = .binary })`: the message encodes as `binary/protobuf`.
pub fn ProtoBinary(comptime T: type) type {
    return Proto(T, .{ .encoding = .binary });
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test {
    _ = codec;
    _ = codec_handler;
}

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

test "fields from a proto map keep wire order and decode by name" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var memo: proto.temporal.api.common.v1.Memo = .{};
    try memo.fields.append(a, .{ .key = "description", .value = try toPayload(a, "nightly import") });
    try memo.fields.append(a, .{ .key = "empty" });
    const fields = try fieldsFromMap(a, memo.fields.items);
    try testing.expectEqual(@as(usize, 2), fields.len);
    try testing.expectEqualStrings("description", fields[0].name);
    try testing.expectEqualStrings("nightly import", (try fieldValue([]const u8, a, fields, "description")).?);
    try testing.expectEqualStrings("empty", fields[1].name);
    try testing.expectEqual(@as(usize, 0), fields[1].value.data.len);
    try testing.expectEqual(@as(usize, 0), (try fieldsFromMap(a, memo.fields.items[0..0])).len);
}

test "EncodedValues decodes arguments one at a time or as a tuple" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ps = try toPayloads(a, .{ @as([]const u8, "x"), @as(i64, 42) });
    const values: EncodedValues = .{ .payloads = ps.items };
    try testing.expectEqual(@as(usize, 2), values.len());
    try testing.expectEqualStrings("x", try values.get([]const u8, a, 0));
    try testing.expectEqual(@as(i64, 42), try values.get(i64, a, 1));
    try testing.expectEqual(@as(?bool, null), try values.get(?bool, a, 2));
    try testing.expectError(error.WrongArgumentCount, values.get(bool, a, 2));
    try testing.expectError(error.InvalidPayload, values.get(i64, a, 0));
    const args = try values.decode(struct { []const u8, i64 }, a);
    try testing.expectEqual(@as(i64, 42), args[1]);
    try testing.expectError(error.InvalidPayload, values.decode(struct { i64, i64 }, a));
}

/// The example from the file's header: a type with its own payload conversion.
const Point = struct {
    x: i32,
    y: i32,

    pub fn toPayload(p: Point, arena: Allocator) error{OutOfMemory}!Payload {
        const data = try arena.alloc(u8, 8);
        std.mem.writeInt(i32, data[0..4], p.x, .little);
        std.mem.writeInt(i32, data[4..8], p.y, .little);
        return payloadWithEncoding(arena, "binary/point", data);
    }

    pub fn fromPayload(_: Allocator, p: Payload) DecodeError!Point {
        const enc = encodingOf(p) orelse return error.UnknownEncoding;
        if (!std.mem.eql(u8, enc, "binary/point")) return error.UnknownEncoding;
        if (p.data.len != 8) return error.InvalidPayload;
        return .{
            .x = std.mem.readInt(i32, p.data[0..4], .little),
            .y = std.mem.readInt(i32, p.data[4..8], .little),
        };
    }
};

test "a type's own toPayload and fromPayload take precedence" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const p = try toPayload(a, Point{ .x = -3, .y = 7 });
    try testing.expectEqualStrings("binary/point", encodingOf(p).?);
    try testing.expectEqualSlices(u8, &.{ 0xfd, 0xff, 0xff, 0xff, 7, 0, 0, 0 }, p.data);
    try testing.expectEqual(Point{ .x = -3, .y = 7 }, try fromPayload(Point, a, p));

    // Optionals of the type use the hooks too; null stays binary/null.
    const some = try toPayload(a, @as(?Point, .{ .x = 1, .y = 2 }));
    try testing.expectEqualStrings("binary/point", encodingOf(some).?);
    try testing.expectEqual(@as(?Point, .{ .x = 1, .y = 2 }), try fromPayload(?Point, a, some));
    try testing.expectEqual(@as(?Point, null), try fromPayload(?Point, a, try toPayload(a, @as(?Point, null))));

    // A hook sees every payload, so it decides which encodings it accepts.
    try testing.expectError(error.UnknownEncoding, fromPayload(Point, a, try toPayload(a, @as(u32, 1))));
    const args = try fromPayloads(struct { Point, []const u8 }, a, (try toPayloads(a, .{ Point{ .x = 5, .y = 6 }, @as([]const u8, "z") })).items);
    try testing.expectEqual(@as(i32, 6), args[0].y);
}

test "protobuf messages round-trip as json/protobuf and binary/protobuf" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Failure = proto.temporal.api.failure.v1.Failure;

    var cause: Failure = .{ .message = "root cause" };
    const f: Failure = .{
        .message = "boom",
        .cause = &cause,
        .failure_info = .{ .application_failure_info = .{ .type = "Oops", .non_retryable = true } },
    };

    const json = try toPayload(a, f);
    try testing.expectEqualStrings("json/protobuf", encodingOf(json).?);
    try testing.expectEqualStrings("messageType", json.metadata.items[1].key);
    try testing.expectEqualStrings("temporal.api.failure.v1.Failure", json.metadata.items[1].value);
    // Standard ProtoJSON: camelCase names and the oneof's field written flat.
    try testing.expectEqualStrings(
        "{\"message\":\"boom\",\"cause\":{\"message\":\"root cause\"},\"applicationFailureInfo\":{\"type\":\"Oops\",\"nonRetryable\":true}}",
        json.data,
    );
    const from_json = try fromPayload(Failure, a, json);
    try testing.expectEqualStrings("boom", from_json.message);
    try testing.expectEqualStrings("root cause", from_json.cause.?.message);
    try testing.expectEqualStrings("Oops", from_json.failure_info.?.application_failure_info.type);

    const binary = try toPayload(a, ProtoBinary(Failure){ .message = f });
    try testing.expectEqualStrings("binary/protobuf", encodingOf(binary).?);
    try testing.expectEqualStrings("temporal.api.failure.v1.Failure", binary.metadata.items[1].value);
    // Either encoding decodes into the message or its wrapper.
    for ([_]Payload{ json, binary }) |p| {
        const back = try fromPayload(ProtoBinary(Failure), a, p);
        try testing.expect(back.message.failure_info.?.application_failure_info.non_retryable);
        try testing.expectEqualStrings("root cause", (try fromPayload(?Failure, a, p)).?.cause.?.message);
    }
    try testing.expectEqual(@as(?Failure, null), try fromPayload(?Failure, a, try toPayload(a, {})));

    const named = try toPayload(a, Proto(Failure, .{ .message_type = "example.v1.Problem" }){ .message = f });
    try testing.expectEqualStrings("example.v1.Problem", named.metadata.items[1].value);
    try testing.expectEqualStrings("temporal.api.common.v1.Payload.MetadataEntry", protoMessageType(Payload.MetadataEntry));

    try testing.expectError(error.UnknownEncoding, fromPayload(Failure, a, try toPayload(a, @as([]const u8, "x"))));
    var corrupt = binary;
    corrupt.data = "\x0a\x0aX"; // a 10-byte message field holding one byte
    try testing.expectError(error.InvalidPayload, fromPayload(Failure, a, corrupt));
}
