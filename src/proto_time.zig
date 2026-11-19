//! Time values as protobuf carries them: conversion between std.Io.Duration/Timestamp and
//! google.protobuf.Duration/Timestamp, the proleptic Gregorian calendar arithmetic that their
//! text forms need, and the RFC 3339 text of a protobuf Timestamp. Pure and deterministic, so
//! safe on any thread and from workflow code.
const std = @import("std");
const pb = @import("proto.zig").google.protobuf;

/// A day of the proleptic Gregorian calendar. `month` is 1 to 12 and `day` is 1 to 31.
pub const CivilDate = struct { year: i64, month: u8, day: u8 };

// Days from 0000-03-01, where Hinnant's eras start, to 1970-01-01.
const days_to_unix_epoch = 719_468;
const days_per_era = 146_097;

/// The date `days` days after 1970-01-01 (negative means before), by Howard Hinnant's
/// civil_from_days with the correct divisors: 1460, 36524 and 146096.
/// Exact for every `days` in [-(1 << 60), 1 << 60].
pub fn civilFromDays(days: i64) CivilDate {
    // Outside the documented range the arithmetic would overflow i64; i128 keeps every
    // input total, and the year still fits i64 for any i64 day count.
    const z: i128 = @as(i128, days) + days_to_unix_epoch;
    const era: i128 = @divFloor(z, days_per_era);
    const doe: u32 = @intCast(z - era * days_per_era); // [0, 146096]
    const yoe: u32 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    const doy: u32 = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    const mp: u32 = (5 * doy + 2) / 153; // [0, 11], March first
    const day: u32 = doy - (153 * mp + 2) / 5 + 1;
    const month: u32 = if (mp < 10) mp + 3 else mp - 9;
    const year: i128 = @as(i128, yoe) + era * 400 + @intFromBool(month <= 2);
    return .{ .year = @intCast(year), .month = @intCast(month), .day = @intCast(day) };
}

/// Days from 1970-01-01 to `year`-`month`-`day`. The caller checks that `month` is 1..12
/// and that `day` exists in that month; an impossible date gives the day count of the date
/// it overflows into, never a panic. A year so far out that its day count does not fit i64
/// gives the nearest i64.
pub fn daysFromCivil(year: i64, month: u8, day: u8) i64 {
    const y: i128 = @as(i128, year) - @intFromBool(month <= 2);
    const era: i128 = @divFloor(y, 400);
    const yoe: i128 = y - era * 400; // [0, 399]
    const m: i128 = month;
    const doy: i128 = @divFloor(153 * (if (m > 2) m - 3 else m + 9) + 2, 5) + @as(i128, day) - 1;
    const doe: i128 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const days = era * days_per_era + doe - days_to_unix_epoch;
    return std.math.cast(i64, days) orelse if (days < 0) std.math.minInt(i64) else std.math.maxInt(i64);
}

/// The seconds of the earliest and latest protobuf Timestamp: 0001-01-01T00:00:00Z and
/// 9999-12-31T23:59:59Z.
pub const min_timestamp_seconds: i64 = -62_135_596_800;
pub const max_timestamp_seconds: i64 = 253_402_300_799;

/// The longest text `formatTimestamp` writes: "9999-12-31T23:59:59.999999999Z".
pub const timestamp_text_max = 30;

/// Writes `t` in UTC as ProtoJSON does: "YYYY-MM-DDTHH:MM:SSZ", with 3, 6 or 9 fraction
/// digits when `nanos` is not zero, using the fewest of those that holds the value exactly.
/// No quotes. The text is a slice of `buf`.
/// Returns error.OutOfRange when `seconds` is outside
/// [min_timestamp_seconds, max_timestamp_seconds] or `nanos` is outside [0, 999_999_999].
pub fn formatTimestamp(t: pb.Timestamp, buf: *[timestamp_text_max]u8) error{OutOfRange}![]const u8 {
    if (t.seconds < min_timestamp_seconds or t.seconds > max_timestamp_seconds) return error.OutOfRange;
    if (t.nanos < 0 or t.nanos > 999_999_999) return error.OutOfRange;
    const date = civilFromDays(@divFloor(t.seconds, std.time.s_per_day));
    const second_of_day: u32 = @intCast(@mod(t.seconds, std.time.s_per_day));
    writeDigits(buf[0..4], @intCast(date.year));
    buf[4] = '-';
    writeDigits(buf[5..7], date.month);
    buf[7] = '-';
    writeDigits(buf[8..10], date.day);
    buf[10] = 'T';
    writeDigits(buf[11..13], second_of_day / 3600);
    buf[13] = ':';
    writeDigits(buf[14..16], second_of_day / 60 % 60);
    buf[16] = ':';
    writeDigits(buf[17..19], second_of_day % 60);
    var len: usize = 19;
    const nanos: u32 = @intCast(t.nanos);
    if (nanos != 0) {
        buf[len] = '.';
        len += 1;
        const digits: usize = if (nanos % 1_000_000 == 0) 3 else if (nanos % 1_000 == 0) 6 else 9;
        const scale: u32 = std.math.pow(u32, 10, @intCast(9 - digits));
        writeDigits(buf[len..][0..digits], nanos / scale);
        len += digits;
    }
    buf[len] = 'Z';
    return buf[0 .. len + 1];
}

/// The longest protobuf Duration: 315,576,000,000 s plus 999,999,999 ns (about 10,000
/// years). `min_duration` is its negation.
pub const max_duration_seconds: i64 = 315_576_000_000;
pub const max_duration: std.Io.Duration = .{ .nanoseconds = @as(i96, max_duration_seconds) * std.time.ns_per_s + 999_999_999 };
pub const min_duration: std.Io.Duration = .{ .nanoseconds = -max_duration.nanoseconds };

/// The earliest and latest protobuf Timestamp, as std.Io.Timestamps.
pub const min_timestamp: std.Io.Timestamp = .{ .nanoseconds = @as(i96, min_timestamp_seconds) * std.time.ns_per_s };
pub const max_timestamp: std.Io.Timestamp = .{ .nanoseconds = @as(i96, max_timestamp_seconds) * std.time.ns_per_s + 999_999_999 };

/// `d` as a protobuf Duration, or null for null. A value beyond `max_duration` or
/// `min_duration` is clamped to that bound; anything in between is exact. `seconds` and
/// `nanos` carry the same sign, using truncating division: -1.5 s is
/// `{ .seconds = -1, .nanos = -500_000_000 }`. Never panics.
pub fn durationToProto(d: ?std.Io.Duration) ?pb.Duration {
    const v = d orelse return null;
    // Clamped in i96 first, so both casts below are in range.
    const ns = std.math.clamp(v.nanoseconds, min_duration.nanoseconds, max_duration.nanoseconds);
    return .{
        .seconds = @intCast(@divTrunc(ns, std.time.ns_per_s)),
        .nanos = @intCast(@rem(ns, std.time.ns_per_s)),
    };
}

/// `t` as a protobuf Timestamp, or null for null. A value before `min_timestamp` or after
/// `max_timestamp` is clamped to that bound. `nanos` is in [0, 999_999_999] (floor
/// division). Never panics.
pub fn timestampToProto(t: ?std.Io.Timestamp) ?pb.Timestamp {
    const v = t orelse return null;
    const ns = std.math.clamp(v.nanoseconds, min_timestamp.nanoseconds, max_timestamp.nanoseconds);
    return .{
        .seconds = @intCast(@divFloor(ns, std.time.ns_per_s)),
        .nanos = @intCast(@mod(ns, std.time.ns_per_s)),
    };
}

/// The protobuf value as std.Io's type, or null for null. Exact, with no range check: i64
/// seconds times 10^9 plus i32 nanos always fits i96, so this never fails or panics,
/// whatever the server or core sends.
pub fn durationFromProto(d: ?pb.Duration) ?std.Io.Duration {
    const v = d orelse return null;
    return .{ .nanoseconds = @as(i96, v.seconds) * std.time.ns_per_s + v.nanos };
}

/// See `durationFromProto`.
pub fn timestampFromProto(t: ?pb.Timestamp) ?std.Io.Timestamp {
    const v = t orelse return null;
    return .{ .nanoseconds = @as(i96, v.seconds) * std.time.ns_per_s + v.nanos };
}

/// Like `durationFromProto`, but null also for zero or negative. This is for fields where
/// the server sends zero to mean unset.
pub fn positiveDurationFromProto(d: ?pb.Duration) ?std.Io.Duration {
    const v = durationFromProto(d) orelse return null;
    return if (v.nanoseconds > 0) v else null;
}

/// Writes `value` as exactly `out.len` decimal digits, zero-padded; `value` must fit.
fn writeDigits(out: []u8, value: u32) void {
    var v = value;
    var i = out.len;
    while (i > 0) {
        i -= 1;
        out[i] = '0' + @as(u8, @intCast(v % 10));
        v /= 10;
    }
}

const testing = std.testing;

fn expectFormat(want: []const u8, seconds: i64, nanos: i32) !void {
    var buf: [timestamp_text_max]u8 = undefined;
    try testing.expectEqualStrings(want, try formatTimestamp(.{ .seconds = seconds, .nanos = nanos }, &buf));
}

test "civilFromDays and daysFromCivil agree on every day from 0001 to 9999" {
    const first = daysFromCivil(1, 1, 1);
    const last = daysFromCivil(9999, 12, 31);
    try testing.expectEqual(@divExact(min_timestamp_seconds, std.time.s_per_day), first);
    try testing.expectEqual(@divFloor(max_timestamp_seconds, std.time.s_per_day), last);
    var previous: CivilDate = .{ .year = 0, .month = 12, .day = 31 };
    var d = first;
    while (d <= last) : (d += 1) {
        const date = civilFromDays(d);
        try testing.expectEqual(d, daysFromCivil(date.year, date.month, date.day));
        try testing.expect(date.month >= 1 and date.month <= 12);
        const year: std.time.epoch.Year = @intCast(date.year);
        try testing.expect(date.day >= 1 and date.day <= std.time.epoch.getDaysInMonth(year, @fromBackingInt(@intCast(date.month))));
        // Each day follows the previous one in the calendar.
        if (date.day == 1) {
            try testing.expect(previous.month != date.month);
        } else {
            try testing.expectEqual(previous.month, date.month);
            try testing.expectEqual(previous.day + 1, date.day);
        }
        previous = date;
    }
    try testing.expectEqual(CivilDate{ .year = 9999, .month = 12, .day = 31 }, previous);
}

test "civilFromDays on the last day of each leap cycle and at the ends of its range" {
    try testing.expectEqual(CivilDate{ .year = 1970, .month = 1, .day = 1 }, civilFromDays(0));
    try testing.expectEqual(CivilDate{ .year = 2024, .month = 2, .day = 29 }, civilFromDays(daysFromCivil(2024, 2, 29)));
    try testing.expectEqual(CivilDate{ .year = 2000, .month = 2, .day = 29 }, civilFromDays(11_016));
    try testing.expectEqual(CivilDate{ .year = 1969, .month = 12, .day = 31 }, civilFromDays(-1));
    for ([_]i64{ -(1 << 60), 1 << 60, std.math.minInt(i64), std.math.maxInt(i64) }) |d| {
        const date = civilFromDays(d);
        if (d >= -(1 << 60) and d <= 1 << 60) try testing.expectEqual(d, daysFromCivil(date.year, date.month, date.day));
    }
}

test "daysFromCivil of an impossible date or an extreme year does not panic" {
    try testing.expectEqual(daysFromCivil(2023, 3, 1), daysFromCivil(2023, 2, 29));
    try testing.expectEqual(daysFromCivil(2024, 5, 1), daysFromCivil(2024, 4, 31));
    _ = daysFromCivil(2024, 0, 0);
    _ = daysFromCivil(2024, 255, 255);
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), daysFromCivil(std.math.maxInt(i64), 12, 31));
    try testing.expectEqual(@as(i64, std.math.minInt(i64)), daysFromCivil(std.math.minInt(i64), 1, 1));
}

test "formatTimestamp writes RFC 3339 with 0, 3, 6 or 9 fraction digits" {
    try expectFormat("1970-01-01T00:00:00Z", 0, 0);
    try expectFormat("2024-02-29T12:00:00Z", 1_709_208_000, 0);
    try expectFormat("2100-02-28T00:00:00Z", 4_107_456_000, 0);
    try expectFormat("2000-02-29T00:00:00Z", 951_782_400, 0);
    try expectFormat("9996-02-29T23:59:59.999999999Z", 253_281_254_399, 999_999_999);
    try expectFormat("0001-01-01T00:00:00Z", min_timestamp_seconds, 0);
    try expectFormat("9999-12-31T23:59:59.999999999Z", max_timestamp_seconds, 999_999_999);
    try expectFormat("1969-12-31T23:59:59.500Z", -1, 500_000_000);
    try expectFormat("2020-07-30T00:30:03.082421843Z", 1_596_069_003, 82_421_843);
    try expectFormat("2020-07-30T00:30:03.082421Z", 1_596_069_003, 82_421_000);
    try expectFormat("2020-07-30T00:30:03.000001Z", 1_596_069_003, 1_000);
    try expectFormat("2020-07-30T00:30:03.000000001Z", 1_596_069_003, 1);
}

test "formatTimestamp refuses a Timestamp outside protobuf's range" {
    var buf: [timestamp_text_max]u8 = undefined;
    for ([_]pb.Timestamp{
        .{ .seconds = min_timestamp_seconds - 1 },
        .{ .seconds = max_timestamp_seconds + 1 },
        .{ .seconds = std.math.minInt(i64) },
        .{ .seconds = std.math.maxInt(i64) },
        .{ .nanos = -1 },
        .{ .nanos = 1_000_000_000 },
        .{ .nanos = std.math.minInt(i32) },
    }) |t| try testing.expectError(error.OutOfRange, formatTimestamp(t, &buf));
}

test "the protobuf Duration and Timestamp conversions clamp going out and are exact coming in" {
    // Durations clamp to protobuf's range, with seconds and nanos of the same sign.
    {
        const max: pb.Duration = .{ .seconds = 315_576_000_000, .nanos = 999_999_999 };
        const min: pb.Duration = .{ .seconds = -315_576_000_000, .nanos = -999_999_999 };
        try testing.expectEqual(max, durationToProto(std.Io.Duration.max).?);
        try testing.expectEqual(max, durationToProto(max_duration).?);
        try testing.expectEqual(max, durationToProto(.{ .nanoseconds = max_duration.nanoseconds + 1 }).?);
        try testing.expectEqual(min, durationToProto(.{ .nanoseconds = std.math.minInt(i96) }).?);
        try testing.expectEqual(min, durationToProto(min_duration).?);
        try testing.expectEqual(pb.Duration{ .seconds = 315_575_999_999, .nanos = 999_999_999 }, durationToProto(.{ .nanoseconds = max_duration.nanoseconds - std.time.ns_per_s }).?);
        try testing.expectEqual(pb.Duration{ .seconds = 0, .nanos = -1 }, durationToProto(.fromNanoseconds(-1)).?);
        try testing.expectEqual(pb.Duration{ .seconds = -1, .nanos = -500_000_000 }, durationToProto(.fromMilliseconds(-1500)).?);
        try testing.expectEqual(pb.Duration{ .seconds = 1, .nanos = 500_000_000 }, durationToProto(.fromMilliseconds(1500)).?);
        try testing.expectEqual(pb.Duration{}, durationToProto(.zero).?);
        try testing.expectEqual(@as(?pb.Duration, null), durationToProto(null));
    }
    // Timestamps clamp to 0001-01-01 and 9999-12-31, with floored nanos.
    {
        try testing.expectEqual(pb.Timestamp{ .seconds = max_timestamp_seconds, .nanos = 999_999_999 }, timestampToProto(.{ .nanoseconds = std.math.maxInt(i96) }).?);
        try testing.expectEqual(pb.Timestamp{ .seconds = max_timestamp_seconds, .nanos = 999_999_999 }, timestampToProto(max_timestamp).?);
        try testing.expectEqual(pb.Timestamp{ .seconds = min_timestamp_seconds }, timestampToProto(.{ .nanoseconds = std.math.minInt(i96) }).?);
        try testing.expectEqual(pb.Timestamp{ .seconds = min_timestamp_seconds }, timestampToProto(min_timestamp).?);
        try testing.expectEqual(pb.Timestamp{ .seconds = min_timestamp_seconds }, timestampToProto(.{ .nanoseconds = min_timestamp.nanoseconds - 1 }).?);
        try testing.expectEqual(pb.Timestamp{ .seconds = -1, .nanos = 999_999_999 }, timestampToProto(.{ .nanoseconds = -1 }).?);
        try testing.expectEqual(pb.Timestamp{ .seconds = 1_709_208_000, .nanos = 5 }, timestampToProto(.{ .nanoseconds = 1_709_208_000 * std.time.ns_per_s + 5 }).?);
        try testing.expectEqual(@as(?pb.Timestamp, null), timestampToProto(null));
        // Every clamped value is one formatTimestamp writes.
        var buf: [timestamp_text_max]u8 = undefined;
        _ = try formatTimestamp(timestampToProto(.{ .nanoseconds = std.math.maxInt(i96) }).?, &buf);
        _ = try formatTimestamp(timestampToProto(.{ .nanoseconds = std.math.minInt(i96) }).?, &buf);
    }
    // Coming in, the conversions are exact for extreme seconds and nanos.
    {
        const big: i96 = @as(i96, std.math.maxInt(i64)) * std.time.ns_per_s + std.math.maxInt(i32);
        const small: i96 = @as(i96, std.math.minInt(i64)) * std.time.ns_per_s + std.math.minInt(i32);
        try testing.expectEqual(big, durationFromProto(.{ .seconds = std.math.maxInt(i64), .nanos = std.math.maxInt(i32) }).?.nanoseconds);
        try testing.expectEqual(small, durationFromProto(.{ .seconds = std.math.minInt(i64), .nanos = std.math.minInt(i32) }).?.nanoseconds);
        try testing.expectEqual(big, timestampFromProto(.{ .seconds = std.math.maxInt(i64), .nanos = std.math.maxInt(i32) }).?.nanoseconds);
        try testing.expectEqual(small, timestampFromProto(.{ .seconds = std.math.minInt(i64), .nanos = std.math.minInt(i32) }).?.nanoseconds);
        try testing.expectEqual(@as(?std.Io.Duration, null), durationFromProto(null));
        try testing.expectEqual(@as(?std.Io.Timestamp, null), timestampFromProto(null));
        try testing.expectEqual(big, positiveDurationFromProto(.{ .seconds = std.math.maxInt(i64), .nanos = std.math.maxInt(i32) }).?.nanoseconds);
        try testing.expectEqual(@as(?std.Io.Duration, null), positiveDurationFromProto(.{ .seconds = std.math.minInt(i64), .nanos = std.math.minInt(i32) }));
        try testing.expectEqual(@as(?std.Io.Duration, null), positiveDurationFromProto(.{}));
        try testing.expectEqual(@as(?std.Io.Duration, null), positiveDurationFromProto(.{ .seconds = 1, .nanos = -1_000_000_000 }));
        try testing.expectEqual(@as(i96, 1), positiveDurationFromProto(.{ .nanos = 1 }).?.nanoseconds);
        try testing.expectEqual(@as(?std.Io.Duration, null), positiveDurationFromProto(null));
    }
}
