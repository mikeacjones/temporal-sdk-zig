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
