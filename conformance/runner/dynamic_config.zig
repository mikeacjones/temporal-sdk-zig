const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Entry = struct { key: []const u8, json_value: []const u8 };

/// Reads the suite's block mapping of lists of values with empty constraints.
/// Values must be JSON-compatible scalars. Entries borrow their strings from yaml.
pub fn parse(arena: Allocator, yaml: []const u8) error{ OutOfMemory, InvalidDynamicConfig }![]const Entry {
    var entries: std.ArrayList(Entry) = .empty;
    var keys: std.StringHashMapUnmanaged(void) = .empty;
    defer keys.deinit(arena);
    var key: []const u8 = "";
    var value: []const u8 = "";
    var state: enum { key, first_value, constraints, next_entry } = .key;
    var lines = std.mem.splitScalar(u8, yaml, '\n');
    var line_number: usize = 0;
    var last_line: []const u8 = "";
    var last_line_number: usize = 1;
    while (lines.next()) |raw| {
        line_number += 1;
        const line = std.mem.trimEnd(u8, withoutComment(raw), " \t\r");
        if (line.len == 0) continue;
        last_line = raw;
        last_line_number = line_number;

        if (state == .constraints) {
            const prefix = "    constraints: ";
            if (!std.mem.startsWith(u8, line, prefix) or
                !std.mem.eql(u8, std.mem.trimStart(u8, line[prefix.len..], " \t"), "{}"))
                return invalidLine(line_number, raw, "expected constraints: {}");
            try entries.append(arena, .{ .key = key, .json_value = value });
            state = .next_entry;
            continue;
        }

        if (state == .first_value or (state == .next_entry and line[0] == ' ')) {
            const prefix = "  - value: ";
            if (!std.mem.startsWith(u8, line, prefix))
                return invalidLine(line_number, raw, "expected an indented list item starting with - value:");
            value = std.mem.trimStart(u8, line[prefix.len..], " \t");
            if (value.len == 0 or value[0] == '[' or value[0] == '{' or !try std.json.validate(arena, value))
                return invalidLine(line_number, raw, "expected a JSON-compatible scalar value");
            state = .constraints;
            continue;
        }

        if (line[line.len - 1] != ':' or line.len == 1)
            return invalidLine(line_number, raw, "expected a plain setting key followed by a colon");
        key = line[0 .. line.len - 1];
        for (key) |c| {
            if (!std.ascii.isAlphanumeric(c) and c != '.' and c != '_' and c != '-')
                return invalidLine(line_number, raw, "expected a plain setting key");
        }
        if ((try keys.getOrPut(arena, key)).found_existing)
            return invalidLine(line_number, raw, "duplicate setting key");
        state = .first_value;
    }
    switch (state) {
        .key => return invalidLine(last_line_number, last_line, "expected at least one setting"),
        .first_value => return invalidLine(last_line_number, last_line, "setting has no list items"),
        .constraints => return invalidLine(last_line_number, last_line, "list item is missing constraints: {}"),
        .next_entry => {},
    }

    sortEntries(entries.items);
    return entries.items;
}

fn sortEntries(entries: []Entry) void {
    // Preserve the order of values within each setting's list.
    std.mem.sort(Entry, entries, {}, struct {
        fn lessThan(_: void, a: Entry, b: Entry) bool {
            return std.mem.lessThan(u8, a.key, b.key);
        }
    }.lessThan);
}

pub fn merge(arena: Allocator, defaults: []const Entry, overrides: []const Entry) error{OutOfMemory}![]const Entry {
    var entries: std.ArrayList(Entry) = try .initCapacity(arena, defaults.len + overrides.len);
    for (defaults) |entry| {
        for (overrides) |override| {
            if (std.mem.eql(u8, entry.key, override.key)) break;
        } else entries.appendAssumeCapacity(entry);
    }
    entries.appendSliceAssumeCapacity(overrides);
    sortEntries(entries.items);
    return entries.items;
}

fn withoutComment(line: []const u8) []const u8 {
    var quoted = false;
    var escaped = false;
    for (line, 0..) |c, i| {
        if (escaped) {
            escaped = false;
        } else if (c == '"') {
            quoted = !quoted;
        } else if (quoted and c == '\\') {
            escaped = true;
        } else if (!quoted and c == '#' and (i == 0 or line[i - 1] == ' ' or line[i - 1] == '\t')) {
            return line[0..i];
        }
    }
    return line;
}

fn invalidLine(number: usize, line: []const u8, reason: []const u8) error{InvalidDynamicConfig} {
    std.log.warn("invalid dynamic config at line {d}: {s} ({s})", .{ number, reason, line });
    return error.InvalidDynamicConfig;
}

pub fn serverArgs(arena: Allocator, entries: []const Entry) error{OutOfMemory}![]const []const u8 {
    const args = try arena.alloc([]const u8, 2 * entries.len);
    for (entries, 0..) |entry, i| {
        args[2 * i] = "--dynamic-config-value";
        args[2 * i + 1] = try std.fmt.allocPrint(arena, "{s}={s}", .{ entry.key, entry.json_value });
    }
    return args;
}

test "dynamic config passes every value to the dev server in key and list order" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const entries = try parse(arena,
        \\# Example settings
        \\worker.rate:
        \\  - value: 2.5
        \\    constraints: {}
        \\
        \\test.flag: # changes in order
        \\  - value: false
        \\    constraints: {}
        \\  - value: true
        \\    constraints: {}
        \\test.label:
        \\  - value: "say \"hi\" # now" # comment after the string
        \\    constraints: {}
    ++ "    \r\n");
    try std.testing.expectEqualDeep(@as([]const []const u8, &.{
        "--dynamic-config-value", "test.flag=false",
        "--dynamic-config-value", "test.flag=true",
        "--dynamic-config-value", "test.label=\"say \\\"hi\\\" # now\"",
        "--dynamic-config-value", "worker.rate=2.5",
    }), try serverArgs(arena, entries));
}

test "dynamic config rejects unsupported keys, values, constraints and incomplete lists" {
    const level = std.testing.log_level;
    defer std.testing.log_level = level;
    std.testing.log_level = .err;

    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const valid = "test.flag:\n  - value: true\n    constraints: {}\n";
    for ([_][]const u8{
        "",
        "test.flag: []",
        "test flag:\n  - value: true\n    constraints: {}",
        valid ++ valid,
        valid ++ "test.other:",
        "test.flag:\n    value: true",
        "test.flag:\n  - unknown: true\n    constraints: {}",
        "test.flag:\n  - value: [true]\n    constraints: {}",
        "test.flag:\n  - value: {\"flag\": true}\n    constraints: {}",
        "test.flag:\n  - value: true false\n    constraints: {}",
        "test.flag:\n  - value: true#not-a-comment\n    constraints: {}",
        "test.flag:\n  - value: true\n    constraints: {namespace: test}",
        "test.flag:\n  - value: true\n    constraints:{}",
        "test.flag:\n  - value: true\n    unknown: {}",
        "test.flag:\n  - value: true",
    }) |source| {
        try std.testing.expectError(error.InvalidDynamicConfig, parse(state.allocator(), source));
    }
}

test "dynamic config variants replace all values for overridden keys and preserve other values" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const defaults: []const Entry = &.{
        .{ .key = "b", .json_value = "1" },
        .{ .key = "b", .json_value = "2" },
        .{ .key = "c", .json_value = "8" },
        .{ .key = "c", .json_value = "3" },
    };
    const overrides: []const Entry = &.{
        .{ .key = "b", .json_value = "4" },
        .{ .key = "a", .json_value = "true" },
    };
    const arena = state.allocator();
    try std.testing.expectEqualDeep(@as([]const []const u8, &.{
        "--dynamic-config-value", "a=true",
        "--dynamic-config-value", "b=4",
        "--dynamic-config-value", "c=8",
        "--dynamic-config-value", "c=3",
    }), try serverArgs(arena, try merge(arena, defaults, overrides)));
    try std.testing.expectEqualDeep(defaults, try merge(arena, defaults, &.{}));
}
