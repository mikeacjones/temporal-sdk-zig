const std = @import("std");
const harness = @import("harness");
const dynamic_config = @import("dynamic_config.zig");

const Allocator = std.mem.Allocator;
const Capability = harness.Context.Capability;

pub const Variant = struct {
    name: []const u8,
    dynamic_config: []const dynamic_config.Entry = &.{},
    expect_namespace_capabilities: []const Capability = &.{},
};

pub const Config = struct {
    needs_nexus_endpoint: bool = false,
    run_variants: []const Variant = &.{},
};

pub const Parsed = union(enum) { config: Config, unsupported: []const u8 };

pub fn parse(arena: Allocator, json: []const u8) error{OutOfMemory}!Parsed {
    const root = std.json.parseFromSliceLeaky(std.json.Value, arena, json, .{ .allocate = .alloc_always, .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return unsupported(arena, "invalid JSON: {t}", .{err}),
    };
    if (root != .object) return .{ .unsupported = "expected a JSON object" };

    var config: Config = .{};
    var fields = root.object.iterator();
    while (fields.next()) |field| {
        const key = field.key_ptr.*;
        const value = field.value_ptr.*;
        if (std.mem.eql(u8, key, "needsNexusEndpoint")) {
            if (value != .bool) return .{ .unsupported = "needsNexusEndpoint must be a boolean" };
            config.needs_nexus_endpoint = value.bool;
        } else if (std.mem.eql(u8, key, "runVariants")) {
            if (value != .array) return .{ .unsupported = "runVariants must be an array" };
            const variants = try arena.alloc(Variant, value.array.items.len);
            for (value.array.items, variants, 0..) |item, *variant, i| {
                if (item != .object) return unsupported(arena, "runVariants[{d}] must be an object", .{i});
                const name = item.object.get("name") orelse .null;
                if (name != .string or name.string.len == 0)
                    return unsupported(arena, "runVariants[{d}].name must be a non-empty string", .{i});
                if (std.mem.indexOfScalar(u8, name.string, 0) != null)
                    return unsupported(arena, "runVariants[{d}].name cannot contain a NUL byte", .{i});
                for (variants[0..i]) |previous| {
                    if (std.mem.eql(u8, previous.name, name.string))
                        return unsupported(arena, "duplicate run variant name \"{s}\"", .{name.string});
                }
                variant.* = .{ .name = name.string };
                if (try parseVariant(arena, item.object, variant)) |message| return .{ .unsupported = message };
            }
            config.run_variants = variants;
        } else {
            for ([_][]const u8{ "go", "java", "ts", "py", "cs", "php", "rb", "rs" }) |sdk| {
                if (std.mem.eql(u8, key, sdk)) break;
            } else return unsupported(arena, "unsupported key \"{s}\"", .{key});
        }
    }
    return .{ .config = config };
}

fn parseVariant(arena: Allocator, object: std.json.ObjectMap, variant: *Variant) error{OutOfMemory}!?[]const u8 {
    var fields = object.iterator();
    while (fields.next()) |field| {
        const key = field.key_ptr.*;
        const value = field.value_ptr.*;
        if (std.mem.eql(u8, key, "name")) continue;
        if (std.mem.eql(u8, key, "dynamicConfig")) {
            if (value != .object) return try std.fmt.allocPrint(arena, "runVariants \"{s}\" dynamicConfig must be an object", .{variant.name});
            const overrides = try arena.alloc(dynamic_config.Entry, value.object.count());
            for (overrides, value.object.keys(), value.object.values()) |*entry, setting, setting_value| {
                if (setting.len == 0 or std.mem.indexOfAny(u8, setting, "\x00=") != null)
                    return try std.fmt.allocPrint(arena, "runVariants \"{s}\" has invalid dynamicConfig key \"{f}\"", .{ variant.name, std.zig.fmtString(setting) });
                entry.* = .{ .key = setting, .json_value = try std.json.Stringify.valueAlloc(arena, setting_value, .{}) };
            }
            variant.dynamic_config = overrides;
        } else if (std.mem.eql(u8, key, "expectNamespaceCapabilities")) {
            if (value != .object) return try std.fmt.allocPrint(arena, "runVariants \"{s}\" expectNamespaceCapabilities must be an object", .{variant.name});
            const capabilities = try arena.alloc(Capability, value.object.count());
            for (capabilities, value.object.keys(), value.object.values()) |*capability, name, expected| {
                if (expected != .bool) return try std.fmt.allocPrint(arena, "runVariants \"{s}\" namespace capability \"{s}\" must be a boolean", .{ variant.name, name });
                capability.* = .{ .name = name, .value = expected.bool };
            }
            variant.expect_namespace_capabilities = capabilities;
        } else {
            return try std.fmt.allocPrint(arena, "runVariants \"{s}\" has unsupported key \"{s}\"", .{ variant.name, key });
        }
    }
    return null;
}

fn unsupported(arena: Allocator, comptime fmt: []const u8, args: anytype) error{OutOfMemory}!Parsed {
    return .{ .unsupported = try std.fmt.allocPrint(arena, fmt, args) };
}

test "feature config reads endpoint and ordered variants with JSON overrides" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const parsed = try parse(arena,
        \\{
        \\  "go": {"ignored": true},
        \\  "needsNexusEndpoint": true,
        \\  "runVariants": [
        \\    {"name": "z", "dynamicConfig": {"test.flag": true, "test.map": {"b": [1, "x"]}, "test.number": 1.00000000000000000001},
        \\     "expectNamespaceCapabilities": {"workerHeartbeats": false}},
        \\    {"name": "a"}
        \\  ]
        \\}
    );
    try std.testing.expectEqualDeep(@as(Parsed, .{ .config = .{
        .needs_nexus_endpoint = true,
        .run_variants = &.{
            .{ .name = "z", .dynamic_config = &.{
                .{ .key = "test.flag", .json_value = "true" },
                .{ .key = "test.map", .json_value = "{\"b\":[1,\"x\"]}" },
                .{ .key = "test.number", .json_value = "1.00000000000000000001" },
            }, .expect_namespace_capabilities = &.{.{ .name = "workerHeartbeats", .value = false }} },
            .{ .name = "a" },
        },
    } }), parsed);
    try std.testing.expectEqualDeep(@as(Parsed, .{ .config = .{} }), try parse(arena, "{}"));
}

test "feature config rejects unsupported keys and malformed variants" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    for ([_]struct { json: []const u8, message: []const u8 }{
        .{ .json = "[", .message = "invalid JSON" },
        .{ .json = "[]", .message = "expected a JSON object" },
        .{ .json = "{\"noWorkflow\": false}", .message = "unsupported key \"noWorkflow\"" },
        .{ .json = "{\"needsNexusEndpoint\": 1}", .message = "needsNexusEndpoint" },
        .{ .json = "{\"runVariants\": {}}", .message = "runVariants must be an array" },
        .{ .json = "{\"runVariants\": [false]}", .message = "runVariants[0]" },
        .{ .json = "{\"runVariants\": [{\"name\": \"\"}]}", .message = "non-empty string" },
        .{ .json = "{\"runVariants\": [{}]}", .message = "non-empty string" },
        .{ .json = "{\"runVariants\": [{\"name\": \"a\\u0000b\"}]}", .message = "cannot contain a NUL byte" },
        .{ .json = "{\"runVariants\": [{\"name\": \"a\"}, {\"name\": \"a\"}]}", .message = "duplicate run variant name \"a\"" },
        .{ .json = "{\"runVariants\": [{\"name\": \"a\", \"dynamicConfigs\": {}}]}", .message = "unsupported key \"dynamicConfigs\"" },
        .{ .json = "{\"runVariants\": [{\"name\": \"a\", \"dynamicConfig\": []}]}", .message = "dynamicConfig must be an object" },
        .{ .json = "{\"runVariants\": [{\"name\": \"a\", \"dynamicConfig\": {\"a=b\": true}}]}", .message = "invalid dynamicConfig key \"a=b\"" },
        .{ .json = "{\"runVariants\": [{\"name\": \"a\", \"expectNamespaceCapabilities\": false}]}", .message = "expectNamespaceCapabilities must be an object" },
        .{ .json = "{\"runVariants\": [{\"name\": \"a\", \"expectNamespaceCapabilities\": {\"workerHeartbeats\": \"false\"}}]}", .message = "namespace capability \"workerHeartbeats\" must be a boolean" },
    }) |case| {
        const result = try parse(state.allocator(), case.json);
        try std.testing.expect(result == .unsupported);
        try std.testing.expect(std.mem.indexOf(u8, result.unsupported, case.message) != null);
    }
}
