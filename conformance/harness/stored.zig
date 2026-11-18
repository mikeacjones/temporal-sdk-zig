//! File layout and validation for the conformance history store.
const std = @import("std");
const temporal = @import("temporal");
const Allocator = std.mem.Allocator;

pub const manifest_name = "runs.json";

/// One file of conformance/histories, with its path relative to that directory.
pub const File = struct { path: []const u8, bytes: []const u8 };

pub const Run = struct { file: []const u8, workflow_id: []const u8 };
pub const Manifest = struct { namespace: []const u8, task_queue: []const u8, runs: []const Run };

pub const Name = struct { n: u32, workflow_type: []const u8 };
pub const Location = struct { feature: []const u8, version: []const u8, variant: ?[]const u8 };

pub const History = struct {
    path: []const u8,
    n: u32,
    workflow_type: []const u8,
    workflow_id: []const u8,
    history: temporal.history.History,
};

/// One leaf directory, with its histories in run number order.
pub const Directory = struct {
    path: []const u8,
    namespace: []const u8,
    task_queue: []const u8,
    histories: []const History,
};

pub fn validName(name: []const u8) bool {
    if (name.len == 0 or name[0] == '.') return false;
    for (name) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '.' and c != '_' and c != '-') return false;
    }
    return true;
}

pub fn fileName(arena: Allocator, n: u32, workflow_type: []const u8) error{ OutOfMemory, InvalidName }![]u8 {
    if (n == 0 or !validName(workflow_type)) return error.InvalidName;
    return std.fmt.allocPrint(arena, "{d}-{s}.json", .{ n, workflow_type });
}

pub fn parseFileName(name: []const u8) ?Name {
    const suffix = ".json";
    if (!std.mem.endsWith(u8, name, suffix)) return null;
    const stem = name[0 .. name.len - suffix.len];
    const dash = std.mem.indexOfScalar(u8, stem, '-') orelse return null;
    if (dash == 0 or stem[0] == '0') return null;
    for (stem[0..dash]) |c| if (!std.ascii.isDigit(c)) return null;
    const n = std.fmt.parseInt(u32, stem[0..dash], 10) catch return null;
    const workflow_type = stem[dash + 1 ..];
    if (!validName(workflow_type)) return null;
    return .{ .n = n, .workflow_type = workflow_type };
}

pub fn directoryPath(arena: Allocator, feature: []const u8, version: []const u8, variant: ?[]const u8) error{ OutOfMemory, InvalidName }![]u8 {
    if (!validName(version)) return error.InvalidName;
    if (variant) |name| {
        if (!validName(name)) return error.InvalidName;
        return std.fmt.allocPrint(arena, "{s}/{s}/{s}", .{ feature, version, name });
    }
    return std.fmt.allocPrint(arena, "{s}/{s}", .{ feature, version });
}

/// The longest matching feature takes precedence. The result borrows both arguments.
pub fn locate(features: []const []const u8, directory: []const u8) ?Location {
    var feature: []const u8 = "";
    for (features) |candidate| {
        if (candidate.len <= feature.len or !std.mem.startsWith(u8, directory, candidate)) continue;
        if (candidate.len == directory.len) return null;
        if (directory[candidate.len] == '/') feature = candidate;
    }
    if (feature.len == 0) return null;
    var parts = std.mem.splitScalar(u8, directory[feature.len + 1 ..], '/');
    const version = parts.first();
    const variant = parts.next();
    if (!validName(version) or (variant != null and !validName(variant.?)) or parts.next() != null) return null;
    return .{ .feature = feature, .version = version, .variant = variant };
}

pub fn writeManifest(manifest: Manifest, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try std.json.Stringify.value(manifest, .{ .whitespace = .indent_2 }, writer);
    try writer.writeByte('\n');
}

pub fn parseManifest(arena: Allocator, json: []const u8) error{ OutOfMemory, InvalidManifest }!Manifest {
    // Parse the JSON shape explicitly because a typed byte slice also accepts an array of integers.
    const value = std.json.parseFromSliceLeaky(std.json.Value, arena, json, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidManifest,
    };
    if (value != .object or value.object.count() != 3) return error.InvalidManifest;
    const namespace = value.object.get("namespace") orelse return error.InvalidManifest;
    const task_queue = value.object.get("task_queue") orelse return error.InvalidManifest;
    const list = value.object.get("runs") orelse return error.InvalidManifest;
    if (namespace != .string or task_queue != .string or list != .array) return error.InvalidManifest;
    if (namespace.string.len == 0 or task_queue.string.len == 0) return error.InvalidManifest;
    const runs = try arena.alloc(Run, list.array.items.len);
    for (list.array.items, runs) |item, *run| {
        if (item != .object or item.object.count() != 2) return error.InvalidManifest;
        const file = item.object.get("file") orelse return error.InvalidManifest;
        const workflow_id = item.object.get("workflow_id") orelse return error.InvalidManifest;
        if (file != .string or workflow_id != .string or workflow_id.string.len == 0) return error.InvalidManifest;
        run.* = .{ .file = file.string, .workflow_id = workflow_id.string };
    }
    return .{ .namespace = namespace.string, .task_queue = task_queue.string, .runs = runs };
}

pub fn startedWorkflowType(event: temporal.history.HistoryEvent) ?[]const u8 {
    if (event.event_type != .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) return null;
    const attrs = event.attributes orelse return null;
    return switch (attrs) {
        .workflow_execution_started_event_attributes => |started| if (started.workflow_type) |ty| ty.name else null,
        else => null,
    };
}

const Oom = error{OutOfMemory};
const Group = struct {
    manifest: ?File = null,
    histories: std.StringArrayHashMapUnmanaged(HistoryFile) = .{},
};

const HistoryFile = struct {
    file: File,
    name: Name,
    parsed: ?temporal.history.History,
    listed: bool = false,
};

/// Report store defects and return the usable directories and histories. Paths borrow `files`.
pub fn load(arena: Allocator, files: []const File, problems: *std.ArrayList([]const u8)) Oom![]const Directory {
    var groups: std.StringArrayHashMapUnmanaged(Group) = .{};
    for (files) |file| {
        const slash = std.mem.lastIndexOfScalar(u8, file.path, '/') orelse {
            try problem(arena, problems, file.path, "expected a file in a history directory", .{});
            continue;
        };
        const basename = file.path[slash + 1 ..];
        const is_manifest = std.mem.eql(u8, basename, manifest_name);
        const name = parseFileName(basename);
        if (!is_manifest and name == null) {
            try problem(arena, problems, file.path, "expected runs.json or <n>-<workflow type>.json", .{});
            continue;
        }

        const entry = try groups.getOrPut(arena, file.path[0..slash]);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        const group = entry.value_ptr;
        if (is_manifest) {
            if (group.manifest == null) {
                group.manifest = file;
            } else {
                try problem(arena, problems, file.path, "duplicate manifest", .{});
            }
        } else {
            const slot = try group.histories.getOrPut(arena, basename);
            if (slot.found_existing) {
                try problem(arena, problems, file.path, "duplicate history file", .{});
                continue;
            }
            slot.value_ptr.* = .{
                .file = file,
                .name = name.?,
                .parsed = try parseHistory(arena, file, name.?, problems),
            };
        }
    }

    var directories: std.ArrayList(Directory) = .empty;
    for (groups.keys(), groups.values()) |path, *group| {
        if (try loadDirectory(arena, path, group, problems)) |directory|
            try directories.append(arena, directory);
    }
    return directories.items;
}

fn parseHistory(arena: Allocator, file: File, name: Name, problems: *std.ArrayList([]const u8)) Oom!?temporal.history.History {
    const history = temporal.history.fromJson(arena, file.bytes) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidHistoryJson => {
            try problem(arena, problems, file.path, "invalid history JSON", .{});
            return null;
        },
    };
    const events = history.events.items;
    const workflow_type = if (events.len == 0) null else startedWorkflowType(events[0]);
    if (workflow_type == null or !std.mem.eql(u8, workflow_type.?, name.workflow_type)) {
        try problem(arena, problems, file.path, "first event must be WorkflowExecutionStarted naming workflow type \"{s}\"", .{name.workflow_type});
        return null;
    }
    return history;
}

fn loadDirectory(arena: Allocator, path: []const u8, group: *Group, problems: *std.ArrayList([]const u8)) Oom!?Directory {
    const manifest_file = group.manifest orelse {
        try problem(arena, problems, path, "history files have no runs.json", .{});
        return null;
    };
    if (group.histories.count() == 0)
        try problem(arena, problems, manifest_file.path, "manifest has no history files", .{});
    const manifest = parseManifest(arena, manifest_file.bytes) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidManifest => {
            try problem(arena, problems, manifest_file.path, "invalid manifest JSON", .{});
            return null;
        },
    };

    var in_order = true;
    var histories: std.ArrayList(History) = .empty;
    for (manifest.runs, 1..) |run, n| {
        const name = parseFileName(run.file);
        if (name == null or name.?.n != n) in_order = false;
        const entry = group.histories.getPtr(run.file) orelse {
            try problem(arena, problems, manifest_file.path, "entry \"{s}\" has no history file", .{run.file});
            continue;
        };
        if (entry.listed) continue;
        entry.listed = true;
        if (entry.parsed) |parsed| try histories.append(arena, .{
            .path = entry.file.path,
            .n = entry.name.n,
            .workflow_type = entry.name.workflow_type,
            .workflow_id = run.workflow_id,
            .history = parsed,
        });
    }
    if (!in_order)
        try problem(arena, problems, manifest_file.path, "runs must be numbered 1..N in order", .{});
    for (group.histories.values()) |entry| {
        if (!entry.listed) try problem(arena, problems, entry.file.path, "history file has no manifest entry", .{});
    }
    if (!in_order or histories.items.len == 0) return null;
    return .{ .path = path, .namespace = manifest.namespace, .task_queue = manifest.task_queue, .histories = histories.items };
}

fn problem(arena: Allocator, problems: *std.ArrayList([]const u8), path: []const u8, comptime fmt: []const u8, args: anytype) Oom!void {
    try problems.append(arena, try std.fmt.allocPrint(arena, "conformance/histories/{s}: " ++ fmt, .{path} ++ args));
}

test "conformance stored paths name numbered runs, versions and variants" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();

    const name = try fileName(arena, 12, "Foo_Bar-v2.1");
    try std.testing.expectEqualStrings("12-Foo_Bar-v2.1.json", name);
    try std.testing.expectEqualDeep(Name{ .n = 12, .workflow_type = "Foo_Bar-v2.1" }, parseFileName(name).?);
    try std.testing.expectEqual(std.math.maxInt(u32), parseFileName("4294967295-Workflow.json").?.n);
    for ([_][]const u8{ "", "runs.json", "0-W.json", "01-W.json", "-W.json", "+1-W.json", "1_0-W.json", "4294967296-W.json", "1-.json", "1-.W.json", "1-A/B.json", "1-W.JSON" }) |invalid| {
        try std.testing.expectEqual(null, parseFileName(invalid));
    }
    try std.testing.expectError(error.InvalidName, fileName(arena, 0, "Workflow"));
    try std.testing.expectError(error.InvalidName, fileName(arena, 1, "Workflow Type"));
    try std.testing.expectEqualStrings("area/name/0.0.0", try directoryPath(arena, "area/name", "0.0.0", null));
    try std.testing.expectEqualStrings("area/name/0.0.0/variant", try directoryPath(arena, "area/name", "0.0.0", "variant"));
    try std.testing.expectError(error.InvalidName, directoryPath(arena, "area/name", "", null));
    try std.testing.expectError(error.InvalidName, directoryPath(arena, "area/name", "0.0.0", "../old"));
}

test "conformance stored paths identify the longest owning feature" {
    const features: []const []const u8 = &.{ "area/name", "area/name/nested" };
    try std.testing.expectEqualDeep(Location{ .feature = "area/name", .version = "0.0.0", .variant = null }, locate(features, "area/name/0.0.0").?);
    try std.testing.expectEqualDeep(Location{ .feature = "area/name/nested", .version = "0.0.0", .variant = "variant" }, locate(features, "area/name/nested/0.0.0/variant").?);
    for ([_][]const u8{ "area/name", "area/names/0.0.0", "area/name/nested", "area/name//v", "area/name/0.0.0/", "area/name/0.0.0/a/b", "area/name/0.0.0/.." }) |invalid| {
        try std.testing.expectEqual(null, locate(features, invalid));
    }
}

test "conformance stored manifests round trip and require the JSON shape" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const manifest: Manifest = .{ .namespace = "default", .task_queue = "queue", .runs = &.{.{ .file = "1-W.json", .workflow_id = "workflow" }} };
    var output: std.Io.Writer.Allocating = .init(arena);
    try writeManifest(manifest, &output.writer);
    try std.testing.expectEqualStrings(
        \\{
        \\  "namespace": "default",
        \\  "task_queue": "queue",
        \\  "runs": [
        \\    {
        \\      "file": "1-W.json",
        \\      "workflow_id": "workflow"
        \\    }
        \\  ]
        \\}
        \\
    , output.written());
    try std.testing.expectEqualDeep(manifest, try parseManifest(arena, output.written()));
    for ([_][]const u8{
        "[",
        "{}",
        \\{"namespace":[65],"task_queue":"q","runs":[]}
        ,
        \\{"namespace":"n","task_queue":"q","runs":{}}
        ,
        \\{"namespace":"n","task_queue":"q","runs":[{"file":"1-W.json","workflow_id":null}]}
        ,
    }) |bad| try std.testing.expectError(error.InvalidManifest, parseManifest(arena, bad));
}

const test_history =
    \\{"events":[{"eventType":"WorkflowExecutionStarted","workflowExecutionStartedEventAttributes":{"workflowType":{"name":"W"}}}]}
;
const test_manifest =
    \\{"namespace":"default","task_queue":"queue","runs":[{"file":"1-W.json","workflow_id":"one"}]}
;

test "conformance stored histories report each file and manifest defect" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const path = "area/name/0.0.0/";
    const history: File = .{ .path = path ++ "1-W.json", .bytes = test_history };
    const manifest: File = .{ .path = path ++ "runs.json", .bytes = test_manifest };
    const cases: []const struct { files: []const File, problems: []const []const u8 } = &.{
        .{ .files = &.{.{ .path = path ++ "note.txt", .bytes = "" }}, .problems = &.{path ++ "note.txt: expected runs.json"} },
        .{ .files = &.{history}, .problems = &.{"area/name/0.0.0: history files have no runs.json"} },
        .{ .files = &.{manifest}, .problems = &.{ path ++ "runs.json: manifest has no history files", path ++ "runs.json: entry \"1-W.json\" has no history file" } },
        .{
            .files = &.{ history, .{ .path = manifest.path, .bytes = "{}" } },
            .problems = &.{path ++ "runs.json: invalid manifest JSON"},
        },
        .{
            .files = &.{ history, manifest, .{ .path = path ++ "2-W.json", .bytes = test_history } },
            .problems = &.{path ++ "2-W.json: history file has no manifest entry"},
        },
        .{
            .files = &.{
                history,
                .{ .path = path ++ "3-W.json", .bytes = test_history },
                .{ .path = manifest.path, .bytes =
                \\{"namespace":"default","task_queue":"queue","runs":[{"file":"1-W.json","workflow_id":"one"},{"file":"3-W.json","workflow_id":"three"}]}
                },
            },
            .problems = &.{path ++ "runs.json: runs must be numbered 1..N in order"},
        },
        .{
            .files = &.{
                history,
                .{ .path = path ++ "2-W.json", .bytes = test_history },
                .{ .path = manifest.path, .bytes =
                \\{"namespace":"default","task_queue":"queue","runs":[{"file":"2-W.json","workflow_id":"two"},{"file":"1-W.json","workflow_id":"one"}]}
                },
            },
            .problems = &.{path ++ "runs.json: runs must be numbered 1..N in order"},
        },
        .{
            .files = &.{ manifest, .{ .path = history.path, .bytes = "{" } },
            .problems = &.{path ++ "1-W.json: invalid history JSON"},
        },
        .{
            .files = &.{ manifest, .{ .path = history.path, .bytes = "{\"events\":[]}" } },
            .problems = &.{path ++ "1-W.json: first event must be WorkflowExecutionStarted naming workflow type \"W\""},
        },
        .{
            .files = &.{
                manifest,
                .{ .path = history.path, .bytes =
                \\{"events":[{"eventType":"WorkflowExecutionStarted","workflowExecutionStartedEventAttributes":{"workflowType":{"name":"Other"}}}]}
                },
            },
            .problems = &.{path ++ "1-W.json: first event must be WorkflowExecutionStarted naming workflow type \"W\""},
        },
    };
    for (cases) |case| {
        var problems: std.ArrayList([]const u8) = .empty;
        _ = try load(arena, case.files, &problems);
        try std.testing.expectEqual(case.problems.len, problems.items.len);
        for (case.problems, problems.items) |want, actual| {
            try std.testing.expect(std.mem.startsWith(u8, actual, "conformance/histories/"));
            try std.testing.expect(std.mem.startsWith(u8, actual["conformance/histories/".len..], want));
        }
    }
}

test "conformance stored histories load in manifest order and retain identity" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    const path = "area/name/0.0.0/variant";
    const files: []const File = &.{
        .{ .path = path ++ "/2-W.json", .bytes = test_history },
        .{ .path = path ++ "/runs.json", .bytes =
        \\{"namespace":"default","task_queue":"queue","runs":[{"file":"1-W.json","workflow_id":"one"},{"file":"2-W.json","workflow_id":"two"}]}
        },
        .{ .path = path ++ "/1-W.json", .bytes = test_history },
        .{ .path = "area/other/0.0.0/1-W.json", .bytes = test_history },
    };
    var problems: std.ArrayList([]const u8) = .empty;
    const directories = try load(state.allocator(), files, &problems);
    try std.testing.expectEqual(1, problems.items.len);
    try std.testing.expectEqual(1, directories.len);
    const directory = directories[0];
    try std.testing.expectEqualStrings(path, directory.path);
    try std.testing.expectEqualStrings("default", directory.namespace);
    try std.testing.expectEqualStrings("queue", directory.task_queue);
    try std.testing.expectEqual(2, directory.histories.len);
    for (directory.histories, [_][]const u8{ "one", "two" }, 1..) |history, id, n| {
        try std.testing.expectEqual(n, history.n);
        try std.testing.expectEqualStrings(id, history.workflow_id);
        try std.testing.expectEqualStrings("W", history.workflow_type);
    }
}
