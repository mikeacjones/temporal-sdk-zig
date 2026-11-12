//! Batch sliding window: a batch workflow splits a data set into partitions and processes each
//! with a sliding window workflow. A sliding window workflow runs up to a fixed number of record
//! processor child workflows at once, starting a new child as soon as one finishes.
//!
//! A sliding window workflow continues as new after starting a page of children, to keep its
//! history bounded; the records still in flight travel to the next run in its arguments. The
//! children outlive that continue-as-new (their parent close policy is `abandon`) and report
//! their completion with the "ReportCompletion" signal to their parent's workflow ID, which
//! reaches whichever run is current. The "state" query shows a sliding window's progress.
//!
//! One sliding window workflow has a limited window and throughput; a larger batch runs several
//! of them in parallel, one per partition.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const Allocator = std.mem.Allocator;

pub const task_queue = "batch-sliding-window";

pub const SingleRecord = struct {
    id: u32,
};

pub const GetRecordsInput = struct {
    page_size: u32,
    offset: u32,
    max_offset: u32,
};

pub const GetRecordsOutput = struct {
    records: []const SingleRecord = &.{},
};

/// Loads the records to process. The sample makes up `record_count` records numbered from 0.
pub const RecordLoader = struct {
    record_count: u32,

    /// The total record count, used to divide the records into partitions.
    pub fn getRecordCount(l: *RecordLoader, ctx: *activity.Context) !u32 {
        _ = ctx;
        return l.record_count;
    }

    /// Up to `page_size` records from `offset`, stopping before `max_offset`.
    pub fn getRecords(l: *RecordLoader, ctx: *activity.Context, input: GetRecordsInput) !GetRecordsOutput {
        if (input.max_offset > l.record_count) {
            return ctx.fail(.{
                .message = try std.fmt.allocPrint(ctx.allocator(), "max_offset {d} is past the record count {d}", .{ input.max_offset, l.record_count }),
                .type = "InvalidOffset",
                .non_retryable = true,
            });
        }
        const limit = @min(input.offset +| input.page_size, input.max_offset);
        if (limit <= input.offset) return .{};
        const records = try ctx.allocator().alloc(SingleRecord, limit - input.offset);
        for (records, input.offset..) |*r, id| r.* = .{ .id = @intCast(id) };
        return .{ .records = records };
    }
};

pub const GetRecordCount = activity.Def("GetRecordCount", RecordLoader.getRecordCount);
pub const GetRecords = activity.Def("GetRecords", RecordLoader.getRecords);

const activity_options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(5) };

pub const ProcessBatchWorkflowInput = struct {
    /// How many children one run of a sliding window workflow starts.
    page_size: u32,
    /// How many children run at once, across all partitions.
    sliding_window_size: u32,
    /// How many sliding window workflows run in parallel.
    partitions: u32,
};

/// Divides the records into contiguous ranges, one per partition, and the window among the
/// partitions, then runs a sliding window workflow for each. Returns the number of records
/// processed.
fn processBatchWorkflow(ctx: *workflow.Context, input: ProcessBatchWorkflowInput) !u32 {
    const record_count = try ctx.executeActivity(GetRecordCount, activity_options, .{}).get();

    if (input.partitions == 0 or input.sliding_window_size < input.partitions) {
        return ctx.fail(.{
            .message = try std.fmt.allocPrint(ctx.allocator(), "sliding_window_size {d} cannot be less than the number of partitions {d}, which must be at least 1", .{ input.sliding_window_size, input.partitions }),
            .type = "invalidInput",
        });
    }
    const partitions = try divideIntoPartitions(ctx.allocator(), record_count, input.partitions);
    const window_sizes = try divideIntoPartitions(ctx.allocator(), input.sliding_window_size, input.partitions);

    ctx.logger().info("ProcessBatchWorkflow", .{
        .page_size = input.page_size,
        .sliding_window_size = input.sliding_window_size,
        .partitions = input.partitions,
        .record_count = record_count,
        .partition_sizes = partitions,
        .window_sizes = window_sizes,
    });

    const children = try ctx.allocator().alloc(workflow.ChildWorkflowFuture(u32), input.partitions);
    var offset: u32 = 0;
    for (children, partitions, window_sizes, 0..) |*child, partition_size, window_size, i| {
        // A readable child ID: the batch's ID and the partition's index.
        const child_id = try std.fmt.allocPrint(ctx.allocator(), "{s}/{d}", .{ ctx.info().workflow_id, i });
        child.* = ctx.executeChildWorkflow(SlidingWindowWorkflow, .{ .id = child_id }, .{.{
            .page_size = input.page_size,
            .sliding_window_size = window_size,
            .offset = offset,
            .maximum_offset = @min(offset + partition_size, record_count),
        }});
        offset += partition_size;
    }

    var result: u32 = 0;
    for (children) |child| result += try child.get();
    return result;
}

pub const ProcessBatchWorkflow = workflow.Def("ProcessBatchWorkflow", processBatchWorkflow);

/// Splits `number` into `n` parts that differ by at most one, larger parts first. Memory comes
/// from `gpa`; the caller owns it.
pub fn divideIntoPartitions(gpa: Allocator, number: u32, n: u32) error{OutOfMemory}![]u32 {
    const parts = try gpa.alloc(u32, n);
    for (parts, 0..) |*p, i| p.* = number / n + @intFromBool(i < number % n);
    return parts;
}

pub const SlidingWindowWorkflowInput = struct {
    page_size: u32,
    sliding_window_size: u32,
    /// The first record of this run's range (inclusive).
    offset: u32,
    /// The end of the whole range (exclusive).
    maximum_offset: u32,
    /// Records completed by earlier runs.
    progress: u32 = 0,
    /// Records whose children earlier runs started and that haven't reported completion.
    current_records: []const u32 = &.{},
};

/// The "state" query's result.
pub const SlidingWindowState = struct {
    /// Records being processed by child workflows, in ascending order.
    current_records: []const u32,
    children_started_by_this_run: u32,
    offset: u32,
    progress: u32,
};

pub const State = workflow.QueryDef("state", fn () SlidingWindowState);
pub const ReportCompletion = workflow.SignalDef("ReportCompletion", fn (record_id: u32) void);

/// A sliding window workflow's state, shared by the workflow function and its signal and query
/// handlers. Allocated with `ctx.allocator()` so it outlives the workflow function.
const SlidingWindow = struct {
    input: SlidingWindowWorkflowInput,
    /// Records being processed by child workflows, kept sorted.
    current_records: std.ArrayList(u32),
    /// Waited on to start before continuing as new.
    children_started_by_this_run: std.ArrayList(workflow.ChildWorkflowFuture(void)),
    offset: u32,
    progress: u32,

    fn state(s: *SlidingWindow) SlidingWindowState {
        return .{
            .current_records = s.current_records.items,
            .children_started_by_this_run = @intCast(s.children_started_by_this_run.items.len),
            .offset = s.offset,
            .progress = s.progress,
        };
    }

    fn recordCompletion(s: *SlidingWindow, record_id: u32) void {
        // A record counts once, even if its completion is reported again.
        const i = std.mem.indexOfScalar(u32, s.current_records.items, record_id) orelse return;
        _ = s.current_records.orderedRemove(i);
        s.progress += 1;
    }

    fn addRecord(s: *SlidingWindow, gpa: Allocator, record_id: u32) error{OutOfMemory}!void {
        const i = std.sort.lowerBound(u32, s.current_records.items, record_id, orderIds);
        if (i < s.current_records.items.len and s.current_records.items[i] == record_id) return;
        try s.current_records.insert(gpa, i, record_id);
    }

    fn orderIds(key: u32, item: u32) std.math.Order {
        return std.math.order(key, item);
    }

    fn windowHasRoom(s: *SlidingWindow) bool {
        return s.current_records.items.len < s.input.sliding_window_size;
    }

    fn allRecordsDone(s: *SlidingWindow) bool {
        return s.current_records.items.len == 0;
    }
};

/// Processes the records from `input.offset` to `input.maximum_offset` with at most
/// `input.sliding_window_size` record processor children at once, continuing as new after
/// each page. Returns the number of records processed over all runs.
fn slidingWindowWorkflow(ctx: *workflow.Context, input: SlidingWindowWorkflowInput) !u32 {
    ctx.logger().info("SlidingWindowWorkflow", .{
        .sliding_window_size = input.sliding_window_size,
        .page_size = input.page_size,
        .offset = input.offset,
        .maximum_offset = input.maximum_offset,
        .progress = input.progress,
    });

    const gpa = ctx.allocator();
    const s = try gpa.create(SlidingWindow);
    s.* = .{
        .input = input,
        .current_records = .empty,
        .children_started_by_this_run = .empty,
        .offset = input.offset,
        .progress = input.progress,
    };
    for (input.current_records) |id| try s.addRecord(gpa, id);
    try ctx.setQueryHandler(State, s, SlidingWindow.state);
    try ctx.setSignalHandler(ReportCompletion, s, SlidingWindow.recordCompletion);

    var page: GetRecordsOutput = .{};
    if (s.offset < input.maximum_offset) {
        page = try ctx.executeActivity(GetRecords, activity_options, .{.{
            .page_size = input.page_size,
            .offset = s.offset,
            .max_offset = input.maximum_offset,
        }}).get();
    }

    for (page.records) |record| {
        // Counts the children that earlier runs started, too.
        try ctx.await(s, SlidingWindow.windowHasRoom);
        const child = ctx.executeChildWorkflow(RecordProcessorWorkflow, .{
            // The children must survive this run continuing as new.
            .parent_close_policy = .abandon,
            .id = try std.fmt.allocPrint(gpa, "{s}/{d}", .{ ctx.info().workflow_id, record.id }),
        }, .{record});
        try s.children_started_by_this_run.append(gpa, child);
        try s.addRecord(gpa, record.id);
    }

    if (s.offset < input.maximum_offset) {
        // A child whose start command is still pending when the run ends would never start.
        for (s.children_started_by_this_run.items) |child| _ = try child.started();
        // Completions already received are recorded before the next run's arguments are built.
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return ctx.continueAsNew(SlidingWindowWorkflow, .{}, .{.{
            .page_size = input.page_size,
            .sliding_window_size = input.sliding_window_size,
            .offset = input.offset + @as(u32, @intCast(s.children_started_by_this_run.items.len)),
            .maximum_offset = input.maximum_offset,
            .progress = s.progress,
            .current_records = s.current_records.items,
        }});
    }

    // The last run in the chain waits for every child, including earlier runs', to report.
    try ctx.await(s, SlidingWindow.allRecordsDone);
    return s.progress;
}

pub const SlidingWindowWorkflow = workflow.Def("SlidingWindowWorkflow", slidingWindowWorkflow);

/// Processes one record, then reports its completion to the parent sliding window workflow.
fn recordProcessorWorkflow(ctx: *workflow.Context, record: SingleRecord) !void {
    const processed = processRecord(ctx, record);
    // A record processor always has a parent when the batch runs; one started on its own (as
    // in a test) skips the report.
    if (ctx.info().parent) |parent| {
        // No run ID: the parent continues as new, and its current run takes the signal. The
        // child waits for delivery, because a signal still pending when it completes is lost.
        try ctx.signalExternalWorkflow(ReportCompletion, parent.workflow_id, "", .{record.id}).get();
    }
    return processed;
}

pub const RecordProcessorWorkflow = workflow.Def("RecordProcessorWorkflow", recordProcessorWorkflow);

/// Stands in for real processing: waits a random 0 to 9 seconds.
fn processRecord(ctx: *workflow.Context, record: SingleRecord) !void {
    const seconds = try ctx.sideEffect(u32, {}, randomDelaySeconds);
    try ctx.io().sleep(.fromSeconds(seconds), .awake);
    ctx.logger().info("Processed", .{ .record_id = record.id, .delay_seconds = seconds });
}

/// Random on purpose: `ctx.sideEffect` records the value, so a replay reads it from history
/// instead of calling this. The workflow's own `io` draws seeded, replayable randomness, so
/// this takes the process-wide one.
fn randomDelaySeconds(_: void) u32 {
    var bytes: [4]u8 = undefined;
    std.Io.Threaded.global_single_threaded.io().random(&bytes);
    return std.mem.readInt(u32, &bytes, .little) % 10;
}
