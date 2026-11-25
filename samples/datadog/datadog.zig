//! Tracing, metrics and JSON logs for an observability backend such as Datadog: a workflow
//! starts a child workflow with a fixed ID, which runs an activity. The client and the
//! worker trace through `temporal.tracing.Interceptor`, so one trace runs from the starter's
//! `StartWorkflow:Workflow` span through the child workflow to the worker's
//! `RunActivity:Activity`.
//!
//! There is no Zig Datadog tracer, so the programs export their spans over OTLP
//! (HTTP/protobuf) with the shared setup of the opentelemetry sample. With no OTLP variable
//! set they send to http://localhost:4318, where the Datadog Agent's OTLP HTTP receiver
//! listens once enabled (`otlp_config.receiver.protocols.http.endpoint: localhost:4318` in
//! datadog.yaml, or DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT=localhost:4318).
//! OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_SERVICE_NAME change the destination and the service
//! name (temporal-datadog-sample).
//!
//! The worker serves its metrics for Prometheus (which the Datadog Agent's OpenMetrics check
//! can scrape), each named with `metric_prefix`, and writes its log records as JSON lines.
//! Unlike Datadog's Go interceptor, the Zig tracing interceptor does not add trace and span
//! IDs to log records, so logs and traces are not correlated.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "datadog";
pub const child_workflow_id = "DATADOG-CHILD-WORKFLOW-ID";
/// Every metric the worker's client exports starts with this.
pub const metric_prefix = "temporal_datadog_";

fn parentWorkflow(ctx: *workflow.Context, name: []const u8) !void {
    ctx.logger().info("Executing Workflow.", .{ .name = name });
    try ctx.executeChildWorkflow(ChildWorkflow, .{ .id = child_workflow_id }, .{name}).get();
}

pub const Workflow = workflow.Def("Workflow", parentWorkflow);

fn childWorkflow(ctx: *workflow.Context, name: []const u8) !void {
    ctx.logger().info("Executing ChildWorkflow.", .{ .name = name });
    try ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}

pub const ChildWorkflow = workflow.Def("ChildWorkflow", childWorkflow);

fn runActivity(ctx: *activity.Context, name: []const u8) !void {
    ctx.logger().info("Executing Activity.", .{ .name = name });
    try ctx.io().sleep(.fromSeconds(1), .awake);
}

pub const Activity = activity.Def("Activity", runActivity);
