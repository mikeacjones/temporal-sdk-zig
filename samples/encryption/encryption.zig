//! Encrypted payloads: a workflow that greets someone through an activity, run by clients
//! whose payload codecs (`codec.zig`) compress and then encrypt every input and result with
//! AES-GCM. The starter picks the encryption key per workflow by propagating its key ID
//! (`CryptContext`); the codecs read it from the call's propagated values. The workflow and
//! activity don't know their data is encrypted.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const propagation = temporal.propagation;

pub const task_queue = "encryption";

/// Which key encrypts a workflow's payloads, set by the starter and propagated with the
/// workflow's calls.
pub const CryptContext = struct {
    key_id: []const u8,
};

/// The key the value is stored under, and the header field that carries it.
pub const propagate_key = propagation.key(CryptContext, "encryption");

/// Carries `propagate_key` in every call's header, so the workflow and its activities see
/// the key ID the workflow was started with and encrypt their results with the same key.
/// Set it in `client.Options.propagators` in every program; workers use the client's.
pub const propagator = propagation.HeaderPropagator(propagate_key).propagator;

/// The activity's input: fields by name, encoded as a JSON object.
pub const Info = std.json.ArrayHashMap([]const u8);

/// The workflow and activity don't need to know their inputs and results are encrypted: the
/// codecs run on the payloads after conversion.
fn encryptedWorkflow(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const logger = ctx.logger();
    logger.info("Encrypted Payloads workflow started", .{ .name = name });

    var info: Info = .{};
    try info.map.put(ctx.allocator(), "name", name);

    const greeting = ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{info});
    const result = greeting.get() catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("Encrypted Payloads workflow completed.", .{ .result = result });
    return result;
}

pub const Workflow = workflow.Def("Workflow", encryptedWorkflow);

fn greet(ctx: *activity.Context, info: Info) ![]const u8 {
    const a = ctx.allocator();
    ctx.logger().info("Activity", .{ .info = try std.json.Stringify.valueAlloc(a, info, .{}) });
    const name = info.map.get("name") orelse "someone";
    return std.fmt.allocPrint(a, "Hello {s}!", .{name});
}

pub const Activity = activity.Def("Activity", greet);
