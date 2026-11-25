//! The context the sample propagates: a `Values` struct set by the starter travels to the
//! caller workflow, over the Nexus call to the handler, and on to the workflow the handler
//! starts. `propagators` carries it; every client of the sample sets it in
//! `client.Options.propagators`, and the clients' workers use it too.

const temporal = @import("temporal");
const propagation = temporal.propagation;

/// The value the sample propagates.
pub const Values = struct {
    key: []const u8,
    value: []const u8,
};

/// The header field that carries the value in client calls and workflow headers. Over a
/// Nexus call the SDK carries it inside its own Nexus header (README.md "Context
/// propagation").
pub const header_key = "custom-header";

/// The key the value is stored under: in the starter's `propagation.Values` and in what
/// workflows read with `ctx.propagated()`.
pub const propagate_key = propagation.key(Values, header_key);

/// Writes the value under `propagate_key` into the `custom-header` field of every outgoing
/// call, encoded with the default converter, and reads it back when a call is received. A
/// file-level constant, so it outlives every client that borrows it.
pub const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(propagate_key).propagator};
