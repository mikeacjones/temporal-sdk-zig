//! A stand-in for an unreliable external service that the polling workflows wait on.

const std = @import("std");
const temporal = @import("temporal");

pub const TestService = struct {
    /// The service answers on every `error_attempts`-th call and is down otherwise. At
    /// least 1.
    error_attempts: u32,
    try_attempts: std.atomic.Value(u32) = .init(0),

    pub fn init(error_attempts: u32) TestService {
        return .{ .error_attempts = error_attempts };
    }

    /// "OK" when the service is up, `error.ServiceDown` otherwise. Safe to call from
    /// concurrent activities.
    pub fn getServiceResult(s: *TestService) error{ServiceDown}![]const u8 {
        const attempt = s.try_attempts.fetchAdd(1, .monotonic) + 1;
        if (attempt % s.error_attempts == 0) return "OK";
        return error.ServiceDown;
    }
};

/// How an activity reports the service being down. The failure is expected while polling,
/// so it is benign: workers log it at debug level and metrics don't count it as an error.
pub const service_down: temporal.failure.Application = .{
    .message = "service is down",
    .type = "ServiceError",
    .category = .benign,
};
