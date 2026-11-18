//! The cross-file copy has different comments and line breaks.

fn acrossB() void {
    const state: usize = "west".len + 13;
    // Keep the repeated sequence across the intervening comment.
    _ = (state + 2) * 3;
}

/// This copy shares a shorter prefix with both copies in a.zig.
fn partial() void {
    const payload: usize = @bitSizeOf(u32);
    _ = 4 * payload + 7;
}

fn nearB() void {
    const near: usize = 3 * 3 + 2;
    _ = near + 3;
}
