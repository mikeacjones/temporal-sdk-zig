//! Thread Sanitizer annotations for pointers the bridge hands through sdk-core.
//!
//! A pointer passed through the core to a callback on a core thread: the core is not
//! instrumented, so Thread Sanitizer sees no ordering between the writes before the call and
//! the callback's accesses, and reports a race that isn't there. `handToCore` before the call
//! and `takeFromCore` first in the callback give it that ordering. Both are empty without the
//! sanitizer.

const builtin = @import("builtin");

/// Call on the handing thread after the last write to `user_data`'s object, before the core
/// is given the pointer.
pub inline fn handToCore(user_data: *const anyopaque) void {
    if (builtin.sanitize_thread) __tsan_release(user_data);
}

/// Call first in the core's callback, before anything touches `user_data`'s object.
pub inline fn takeFromCore(user_data: *const anyopaque) void {
    if (builtin.sanitize_thread) __tsan_acquire(user_data);
}

extern fn __tsan_release(addr: *const anyopaque) void;
extern fn __tsan_acquire(addr: *const anyopaque) void;
