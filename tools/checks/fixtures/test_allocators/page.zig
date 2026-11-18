const std = @import("std");
const outside = std.heap.smp_allocator;
const FixedBufferAllocator = std.heap.FixedBufferAllocator;

test "page-backed arena" {
    _ = .{ std.heap.page_allocator, std.heap.page_allocator };
    // std.heap.c_allocator
    _ = "std.heap.smp_allocator";
    _ = struct {
        test "nested \"page\" test" {
            _ = std.heap.page_allocator;
        }
    };
}

const Container = struct {
    fn named() void {}

    test named {
        _ = FixedBufferAllocator;
    }
};

test {
    _ = std.heap.smp_allocator;
}
