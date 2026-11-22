# std.Io.Threaded: a future's result aligned above 8 is written past its allocation

Prepared for the Zig issue tracker; **not posted**. The owner posts it. Reproduction:
[repro.zig](repro.zig), run with `zig test docs/upstream/threaded-future-alignment/repro.zig`.

---

**Zig version:** 0.17.1 (aarch64-macos; the arithmetic is the same on every target where
`@alignOf(Future)` is 8)

## Summary

`std.Io.Threaded.Future.create` (lib/std/Io/Threaded.zig, lines 666-698) allocates the future
with `gpa.alignedAlloc(u8, .of(Future), alloc_len)`, so the allocation is aligned only to
`@alignOf(Future)`, 8. It sizes the allocation for the worst case of the context's
misalignment, but not of the result's:

```zig
const max_context_misalignment = context_alignment.toByteUnits() -| @alignOf(Future);
const worst_case_context_offset = context_alignment.forward(@sizeOf(Future) + max_context_misalignment);
const worst_case_result_offset = result_alignment.forward(worst_case_context_offset + context.len);
const alloc_len = worst_case_result_offset + result_len;
```

`worst_case_result_offset` aligns an *offset*, as if the allocation started at an address that
is a multiple of `result_alignment`. The actual result address is then computed from the real
address:

```zig
const actual_result_addr = result_alignment.forward(actual_context_addr + context.len);
```

When `result_alignment` is 16 and the allocator returns an address that is 8 mod 16 (which it
may: only 8 was asked for), `actual_result_addr` lands 8 bytes later than
`worst_case_result_offset`, and the task's result is written up to 8 bytes past `alloc_len`.
`Io.async` and `Io.concurrent` both go through `Future.create`.

## Arithmetic

On aarch64, `@sizeOf(Future)` is 64. For a task with no arguments (context length 0, context
alignment 1) returning `std.Io.Duration` (an `i96`: size 16, alignment 16):

- `worst_case_context_offset` = 64, `worst_case_result_offset` = forward16(64) = 64,
  `alloc_len` = 64 + 16 = 80.
- With the future at an address A where A mod 16 = 8: the context is at A + 64, and the result
  at forward16(A + 64) = A + 72. The result occupies bytes 72..88 of an 80-byte allocation.

A task whose argument tuple is 8 bytes longer has the opposite parity, so for any
`@sizeOf(Future)` one of two such tasks hits the case. Any result type with alignment above 8
is affected: `i96`/`u128` and structs holding them (`std.Io.Duration`, `std.Io.Timestamp`),
16-byte vectors, `align(16)` fields.

## Reproduction

`repro.zig` wraps `std.testing.allocator` in an allocator that returns every allocation aligned
to 8 or less at an address that is 8 mod 16 and writes 16 canary bytes after the requested
length, then checks them on free. It runs a `std.Io.Threaded` on that allocator and starts two
`io.concurrent` tasks returning `std.Io.Duration`, one with no arguments and one with one
`usize`.

Observed with Zig 0.17.1:

```text
1/1 repro.test.a concurrent task's 16-aligned result stays inside its future's allocation...write past the end of a 80-byte allocation at 0x100eb8018
expected 0, found 1
FAIL (TestExpectedEqual)
```

With a general-purpose allocator the overrun writes into the next allocation or the
allocator's metadata. We first saw it as intermittent "free of invalid memory or corrupted
metadata" panics from `std.heap.DebugAllocator`, in a program whose concurrent tasks returned a
struct containing an optional `std.Io.Duration`.

## Suggested fix

Either allocate at the largest alignment involved:

```zig
const alloc_alignment = Alignment.of(Future).max(context_alignment).max(result_alignment);
const future: *Future = @ptrCast(@alignCast(try gpa.rawAlloc(alloc_len, alloc_alignment, @returnAddress()) orelse return error.OutOfMemory));
```

(and free with the same alignment in `destroy`, for example by storing it beside
`context_alignment`), or keep the 8-byte allocation and budget the result's worst-case
misalignment the same way the context's is budgeted:

```zig
const max_result_misalignment = result_alignment.toByteUnits() -| @alignOf(Future);
const alloc_len = worst_case_result_offset + max_result_misalignment + result_len;
```

The first also lets `max_context_misalignment` go.

`Group.Task.create` (same file, around line 495) has no result and budgets the context's
misalignment, so `Group.async`, `Group.concurrent` and `Select` are not affected.

## Workaround

Have the task write its result through a pointer to memory the caller owns and return `void`.
