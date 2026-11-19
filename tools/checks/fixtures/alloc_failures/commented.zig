//! A known-findings entry whose case is only quoted in a comment, as the top comment of
//! src/alloc_failure.zig quotes "converter.Header.fromEntries": here "fixture.commented",
//! which no test declares. The entry names no case, so `expectFindingsHaveCases` must fail.

const alloc_failures = @import("alloc_failures");

test "every known finding names a case" {
    try alloc_failures.expectFindingsHaveCases(@embedFile("commented.zig"));
}
