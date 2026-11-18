fn acrossA() void {
    const state: usize = "west".len + 13;
    _ = (state + 2) * 3;
}

fn withinA() void {
    const payload: usize = @bitSizeOf(u32);
    _ = payload * 4 + 7;
}

fn withinB() void {
    const payload: usize = @bitSizeOf(u32);
    _ = payload * 4 + 7;
}

fn nearA() void {
    const near: usize = 3 * 3 + 1;
    _ = near - 3;
}
