pub const generated = struct {
    pub const Hidden = struct { field: u32 };
};
pub const generated_alias = generated;

pub const nested = struct {
    pub const Options = struct {
        mode: Mode,
        @"odd field": bool,

        pub const default = @This(){ .mode = .ready, .@"odd field" = false };
        const hidden = 1;
    };
    pub const Errors = error{ BadInput, @"bad state" };
};

pub const Options = nested.Options;
pub const Errors = nested.Errors;
pub const Merged = Errors || error{Other};
pub const MergedAlias = Merged;
pub const Any = anyerror;
pub const Empty = error{};
const PrivateErrors = error{PrivateError};

pub const Mode = enum { ready, @"not ready" };
pub const Choice = union(enum) { mode: Mode, number: u32 };
pub const Handle = opaque {
    pub const Id = u64;
};
pub const Tuple = @Tuple(&.{u32});
pub const @"odd.name" = 42;

pub const Cycle = struct {
    next: ?*@This(),

    pub const Self = @This();
    pub const Child = struct {
        pub const Parent = Cycle;
    };
};

pub const instance = Options.default;
pub const error_value = error.SingleValue;
pub var counter: u32 = 0;
const private = struct {
    pub const Unreachable = 1;
};

pub fn fallible() !void {
    return error.OnlyInFunction;
}

pub fn typeFactory() type {
    @compileError("the lister must not call this function");
}
