//! Size limits of fuzz inputs and corpus files, shared by src/fuzz.zig and build.zig.
//! See src/fuzz.zig's top comment for how they relate to the Smith stream format.

/// The most slice bytes a target draws from one stream.
pub const max_input_len = 8192;

/// The most bytes of slice lengths (4 each), fixed-size values (8 each) and eos bytes
/// (1 each) a target draws besides its slice bytes.
pub const max_framing_len = 64;

/// The largest corpus file build.zig accepts, which is the largest stream a target can consume.
pub const max_file_len = max_input_len + max_framing_len;

/// The most bytes build.zig accepts over every corpus file in src/fuzz/*/ together.
pub const max_corpus_len = 65536;
