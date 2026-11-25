//! The Nexus service contract of the caller pattern: the service's name, its operations, and
//! their input and output types. Both sides import this file: the handler worker binds the
//! operations to its handlers, and the caller workflow calls them by these definitions. It
//! holds no handler code.
//!
//! Each operation names the user whose greeting workflow it reaches; the handler turns the
//! user ID into the workflow's ID.

const temporal = @import("temporal");
const nexus = temporal.nexus;

/// A language a greeting can be in.
pub const Language = enum {
    arabic,
    chinese,
    english,
    french,
    hindi,
    portuguese,
    spanish,

    /// The language's English name, such as "Arabic".
    pub fn name(l: Language) []const u8 {
        return switch (l) {
            .arabic => "Arabic",
            .chinese => "Chinese",
            .english => "English",
            .french => "French",
            .hindi => "Hindi",
            .portuguese => "Portuguese",
            .spanish => "Spanish",
        };
    }
};

pub const GetLanguagesInput = struct {
    /// Every language, not only those the workflow has a greeting for.
    include_unsupported: bool,
    user_id: []const u8,
};

pub const GetLanguagesOutput = struct {
    languages: []const Language,
};

pub const GetLanguageInput = struct {
    user_id: []const u8,
};

pub const SetLanguageInput = struct {
    language: Language,
    user_id: []const u8,
};

pub const ApproveInput = struct {
    /// Who approves.
    name: []const u8,
    user_id: []const u8,
};

pub const ApproveOutput = struct {};

/// Queries the workflow for the languages it can greet in.
pub const GetLanguages = nexus.OperationDef("getLanguages", GetLanguagesInput, GetLanguagesOutput);
/// Queries the workflow for its current language.
pub const GetLanguage = nexus.OperationDef("getLanguage", GetLanguageInput, Language);
/// Updates the workflow's language and returns the previous one.
pub const SetLanguage = nexus.OperationDef("setLanguage", SetLanguageInput, Language);
/// Signals the workflow that it is approved, so it completes.
pub const Approve = nexus.OperationDef("approve", ApproveInput, ApproveOutput);

pub const NexusGreetingService = nexus.ServiceDef("NexusGreetingService", .{ GetLanguages, GetLanguage, SetLanguage, Approve });
