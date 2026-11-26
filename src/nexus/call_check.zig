//! The compile-time check of a call to a Nexus operation through a service contract, shared by
//! the callers that take a `nexus.ServiceDef` and a `nexus.OperationDef`. Internal: not
//! re-exported from `nexus.zig`.

/// The compile error for calling `Operation` of `Service` from `caller` (such as
/// "Client.executeNexusOperation"), or null when `Service` is a `nexus.ServiceDef` that has
/// `Operation`, a `nexus.OperationDef`, with the same input and output types.
pub fn callError(comptime caller: []const u8, comptime Service: type, comptime Operation: type) ?[]const u8 {
    const is_service = @typeInfo(Service) == .@"struct" and @hasDecl(Service, "service_name") and @hasDecl(Service, "operation_defs");
    if (!is_service) return caller ++ ": Service must be a nexus.ServiceDef, not " ++ @typeName(Service);
    const what = caller ++ " on nexus service \"" ++ Service.service_name ++ "\"";
    const is_operation = @typeInfo(Operation) == .@"struct" and @hasDecl(Operation, "operation_name") and @hasDecl(Operation, "Input") and @hasDecl(Operation, "Output");
    if (!is_operation) return what ++ ": Operation must be a nexus.OperationDef, not " ++ @typeName(Operation);
    if (Service.hasOperation(Operation)) return null;
    const name = Operation.operation_name;
    if (Service.operationNamed(name)) |D| return what ++ ": its operation \"" ++ name ++ "\" takes " ++ @typeName(D.Input) ++ " and returns " ++ @typeName(D.Output) ++
        ", not " ++ @typeName(Operation.Input) ++ " and " ++ @typeName(Operation.Output) ++ "; call it with the service's own OperationDef";
    var names: []const u8 = "";
    for (Service.operation_defs, 0..) |D, i| names = names ++ (if (i > 0) ", " else "") ++ "\"" ++ D.operation_name ++ "\"";
    return what ++ ": the service has no operation \"" ++ name ++ "\"; its operations are " ++ names;
}
