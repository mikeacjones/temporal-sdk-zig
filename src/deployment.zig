//! Worker Deployments: named sets of workers that roll out new workflow code a version at a
//! time. A worker joins one with `worker.Options.deployment`, naming the deployment and its
//! own build ID; together they are its Worker Deployment Version. The server routes a task
//! queue's workflows to the deployment's current version (and a share of them to a ramping
//! version), and each workflow's versioning behavior (`workflow.VersioningBehavior`) says
//! whether a run stays on the version it started on or moves to the current one.

/// A Worker Deployment Version: a deployment and one build of its workers' code. The slices
/// are borrowed from wherever the version came from.
pub const Version = struct {
    deployment_name: []const u8,
    build_id: []const u8,
};
