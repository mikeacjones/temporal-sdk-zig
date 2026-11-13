# Upgrading

This guide shows how to move code to a release that has breaking changes. Each such release gets a section here, `## Upgrading to <version>`, newest first, with before and after code for each change that a one-line entry in [CHANGELOG.md](../CHANGELOG.md) can't describe; the release's breaking entries link to it. A release without breaking changes has no section: upgrading to it needs no code change.

When upgrading across several releases, apply the sections from the oldest to the newest. Each release supports one Zig version and one sdk-core release; the first line of each release's section in CHANGELOG.md names them. [docs/releasing.md](releasing.md#breaking-changes) defines what counts as a breaking change, and how deprecated declarations are kept for at least one minor release before they are removed.

No release has breaking changes yet.
