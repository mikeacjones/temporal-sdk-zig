# Changelog fragments

Each user-visible change adds its release-notes entry here, in the same change, as a fragment. When a release is prepared, `scripts/changelog-assemble.sh <version>` writes the fragments into [CHANGELOG.md](../CHANGELOG.md) as the release's section and deletes them, so this directory holds only what hasn't been released yet.

## Fragments

A fragment is `changelog/<category>/<short-kebab-name>.md`, holding one entry per line with no list marker and no blank lines. The categories, in the order the release notes list them:

| Directory | Heading | For |
| --- | --- | --- |
| `breaking-changes` | :boom: Breaking Changes | code that compiled or behaved one way now needs a change (see below) |
| `added` | Added | a new capability, API, or option |
| `stabilized` | Stabilized | an experimental API that lost its `Experimental:` label |
| `changed` | Changed | a behavior change that needs no change from users |
| `deprecated` | Deprecated | a declaration kept as an alias for a replacement |
| `fixed` | Fixed | a bug fix |
| `security` | Security | a fix for a vulnerability |

## Writing an entry

- One or two sentences about what a user can observe: the API, the behavior, the error, the default. Not how the code does it.
- Public names in backticks, as code uses them: `Client.executeWorkflow`, `worker.Options.graceful_shutdown_period`.
- No tracker or bead IDs, and no internal file names. Link README.md sections or docs/ files when they help, with full `https://github.com/...` URLs: the release notes show the entry outside the repository.
- An entry about an experimental API starts with `**Experimental**:`.
- A breaking entry says what changed, what it did before, and what to do. When a line can't carry the migration, it links to the release's section in [docs/upgrading.md](../docs/upgrading.md), which has the before and after code.

## Commits without a fragment

A change users can't observe (tests, CI, internal refactors, maintainer docs) adds no fragment. Its commit message ends with a trailer that says why:

```text
Changelog: none (CI configuration only)
```

## Breaking changes

[docs/releasing.md](../docs/releasing.md#breaking-changes) defines what breaks. A breaking change adds a fragment under `breaking-changes/`, starts its commit subject with 💥 (after `Merge bead/<id>: ` for a merge), and ends its message with a trailer naming what breaks:

```text
Breaking-Change: Worker.run returns fatal poll failures
```

`scripts/changelog-check.sh` checks the fragments, and on a pull request that its commits add a fragment or carry `Changelog: none`, and that a `Breaking-Change:` trailer and a `breaking-changes/` fragment come together. CI runs it.
