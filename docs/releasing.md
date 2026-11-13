# Releasing

A release is a GitHub release tagged `v<version>` with one asset that users fetch: `temporal-sdk-zig-<version>.tar.gz`, the SDK's package without the sdk-core submodule. A build of that package links a prebuilt core bundle, so users need neither Rust nor the submodule. The core bundles are released separately, under their own tags, because they change only with sdk-core.

Two workflows do the work, both started by hand (`workflow_dispatch`) and both dry runs unless told otherwise. Neither changes repository settings, and neither creates, moves or deletes a tag except by publishing a release.

## Version policy

- Versions follow [SemVer 2.0](https://semver.org/spec/v2.0.0.html). The tag is the version with a `v` in front (`v0.1.0-alpha.1`); `build.zig.zon`'s `.version` and the `CHANGELOG.md` heading are the version without it.
- Until the SDK reaches parity (PARITY.md), releases are 0.x pre-releases, starting at `0.1.0-alpha.1` and counting up through `alpha.N`, `beta.N` and `rc.N`. 1.0.0 follows parity.
- In 0.x, a minor version may break the API, and a patch version only fixes bugs. A change of the supported Zig version or of the sdk-core release is at least a minor version.
- A published tag is never moved or deleted. A bad release is followed by a new version.
- Core bundle releases are tagged `core-<CORE_VERSION>-r<revision>` (`core-v1.2.0-r1`). The revision counts rebuilds of the same sdk-core release, for example after a Rust toolchain change.

## Support policy

- Zig: each release supports exactly one Zig version, `build.zig.zon`'s `minimum_zig_version` (0.17.1).
- sdk-core: each release is built against one sdk-core release, the tag in `CORE_VERSION`. Its core bundles are pinned in `build.zig.zon` as `core-<CORE_VERSION>-<target>`.
- Tier 1 targets have a prebuilt core bundle, CI unit tests in Debug and ReleaseSafe, and, for the two marked, the release smoke test:

  | Target | Core bundle | CI unit tests | Release smoke test |
  | --- | --- | --- | --- |
  | `aarch64-macos` | yes | yes | yes |
  | `x86_64-macos` | yes | yes | |
  | `x86_64-linux-gnu` | yes | yes | yes |
  | `aarch64-linux-gnu` | yes | yes | |
  | `x86_64-linux-musl` | yes | yes | |
  | `aarch64-linux-musl` | yes | yes | |

- Other targets that sdk-core and the workflow fibers support build from a checkout with the submodule, where cargo builds the core. They aren't tested. Windows doesn't build yet: workflow fibers don't support Windows x64.
- The gnu bundles are built on Ubuntu 24.04 runners and need at least the glibc version that each core-bundles run prints in its summary. Cross-compiling to a gnu target needs a glibc at least that new in the target, as in `-Dtarget=x86_64-linux-gnu.2.39`. The musl builds are static and need no libc on the host.

## The workflows

### core-bundles.yml

Inputs: `revision` (a positive integer) and `dry_run` (default true).

It refuses a revision that isn't a positive integer, a revision whose tag `core-<CORE_VERSION>-r<revision>` already exists, and a publish run that isn't on main. Then it builds one bundle per tier 1 target with `scripts/package-core-bundle.sh` on a runner of that target, and runs `zig build test -Dcore-bundle=<bundle>` there with cargo removed from PATH. The run's summary lists each bundle's SHA-256, its zig hash, and the `build.zig.zon` entry that pins it, in dry runs too. Unless `dry_run` is set, it publishes the bundles and a `SHA256SUMS` file as a pre-release under the tag.

### release.yml

Inputs: `version` (SemVer without the `v`), `dry_run` and `prerelease` (both default true), and `publish_draft` (default false).

The `gate` job refuses the run, naming each failed check:

| Check | Refused unless |
| --- | --- |
| `semver` | `version` is a SemVer 2.0 version without a leading `v` |
| `tag` | there is no tag `v<version>` |
| `main-head` | the run's commit is main's HEAD |
| `zon` | `build.zig.zon`'s `.version` is `version` |
| `changelog` | `CHANGELOG.md` has a `## [<version>]` section with at least one entry |
| `fragments` | no changelog fragments remain under `changelog/` |
| `ci.yml`, `samples.yml` | the newest CI and Samples runs on the commit succeeded |
| `core-pins` | `build.zig.zon` pins a core bundle at `CORE_VERSION` for every tier 1 target |

`scripts/release-preflight.sh <version>` runs the checks that need only the checkout (`semver`, `zon`, `changelog`, `fragments` and `core-pins`), so run it before dispatching.

Then `package` packs the tarball with `scripts/package-release.sh` and prints its SHA-256 and zig hash. `smoke` runs `scripts/smoke-release.sh` on Linux x86_64 and macOS arm64: it adds the tarball to `tools/release-smoke`, a project that depends on the SDK as a user's does, builds it without cargo, so the SDK links its pinned core bundle, and runs it. Unless `dry_run` is set, `publish` creates a draft release `v<version>` at the gated commit, uploads the tarball and `SHA256SUMS`, and writes the notes: the version's `CHANGELOG.md` section, an install section with the `zig fetch --save` command, and a **Full Changelog** link comparing the previous release's tag with this one (the commit list, for the first release). `prerelease` marks it as a pre-release. Without `publish_draft` the run stops there, leaving the draft for the maintainer to read and publish from the release page; with it, the run publishes the draft. Publishing creates the tag.

## Changelog and release notes

Each user-visible change adds a fragment under `changelog/<category>/` in the same change, and a change with nothing users can observe says so with a `Changelog: none (<reason>)` trailer in its commit message. [changelog/README.md](../changelog/README.md) has the categories and the entry rules, and CI's `changelog` job checks both. `CHANGELOG.md` holds completed releases only.

A release's `CHANGELOG.md` section, which is also the body of its GitHub release notes, has this shape, with empty headings left out:

```markdown
## [0.2.0] - 2026-12-01

Requires Zig 0.17.1. Built against sdk-core v1.2.0.

### :boom: Breaking Changes
### Added
### Stabilized
### Changed
### Deprecated
### Fixed
### Security
```

`scripts/changelog-assemble.sh` writes it. The release notes add the install section and the **Full Changelog** link.

## Breaking changes

A change is breaking when code written against the previous release may stop compiling or behave differently:

- a public declaration is removed, renamed, or given another type or signature;
- an error set changes;
- a default changes;
- the behavior of replaying an existing workflow history changes;
- the required Zig version is raised ("Raised the required Zig version from 0.17.1 to 0.18.0.");
- a new sdk-core release changes observable behavior.

A breaking change adds a fragment under `changelog/breaking-changes/` that says what changed, what it did before, and what to do; when one line can't carry the migration, the entry links to the release's section in [upgrading.md](upgrading.md), `## Upgrading to <version>`, which has the before and after code. The commit's subject starts with 💥 (after `Merge bead/<id>: ` for a merge, so `git log --oneline` shows it), and its message ends with a `Breaking-Change: <what breaks>` trailer. CI requires the trailer and the fragment together. A Zig or sdk-core bump is at least a minor version (see the version policy).

## Deprecated and experimental declarations

- A public declaration that is renamed or replaced stays, as an alias for the new one, with a doc comment that begins `Deprecated: use <new name>.`, and the change adds a `deprecated` fragment naming the replacement. The alias is removed no sooner than the next minor release, by a breaking change whose entry names the release that deprecated it.
- A declaration whose doc comment begins `Experimental: may change or be removed without notice.` may change or go away without the deprecation step, but every such change is still listed in the release notes, its entry starting with `**Experimental**:`. Removing the label is a `stabilized` entry.

## Release preparation

1. Check that every change since the previous release has its fragment:

   ```sh
   scripts/changelog-draft.sh v0.1.0-alpha.1
   ```

   It lists each commit on main's first-parent history since that tag that added no fragment and has no `Changelog: none` trailer. Add the missing fragments, and reread the pending ones under `changelog/` as a user would.

2. Assemble the release's section from the fragments:

   ```sh
   scripts/changelog-assemble.sh 0.2.0 2026-12-01
   ```

   It writes `## [<version>] - <date>` above the previous release in `CHANGELOG.md`, with the Zig and sdk-core line, one heading per category in the order above, and the section's link at the bottom, and deletes the fragments it used. It refuses to run with no fragments or for a version `CHANGELOG.md` already has. Edit the section if entries need merging or rewording.

3. Set `build.zig.zon`'s `.version` to the version, and commit it with the assembled section and the deleted fragments as `Prepare release <version>`.

4. Run `scripts/release-preflight.sh <version>`. Every check but `core-pins` should pass now; `core-pins` passes once the pins below exist.

## Release sequence

1. If `CORE_VERSION` has no pinned bundles yet (the first release, or after a core bump), dispatch core-bundles.yml on main with `revision` 1 and the default dry run, and check the summary.
2. Dispatch it again with `dry_run` off to publish the bundles.
3. Commit the `build.zig.zon` entries from that run's summary, replacing any pins for an older `CORE_VERSION`, and land the commit on main.
4. Land the release preparation commit on main, and wait for its CI and Samples runs to succeed. Nothing else may land on main until the release is published, since the gate requires main's HEAD.
5. Dispatch release.yml on main with the version and the default dry run. It runs every check and both smoke tests.
6. Dispatch it again with `dry_run` off, and `prerelease` off for a release that isn't a pre-release. It stops at the draft release.
7. Read the draft's notes on the release page. Publish it there, which creates the tag, or delete the draft, fix the notes on main, and start again at step 4.
8. Try the `zig fetch --save` command from the published notes in a new project.

## Patch releases

A patch release fixes bugs in the latest release. While main holds nothing but fixes since that release, it is prepared and released from main like any other. Once main has changes that don't belong in a patch, the fixes go on a branch `releases/<major>.<minor>.x` cut from the release's tag: each fix is cherry-picked there with its fragment, the patch release is prepared and released from that branch, and the released section is then added to main's `CHANGELOG.md` with the cherry-picked fragments deleted from main. The workflows release only from main today, so a patch release from a branch needs their main-only checks relaxed first.

## Settings the owner sets

The workflows don't change repository settings. The owner sets these once:

- A tag ruleset on `v*` and `core-*` that blocks updates and deletions, but not creation, so publishing a release can create its tag.
- Actions: the default `GITHUB_TOKEN` permission is read-only. The publish jobs ask for `contents: write` and release.yml's gate asks for `actions: read` themselves.
- Branch protection on main requiring the CI and Samples checks, so the commit that release.yml gates has passed them on its way in.
