# Releasing

A release is a GitHub release tagged `v<version>` with one asset that users fetch: `temporal-sdk-zig-<version>.tar.gz`, the SDK's package without the sdk-core submodule. A build of that package links a prebuilt core bundle, so users need neither Rust nor the submodule. The core bundles are released separately, under their own tags, because they change only with sdk-core.

Two workflows do the work, both started by hand (`workflow_dispatch`) and both dry runs unless told otherwise. Neither changes repository settings, and neither creates, moves or deletes a tag except by publishing a release.

## Version policy

- Versions follow [SemVer 2.0](https://semver.org/spec/v2.0.0.html). The tag is the version with a `v` in front (`v0.1.0-alpha.1`); `build.zig.zon`'s `.version` and the `CHANGELOG.md` heading are the version without it.
- Until the SDK reaches parity (README.md's Parity table), releases are 0.x pre-releases, starting at `0.1.0-alpha.1` and counting up through `alpha.N`, `beta.N` and `rc.N`. 1.0.0 follows parity.
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

Inputs: `version` (SemVer without the `v`), `dry_run` and `prerelease` (both default true).

The `gate` job refuses the run, naming each failed check:

| Check | Refused unless |
| --- | --- |
| `semver` | `version` is a SemVer 2.0 version without a leading `v` |
| `tag` | there is no tag `v<version>` |
| `main-head` | the run's commit is main's HEAD |
| `zon` | `build.zig.zon`'s `.version` is `version` |
| `changelog` | `CHANGELOG.md` has a `## [<version>]` section |
| `ci.yml`, `samples.yml` | the newest CI and Samples runs on the commit succeeded |
| `core-pins` | `build.zig.zon` pins a core bundle at `CORE_VERSION` for every tier 1 target |

`scripts/release-preflight.sh <version>` runs the checks that need only the checkout (`semver`, `zon`, `changelog` and `core-pins`), so run it before dispatching.

Then `package` packs the tarball with `scripts/package-release.sh` and prints its SHA-256 and zig hash. `smoke` runs `scripts/smoke-release.sh` on Linux x86_64 and macOS arm64: it adds the tarball to `tools/release-smoke`, a project that depends on the SDK as a user's does, builds it without cargo, so the SDK links its pinned core bundle, and runs it. Unless `dry_run` is set, `publish` creates a draft release `v<version>` at the gated commit, uploads the tarball and `SHA256SUMS`, writes the notes (the version's `CHANGELOG.md` section and a `zig fetch --save` command), and publishes it, which creates the tag. `prerelease` marks it as a pre-release.

## Release preparation

1. List the work merged since the previous release:

   ```sh
   scripts/changelog-draft.sh v0.1.0-alpha.1
   ```

   It prints one line per `Merge bead/<id>: <summary>` commit on main's first-parent history. Sort the lines into Keep a Changelog's categories (Added, Changed, Deprecated, Removed, Fixed, Security) and reword them for users. Ordinary changes don't edit `CHANGELOG.md`; this step is the only one that does.

2. In one commit, put the section into `CHANGELOG.md` as `## [<version>] - <YYYY-MM-DD>` below an empty `## [Unreleased]`, add its link at the bottom, and set `build.zig.zon`'s `.version` to the version.

3. Run `scripts/release-preflight.sh <version>`. Every check but `core-pins` should pass now; `core-pins` passes once the pins below exist.

## Release sequence

1. If `CORE_VERSION` has no pinned bundles yet (the first release, or after a core bump), dispatch core-bundles.yml on main with `revision` 1 and the default dry run, and check the summary.
2. Dispatch it again with `dry_run` off to publish the bundles.
3. Commit the `build.zig.zon` entries from that run's summary, replacing any pins for an older `CORE_VERSION`, and land the commit on main.
4. Land the release preparation commit on main, and wait for its CI and Samples runs to succeed. Nothing else may land on main until the release is published, since the gate requires main's HEAD.
5. Dispatch release.yml on main with the version and the default dry run. It runs every check and both smoke tests.
6. Dispatch it again with `dry_run` off, and `prerelease` off for a release that isn't a pre-release.
7. Check the release page, then try the `zig fetch --save` command from its notes in a new project.

## Settings the owner sets

The workflows don't change repository settings. The owner sets these once:

- A tag ruleset on `v*` and `core-*` that blocks updates and deletions, but not creation, so publishing a release can create its tag.
- Actions: the default `GITHUB_TOKEN` permission is read-only. The publish jobs ask for `contents: write` and release.yml's gate asks for `actions: read` themselves.
- Branch protection on main requiring the CI and Samples checks, so the commit that release.yml gates has passed them on its way in.
