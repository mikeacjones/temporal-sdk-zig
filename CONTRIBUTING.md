# Contributing to the Temporal Zig SDK

Thanks for your interest in contributing to the Temporal Zig SDK.

This is an unofficial, community-built SDK. Temporal Technologies doesn't maintain, support, or endorse it. Open issues and pull requests for it in this repository, not in Temporal's repositories: Temporal's maintainers can't act on bugs in this SDK.

This guide describes what helps maintainers evaluate an issue or a pull request. [AGENTS.md](AGENTS.md) has the detailed build, test, and review conventions.

## Before You Open an Issue

Search the existing issues first. If one describes the same bug, feature request, or design topic, add your details there instead of opening a duplicate, and upvote it to show that it affects you too.

Issues are assigned to people while they are actively working on them. Before taking on an issue, check whether it is already assigned so you don't duplicate someone else's work.

Use GitHub issues here for actionable bugs and feature work in this SDK. Questions about Temporal itself (how workflows, task queues, or the server behave) are better asked in [Temporal's community channels](https://temporal.io/community), which cover Temporal in general, not this SDK. If you aren't sure whether a problem is in this SDK or in Temporal, open an issue here.

## Bug Reports

When reporting a bug, include enough detail for someone else to reproduce or understand the problem:

- A short summary of the problem.
- A minimal reproduction, preferably as code that can be copied into a small project or test.
- What you expected to happen and what actually happened.
- The SDK version (the release tag, or the commit if you build from source).
- The Zig version, from `zig version`.
- The operating system and architecture.
- The sdk-core version, from [CORE_VERSION](CORE_VERSION).
- Temporal Server or Temporal Cloud details (dev server or self-hosted, and its version), if the issue depends on service behavior.
- Logs, stack traces, workflow histories, or other diagnostics that show the failure.
- Whether the behavior is a regression, and the last version or commit where it worked if known.

The [bug report template](.github/ISSUE_TEMPLATE/bug_report.md) asks for these.

## Feature Requests and Design Changes

Open or join an issue before starting substantial feature work, behavior changes, or API design changes. This gives maintainers and other users a chance to discuss the approach before you invest in a larger implementation.

Check what is already planned first. [PARITY.md](PARITY.md) lists the samples the SDK measures its coverage against and which of them pass, and README.md has a section marked "Not implemented yet" for each planned capability that hasn't landed. A request for one of those is best made on its existing issue, or as a new issue that refers to the README section.

Small bug fixes, documentation fixes, and narrowly scoped maintenance changes can go straight to a pull request.

## Development

README.md's [Development](README.md#development) section lists the prerequisites (Zig 0.17.1, rustup, and the sdk-core submodule) and how to build and test. Before opening a pull request, run the checks AGENTS.md asks for:

```bash
zig build fmt
zig build test
zig build test -Doptimize=ReleaseSafe
zig build samples-test
scripts/check-core-version.sh
```

`zig build test` also checks that README.md's code blocks equal their compiled examples in `examples/readme/`. `zig build samples-test` needs a Temporal dev server and is required when a change touches the client, worker, workflow runtime, activities, Nexus, payload conversion, or interceptors. `scripts/check-core-version.sh` matters when a change touches `deps/` or the sdk-core pin.

Generated files aren't edited by hand: AGENTS.md lists the command that regenerates each one.

## Pull Requests

Good pull requests are focused and easy to review:

- Keep each pull request scoped to one logical change.
- Include tests for behavior changes. A change to the workflow runtime, commands, patching, updates, local activities, or Nexus workflows also needs replay coverage with `testing.WorkflowReplayer`, so existing histories keep replaying.
- Update README.md in the same change when the public API changes or a capability lands. README.md is the API documentation: its examples live in `examples/readme/`, and `zig build readme` copies them into README.md. A new feature also needs its sample (see AGENTS.md).
- Describe user-facing changes in the commit and the pull request so they can be written up for users. Pull requests don't edit CHANGELOG.md: the maintainer writes each release's section from the merged work when the release is prepared (see [docs/releasing.md](docs/releasing.md)).
- Write the commit message and the pull request description in the style below.

Run the checks above before asking for review. CI must pass before a pull request can be merged.

There is no contributor license agreement to sign. Contributions are accepted under the repository's [LICENSE](LICENSE) (MIT): by opening a pull request, you agree to license your change under it.

### Commit messages

- The subject is one imperative sentence that names the behavior that changed, capitalized, with no trailing period, about 72 characters at most: "Fix stale worker slot metrics during polling", not "Fix bug" or "Updates".
- The body is plain prose wrapped at 72 columns, separated from the subject by a blank line. It says what was wrong or missing and who it affects, then what the code does now and why this approach, then what deliberately stays the same (compatibility, replay impact, limits). Name the real types, functions, options, and files.
- A commit that fixes review findings or a failed check describes the defect and the fix, not "address review".
- A trivial change (a typo, a version bump) can be a subject alone.

### Pull request descriptions

The title reads like a commit subject. The body has these sections, which the [pull request template](.github/PULL_REQUEST_TEMPLATE.md) fills in:

- **What was changed**: one bullet per user-visible or structural change, naming the option, type, function, or file and its new behavior. Tests and docs get their own bullets.
- **Why?**: the problem users or maintainers had and why this change solves it, with any alternatives you rejected when that matters.
- **Checklist**:
  1. Closes: the issue this resolves, or "N/A" and why.
  2. How was this tested: the exact commands you ran and their results, on which platforms and in which optimize modes, and plainly what you didn't run.
  3. Any docs updates needed: what you updated in README.md or elsewhere, or what still needs it.

When the branch changes after the pull request is opened, update the description so it still describes what the pull request contains.

## Things to Avoid

Avoid changes that make review harder without improving the contribution:

- Unrelated refactors mixed into a behavior change.
- Style-only churn. `zig build fmt` settles formatting.
- Large feature pull requests that weren't discussed in an issue first.
- Edits to generated files instead of regenerating them.
- License, copyright, or other legal changes without maintainer discussion.

## AI-Generated Contributions

Using AI tools while contributing is fine. You are responsible for the correctness, quality, and maintainability of everything you submit.

You must understand the problem you are fixing and be able to explain your change. Maintainers expect that understanding to show in your replies on the pull request and in design discussions. A contribution whose interaction looks entirely AI-driven may be closed: it adds nothing over maintainers running AI tools themselves.

Review AI-generated code and documentation yourself before opening a pull request. Check that it is correct, deterministic where workflow code requires it, tested, and consistent with the style of the files it changes.

Keep AI-assisted changes concise and scoped. Avoid verbose generated prose, comments that restate the code, and broad rewrites that make the change harder to review.

## Security Issues

Don't open a public issue for a suspected security vulnerability. Report it privately through this repository's **Security** tab, with **Report a vulnerability**.

## Review and CI

Maintainers review pull requests for correctness, determinism, compatibility with existing workflow histories, test coverage, documentation, and long-term maintainability. Review may ask for changes before a pull request can be merged, and it may take some time.

CI is the final gate. `.github/workflows/ci.yml` checks formatting, the core version pins, the generated samples index, and README.md's examples, and runs `zig build test` in Debug and ReleaseSafe; `.github/workflows/samples.yml` runs `zig build samples-test` on Linux and macOS. If CI fails, update the pull request, or ask for help if the failure looks unrelated to your change. Some CI runs may wait for a maintainer to approve them.

## Inactive Pull Requests

Maintainers may close inactive pull requests after following up if they are no longer moving forward. If that happens, you are welcome to reopen the pull request or open a new one when you are ready to continue.

## Community Conduct

Keep discussions respectful, constructive, and focused on the work. Clear context, specific examples, and patience with review feedback help everyone move faster.
