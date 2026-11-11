# Updating sdk-core

The SDK builds against the Temporal Rust core, vendored as a git submodule at `deps/sdk-core`. `CORE_VERSION` names the release tag the submodule must be on, and `scripts/check-core-version.sh` fails if the two disagree.

We pin release tags only, never a branch. The .NET SDK uses the same C bridge, so the tag it pins (`src/Temporalio/Bridge/sdk-core` in temporalio/sdk-dotnet) is a reasonable default for the next bump.

## Checklist

1. See what changed in the C header and the protos:

   ```sh
   scripts/core-header-diff.sh v1.3.0
   ```

   Read the header diff line by line. A changed struct field or callback signature breaks our bindings silently unless the ABI tests catch it, so note each one.

2. Move the submodule and update `CORE_VERSION`:

   ```sh
   git -C deps/sdk-core fetch --depth 1 origin tag v1.3.0
   git -C deps/sdk-core checkout v1.3.0
   echo v1.3.0 > CORE_VERSION
   scripts/check-core-version.sh
   ```

   If the check reports that `deps/Cargo.toml` drifted, copy sdk-core's `[workspace.*]` tables into it again. Our workspace exists to commit a `Cargo.lock`, and member crates inherit those tables from it. Copy a changed toolchain channel into `deps/rust-toolchain.toml` the same way. Then refresh the lockfile from `deps/`:

   ```sh
   cd deps && cargo update --workspace
   ```

   Review the `Cargo.lock` diff: it should only move the dependencies the new core release asks for.

3. Regenerate the Zig protobuf code:

   ```sh
   scripts/list-protos.sh
   zig build gen-proto
   ```

   `list-protos.sh` refreshes `deps/protos.zig`, the list of .proto files to compile. `gen-proto` rewrites `src/proto/` and the `src/proto.zig` index. Review the diff: renamed or removed fields break SDK code that uses them, and the compiler will point at each one.

4. Update `src/bridge/` for every header change from step 1. Then refresh the struct layout snapshot, which `zig build test` checks against the header:

   ```sh
   scripts/update-abi-snapshot.sh
   ```

   The `src/bridge_abi.txt` diff shows every struct whose size or field offsets moved. Each one needs a matching change in the code that builds or reads it.

5. Run `zig build test` (it rebuilds the core with cargo) and the integration tests against a dev server.

6. Commit the submodule move, `CORE_VERSION`, the regenerated code and the bridge changes together, with the old and new tags in the commit message.

Step 4 refers to code that doesn't exist yet. Update this file when the bridge task lands.
