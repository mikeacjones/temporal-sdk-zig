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

3. Regenerate the Zig protobuf code and review the diff of the generated files.

4. Update `src/bridge/` for every header change from step 1, and update the struct size and alignment tests.

5. Run `zig build test` and the integration tests against a dev server.

6. Commit the submodule move, `CORE_VERSION`, the regenerated code and the bridge changes together, with the old and new tags in the commit message.

Steps 3 and 4 refer to code that doesn't exist yet. Update this file when the proto generation and bridge tasks land.
