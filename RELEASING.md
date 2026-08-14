# Releasing

This repository uses manifest-mode Release Please and conventional commits.
The two public packages release independently:

| Path | npm package | GitHub tag |
|---|---|---|
| `.` (artifact in `packages/ambient`) | `@ewhauser/eve-ambient` | `vX.Y.Z` |
| `packages/eve-adapter` | `@ewhauser/eve-ambient-eve` | `eve-ambient-eve-vX.Y.Z` |

The core retains its existing standalone `vX.Y.Z` tag series. The adapter uses
a component-prefixed tag so its independent versions cannot collide. The
initial workspace release bootstraps from the `v0.4.0` commit, preserving every
unreleased core change that preceded the directory migration.

The private workspace root mirrors the core version only so Release Please can
retain the pre-migration root history. Its `extra-files` updater changes the
real `packages/ambient/package.json` version in the same release commit, and
the packer maps release path `.` to that package. Adapter-only paths are
excluded from core version calculation.

Merging a release pull request creates one draft GitHub release per changed
package. The same workflow then:

1. installs the frozen pnpm workspace, including the exact Eve patch;
2. builds and tests all packages, examples, and conformance fixtures against
   PostgreSQL;
3. verifies the installed Eve patch and both package contents;
4. packs only the release paths in a job with no publish credentials;
5. waits for the protected `release` environment;
6. publishes those exact tarballs to npm with short-lived OIDC credentials and
   provenance; and
7. attaches each tarball and checksum to its matching draft GitHub release
   before publishing that immutable release.

All actions are pinned to full commit SHAs. Workflow permissions default to
none and are granted per job. The OIDC-enabled job does not check out or
execute repository code, and release jobs do not use dependency caches.

## One-time Eve adapter bootstrap

`@ewhauser/eve-ambient` is already published and has a trusted publisher. npm
requires the new adapter package name to exist before its trusted publisher can
be configured. For the adapter's first Release Please release, download the
reviewed `npm-release-packages` artifact from the blocked release workflow and
verify it locally:

```sh
sha256sum --check ewhauser-eve-ambient-eve-*.tgz.sha256
tar -xOf ewhauser-eve-ambient-eve-*.tgz package/package.json
npm publish ewhauser-eve-ambient-eve-*.tgz --access public --ignore-scripts
```

Then configure the normal workflow as the trusted publisher:

```sh
npm trust github @ewhauser/eve-ambient-eve \
  --repo ewhauser/eve-ambient \
  --file release.yml \
  --env release \
  --allow-publish
```

Rerun the failed workflow. Its immutable-integrity check accepts the existing
publication only if it is byte-identical, then publishes the draft GitHub
release. Require two-factor authentication for publishing, disallow bypass
tokens, and log out of npm after the bootstrap.

## Required GitHub settings

Before merging the first automated release pull request:

- restrict the `release` environment to `main` and require an independent
  maintainer's approval when a second maintainer is available;
- enable immutable releases and private vulnerability reporting;
- keep the default workflow token read-only while allowing Actions to create
  release pull requests;
- require Actions to be pinned to full-length commit SHAs; and
- protect `main` with pull requests, code-owner review, and the CI, dependency
  review, and GitHub Actions security checks.

Do not use `pull_request_target` or `workflow_run` to work around permission
failures. Do not add a long-lived npm token to repository or organization
secrets.
