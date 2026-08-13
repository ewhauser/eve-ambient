# Releasing

This repository uses Release Please and conventional commits. Merging a release
pull request creates a `vX.Y.Z` tag and a draft GitHub release.

The same workflow then:

1. installs from the frozen pnpm lockfile without a dependency cache;
2. runs type checking, the complete test suite (including PostgreSQL), the
   build, package-content checks, and a high-severity dependency audit;
3. packs the package in a job with no publish credentials;
4. waits for the protected `release` environment;
5. publishes the exact tarball to npm with short-lived OIDC credentials and npm
   provenance; and
6. attaches that tarball and its checksum to the draft GitHub release before
   publishing the immutable release.

All actions are pinned to full commit SHAs. Workflow permissions default to
none and are granted per job. The OIDC-enabled job does not check out or execute
repository code, and release jobs do not use dependency caches.

## One-time scoped-package bootstrap

npm requires a package name to exist before a trusted publisher can be
configured. `@ewhauser/eve-ambient@0.3.0` is therefore bootstrapped from the
same reviewed artifact used to initialize this standalone repository:

```sh
pnpm install --frozen-lockfile
EVE_AMBIENT_POSTGRES_URL='postgresql:///eve_ambient_test?host=/var/run/postgresql' pnpm check
pnpm audit --audit-level high
pnpm pack:release
npm publish release-artifacts/ewhauser-eve-ambient-0.3.0.tgz --access public --ignore-scripts
```

Create a draft `v0.3.0` GitHub release at the exact source commit, attach the
tarball and checksum, and publish it only after the npm publication succeeds.
Then configure this workflow as the package's trusted publisher:

```sh
npm trust github @ewhauser/eve-ambient \
  --repo ewhauser/eve-ambient \
  --file release.yml \
  --env release \
  --allow-publish
```

After verifying the trusted publisher, require two-factor authentication for
publishing and disallow bypass tokens. Log out of npm after the bootstrap.

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
