# Eve Ambient

Durable ambient attention for Eve agents.

This repository is a pnpm workspace containing the provider-independent
attention runtime, its supported Eve integration, deployment examples, and
integration conformance tests.

| Workspace | Purpose | Published |
|---|---|---|
| [`packages/ambient`](packages/ambient) | Typed monitors, idempotency lineage, PostgreSQL and celld mailboxes | `@ewhauser/eve-ambient` |
| [`packages/eve-adapter`](packages/eve-adapter) | Eve channel delivery with the carried `vercel/eve#1842` patch | `@ewhauser/eve-ambient-eve` |
| [`examples/eve-postgres`](examples/eve-postgres) | Eve delivery with the supported PostgreSQL-first runtime | No |
| [`examples/eve-celld`](examples/eve-celld) | Eve delivery with the experimental full-payload celld mailbox | No |
| [`integration/eve-conformance`](integration/eve-conformance) | Exact-version patch and adapter conformance | No |

## Install

Install the core runtime when your application supplies its own delivery
channel:

```sh
pnpm add @ewhauser/eve-ambient
```

Install `@ewhauser/eve-ambient-eve` when delivering directly to Eve. The Eve
adapter supports one exact Eve version and requires the application to apply
the patch shipped in the adapter package. See the
[`@ewhauser/eve-ambient-eve` README](packages/eve-adapter/README.md) for the
copy-and-verify procedure.

## Development

```sh
corepack enable pnpm
pnpm install
pnpm check
```

`pnpm check` builds and tests every workspace, verifies the installed Eve
patch, and validates both publishable npm artifacts.

See [`docs`](docs) for deployment guidance and
[`RFC 0001`](docs/rfcs/0001-full-payload-idempotent-handoffs.md) for the
full-payload, end-to-end idempotency design.
