# Eve + celld example

This private workspace applies the experimental full-payload celld composition
to a concrete high-volume rule:

- [`src/channels/github.ts`](src/channels/github.ts) declares a canonical
  `pull-request-changed` event and its complete Eve target;
- [`src/rules/blocked-pull-request.ts`](src/rules/blocked-pull-request.ts)
  correlates each repository/PR, absorbs webhook bursts, and wakes Eve only for
  current conflicts, requested changes, or failing checks; and
- [`src/publish.ts`](src/publish.ts) publishes verified webhook values against
  that declared channel.

PostgreSQL still owns definitions, runs, receipts, and audit state; a cell owns
complete mailbox payloads after its append receipt. Neither tier exposes event
lookup or replay.

See the core [celld guide](../../docs/celld.md) for deployment and the
[`@ewhauser/eve-ambient-eve` README](../../packages/eve-adapter/README.md) for
the required Eve patch.

```ts
const runtime = createEveCelldRuntime({
  applicationId: "developer-productivity-agent",
  eve: { auth: null, from },
  mailbox: {
    mode: "celld",
    fleetUrl: "https://ambient-cells.example.com",
    evaluatorUrl: "https://ambient-evaluator.example.com",
    secret: process.env.EVALUATOR_SECRET!,
  },
  pool,
});

await runtime.initialize();
await publishPullRequestChanged(runtime, {
  tenantId: "acme",
  installationId: "github-installation-42",
  id: "github-delivery-123",
  data: {
    failingChecks: ["test"],
    mergeState: "clean",
    number: 1842,
    repository: "vercel/eve",
    reviewDecision: "review-required",
    state: "open",
    title: "Carry channel delivery idempotency",
    updatedAt: "2026-08-14T18:00:00.000Z",
  },
  replyTarget: { address: "github:vercel/eve:pull:1842" },
  origin: { kind: "external" },
});
await runtime.drain();
```
