# Eve + PostgreSQL example

This private workspace shows the supported default as an actual ambient rule,
not only storage wiring:

- [`src/channels/slack.ts`](src/channels/slack.ts) declares the canonical Slack
  `message` event and its complete Eve reply target;
- [`src/rules/incident-escalation.ts`](src/rules/incident-escalation.ts)
  correlates Slack threads, debounces bursts, applies a deterministic incident
  rule, projects evidence, and routes a wake to Eve; and
- [`src/publish.ts`](src/publish.ts) publishes authenticated provider input
  against that declared channel, including the direct-chat phase boundary.

An Eve route or receive hook passes its request-scoped `from` function to
`createEvePostgresRuntime`. PostgreSQL owns complete event, batch, and run
values until their terminal outcome or durable handoff.

Apply the Eve patch exactly as documented by
[`@ewhauser/eve-ambient-eve`](../../packages/eve-adapter/README.md), apply the
core SQL migration, call `initialize()`, and then publish through the channel
helper while the application's workers call `drain()`.

```ts
const runtime = createEvePostgresRuntime({
  applicationId: "engineering-agent",
  eve: { auth: null, from },
  pool,
});

await runtime.initialize();
await publishSlackMessage(runtime, {
  tenantId: "acme",
  installationId: "slack-workspace-T1",
  id: "slack-event-123",
  data: {
    channelId: "C123",
    messageTs: "1723651200.000100",
    severity: "critical",
    text: "SEV-1: checkout is unavailable",
  },
  replyTarget: { address: "slack:C123:1723651200.000100" },
  actor: { id: "U123", principalType: "user" },
  origin: { kind: "external" },
});
await runtime.drain();
```

The helper defaults to no direct-chat handler, which activates the
`undispatched` ambient source. If the application supplies direct handlers, it
also supplies their stable `bindingGeneration` as the helper's third argument;
changing handler membership without changing that generation is invalid.
