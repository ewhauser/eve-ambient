# Eve GitHub PR shepherd on celld

This workspace shows a real ambient workflow built on Eve's native
`githubChannel()`: a pull-request shepherd that coalesces PR and check-suite
webhooks, ignores a stale failure when a later success or closure arrives, and
wakes one idempotent Eve turn for the latest unresolved CI failure.

The consumer does not define a GitHub webhook schema. The Eve adapter owns:

- GitHub App verification and Eve's normalized `onPullRequest` and
  `onCheckSuite` hooks;
- delivery, installation, repository, actor, PR conversation, and full raw
  event canonicalization;
- waiting for Ambient's durable acceptance before returning Eve's `200`;
- mapping the final `wakeKey` to Eve's durable turn admission key.

The application only defines its policy in
`src/rules/pull-request-shepherd.ts` and its deployment bindings:

```ts
const application = createEveCelldApplication({
  applicationId: "engineering-agent",
  celld: { url: env.CELLD_URL, secret: env.CELLD_SECRET },
  eve: { from: githubFrom, auth },
});

export default createEngineeringGitHubChannel({
  publisher: application,
  tenantId: context => context.repository.owner,
  credentials,
});
```

Subscribe the GitHub App to `pull_request` and `check_suite` events. Mount
`application.fetch` for celld's `/ambient/prepare` and `/ambient/deliver`
callbacks; Eve mounts the returned GitHub channel at `/eve/v1/github`.

Event-coordinator cells freeze fan-out. Correlation cells temporarily hold the
complete PR/check payloads, drive alarms, checkpoint the prepared outcome, and
delete terminal payloads. There is no PostgreSQL pool, event repository,
payload lookup, history, or replay API.
