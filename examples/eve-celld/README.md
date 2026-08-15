# Eve GitHub PR shepherd on celld

This workspace shows a real ambient workflow built on Eve's native
`githubChannel()`: a pull-request shepherd that coalesces PR and check-suite
webhooks, ignores a stale failure when a later success or closure arrives, and
wakes one idempotent Eve turn for the latest unresolved CI failure.

## Run the console demo

From the repository root:

```sh
pnpm --filter eve-ambient-example-celld demo
```

The credential-free demo starts two local HTTP servers: Eve's real GitHub
channel plus the application callbacks, and a local celld-compatible runtime
running the packaged `AttentionCell` worker. It then sends a burst of synthetic
`pull_request` and `check_suite` webhooks through Eve, advances celld's virtual
clock past the two-minute quiet period, and prints the one final bot turn.

The first synthetic PR includes duplicate delivery, failure, and recovery
events from multiple CI providers. Only the still-failing provider reaches the
bot. A second PR fails and then closes, so it produces no bot turn. The final
Eve destination is a console sink; the demo does not need GitHub, model, or
workflow-provider credentials and does not execute an agent or mutate a
repository.

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

The Eve adapter assigns one stable custody partition per pull request. Its
partition cell deduplicates individual deliveries, freezes fan-out, temporarily
holds complete PR/check payloads, drives rule alarms, checkpoints prepared
outcomes, and deletes terminal payloads. Ten webhook deliveries for the two
synthetic PRs therefore create two cells, not ten. There is no PostgreSQL pool,
event repository, payload lookup, history, or replay API.
