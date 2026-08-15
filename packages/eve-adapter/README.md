# @ewhauser/eve-ambient-eve

The official Eve ingress and delivery adapter for `@ewhauser/eve-ambient`.

```sh
pnpm add @ewhauser/eve-ambient @ewhauser/eve-ambient-eve eve@0.38.1
```

The adapter targets exactly Eve `0.38.1`. Consumers must apply the included
`patches/eve@0.38.1.patch` for `vercel/eve#1842`; the patch makes Eve's durable
session admission honor the supplied idempotency key.

## Attention route

```ts
import { createEveAttentionRoute } from "@ewhauser/eve-ambient-eve";

const eveRoute = createEveAttentionRoute({
  from: channelFrom,
  auth: wake => authForTenant(wake.tenantId),
});
```

By default, the route uses the rule's string `wake.target` as the Eve address.
Set `address` only for custom routing. Use the route in
`defineAmbientApplication({ rules, routes: [eveRoute] })`. It serializes
trusted instructions separately from untrusted evidence and maps the prepared
wake's `wakeKey` directly to Eve's `idempotencyKey`. A retry of the same
recorded wake therefore reaches the same durable Eve turn.

Customize `renderMessage` only when the replacement preserves the trust
boundary and complete lineage needed by the receiving agent.

Stateful Eve channels also need initial channel state when a wake creates a
new session. Supply `state` to the generic route, or use the channel-specific
route below; the state becomes part of the prepared by-value target.

## GitHub ambient ingress

Use Eve's built-in GitHub channel directly instead of redefining GitHub webhook
schemas:

```ts
import { defineAmbientRule } from "@ewhauser/eve-ambient";
import {
  createEveGitHubAmbientChannel,
  createEveGitHubAttentionRoute,
  eveGitHubPullRequestActivity,
} from "@ewhauser/eve-ambient-eve";

const rule = defineAmbientRule({
  id: "pull-request-shepherd",
  version: "v1",
  channel: eveGitHubPullRequestActivity,
  policy,
  decide,
});

const ambient = defineAmbientApplication({
  applicationId: "engineering-agent",
  rules: [rule],
  routes: [createEveGitHubAttentionRoute({ from: githubFrom, auth })],
}).with(celldBackend);

export default createEveGitHubAmbientChannel({
  publisher: ambient,
  tenantId: context => context.repository.owner,
  credentials,
});
```

The returned value is Eve's normal `githubChannel()`. Eve verifies and
normalizes the webhook; the adapter publishes its typed `pull_request` and
`check_suite` hook values into one Ambient channel. Check suites associated
with multiple pull requests produce one stable event per PR. Other Eve GitHub
configuration, including normal comment invocation, remains available on the
same options object.

The adapter also assigns one stable Ambient partition per pull request, so the
rule's default workflow already groups all PR and check-suite activity for
that PR. Set a rule `correlationKey` only to create multiple independent
workflows inside a pull request.

Eve normally schedules inbound hooks after its HTTP acknowledgement and logs
hook failures. This adapter deliberately waits for those hooks and tracks
Ambient admission failures separately, returning `503` unless the complete
event reached durable custody. A standard `X-GitHub-Delivery` header is
therefore required. The canonical event retains Eve's full raw PR or
check-suite object along with normalized source, actor, repository, and
conversation identity. Its prepared target also carries the complete Eve
GitHub channel state required to create or resume the PR session; the GitHub
attention route validates that target and supplies the state to Eve.

## Direct dispatch

```ts
import { createEveDirectDispatchAdapter } from "@ewhauser/eve-ambient-eve";

const direct = createEveDirectDispatchAdapter({
  from: channelFrom,
  address: request => `agent:${request.tenantId}`,
  auth: request => authForTenant(request.tenantId),
});
```

Direct chat delivery is not part of the attention workflow. Configure it as
the publisher's optional `direct.adapter`. The adapter maps the request's
stable direct-dispatch key to Eve's admission key and returns an idempotent
receipt containing the matching key and input hash.

This package stores no events or workflow state. Durable correlation and
prepared-wake retry belong to the selected attention engine; durable final
admission belongs to Eve.
