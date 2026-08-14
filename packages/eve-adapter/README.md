# `@ewhauser/eve-ambient-eve`

Official Eve delivery adapter for `@ewhauser/eve-ambient`.

The adapter sends the complete monitor delivery or direct-dispatch event to an
Eve channel address and passes the stable `wakeKey` or `directDispatchKey` as
Eve's channel-delivery idempotency key. It does not store event payloads and it
does not add replay.

## Compatibility

| Adapter | Core | Eve | Required patch |
|---|---|---|---|
| `0.x` | `>=0.4.0 <1` | exactly `0.38.1` | `patches/eve@0.38.1.patch` |

The Eve version is exact because the patch changes private compiled workflow
code. Do not use a semver range for Eve, and do not carry the patch forward to
another Eve release without rebuilding and running this repository's
conformance suite.

`eve@0.38.1-source.patch` is included beside the installable package patch for
human review and future upgrades. Consumers register only
`eve@0.38.1.patch`, because the npm package contains compiled output rather
than Eve's TypeScript sources.

## Install and apply the patch

A dependency cannot activate pnpm's patch configuration in its consuming
workspace. Every application must copy and register the patch itself:

```sh
pnpm add @ewhauser/eve-ambient @ewhauser/eve-ambient-eve eve@0.38.1
mkdir -p patches
cp node_modules/@ewhauser/eve-ambient-eve/patches/eve@0.38.1.patch patches/
```

Add this to the application's `pnpm-workspace.yaml`:

```yaml
patchedDependencies:
  "eve@0.38.1": patches/eve@0.38.1.patch
```

Then recreate the affected package and verify the public type:

```sh
pnpm install --force
rg "idempotencyKey" node_modules/eve/dist/src/channel/channel-operations.d.ts
```

CI should run that verification after every frozen install. A missing patch is
a correctness failure: the adapter must not silently fall back to an unkeyed
Eve send.

## Monitor delivery

Create the adapter inside an Eve channel route or receive hook, where Eve
provides `from`:

```ts
import { createEveDeliveryChannel } from "@ewhauser/eve-ambient-eve";

const delivery = createEveDeliveryChannel({
  from,
  auth: null,
});
```

Use `delivery` in the core runtime's `deliveryChannels` and route monitors to
the channel id `eve`. An `EveDeliveryTarget` is a complete JSON value:

```ts
const target = { address: "monitor:incident-42" };
```

The default renderer serializes trusted task instructions and untrusted
evidence into separately labelled fields. Applications can provide
`renderMessage` when their agent expects another complete by-value envelope.

## Direct chat dispatch

`createEveDirectDispatchHandler` sends the full canonical event and uses the
core-provided `directDispatchKey` as the Eve delivery key:

```ts
import { createEveDirectDispatchHandler } from "@ewhauser/eve-ambient-eve";

const direct = createEveDirectDispatchHandler({
  from,
  auth: null,
  address: ({ event }) => {
    const target = event.replyTarget;
    return typeof target === "object" &&
      target !== null &&
      !Array.isArray(target) &&
      typeof target.address === "string"
      ? target.address
      : undefined;
  },
});
```

## Guarantee boundary

The carried patch deduplicates delivery admission while the Eve channel
address is owned by its durable conversation session. The address/session
lifetime is therefore the supported admission horizon. A retry outside that
horizon may create a new session and must be outside the application's stated
idempotency window.

This package carries lineage through Eve admission; it does not make arbitrary
external tools idempotent. A final durable action must derive its own stable
`actionKey` from the admitted cause key and use a destination idempotency API,
reconciliation key, or transactional inbox/outbox. See
[`RFC 0001`](https://github.com/ewhauser/eve-ambient/blob/main/docs/rfcs/0001-full-payload-idempotent-handoffs.md).
