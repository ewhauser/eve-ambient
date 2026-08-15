# Prefiltered ingress

Eve Ambient does not need to own the raw event firehose. A rules engine, stream
processor, SIEM, queue consumer, or domain detector may select events first and
publish only signals that may deserve attention.

```text
provider -> external pipeline -> selection -> ambient.publish()
                                            -> attention backend
```

## Boundary

Before `publish()`, the external pipeline owns provider acknowledgement,
retention, offsets, ordering, normalization inputs, and any selection audit.
After a successful `publish()`, the selected attention backend owns the
complete frozen fan-out, correlation, buffering, preparation, delivery, and
payload cleanup.

The channel canonicalization contract still defines stable source identity and
the complete typed event:

```ts
const selectedSignals = defineChannelCanonicalization({
  version: 1,
  canonicalize(record) {
    return {
      id: record.sourceId,
      type: "selected-signal",
      version: 1,
      source: {
        tenantId: record.tenantId,
        channelId: "signal-pipeline",
        installationId: record.topic,
      },
      occurredAt: record.occurredAt,
      actor: record.actor,
      origin: { kind: "external", depth: 0 },
      data: { partition: record.entityKey, signal: record.signal },
    };
  },
  partitionKey: event => event.data.partition,
});

await ambient.publish(selectedSignals, record);
```

## Consumer rules

An upstream pull consumer should:

1. derive a stable source identity from the provider delivery or log record;
2. retry an ambiguous result with the same identity and complete payload;
3. commit its offset only after `publish()` succeeds;
4. preserve the payload until the attention backend accepts custody; and
5. describe its own ordering and delivery guarantees without relying on an
   Eve Ambient replay mechanism.

Intermediate transports may keep their own retention or duplicate ledger.
Those records do not become a central Eve Ambient system interface. Keys are
lineage and idempotency values, never references that the attention workflow
must dereference later.

The package does not ship Kafka or SQS adapters. A custom consumer calls the
same publisher used by provider-facing channels, and its selected backend may
be either PostgreSQL or celld.
