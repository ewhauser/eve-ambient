# Deployment options

Applications choose one attention backend. The application-facing publisher,
rules, callbacks, payload lineage, and final Eve route stay the same.

| Backend | Persistence | Work scheduling | Best fit |
|---|---|---|---|
| Memory | Process memory | Explicit `runDue()` | Tests and executable reference behavior |
| PostgreSQL | Private event/workflow rows | Workers poll `runOnce()` | Existing PostgreSQL deployments and simple operations |
| celld | Event-key and instance-key cells | Cell alarms | Distributed per-key serialization without PostgreSQL |

Backend state is not portable. Switching an active installation requires an
application-specific cutover because no public storage or state-migration
interface exists. There are no legacy production users requiring a migration
for this repository's architecture replacement.

## PostgreSQL

```text
provider -> publisher -> PostgreSQL accept -> due worker
                                            -> prepare -> checkpoint
                                            -> deliver -> receipt
```

Apply the private migration, construct `PostgresAttentionEngine`, and run one
or more pollers. PostgreSQL owns active payload custody, timers, leases, and
bounded receipts. It does not expose an event repository.

Choose it when PostgreSQL is already an acceptable durable dependency and the
workload can be served by its due index plus per-key advisory locks.

## celld

```text
provider -> publisher -> event cell -> correlation cell alarm
                                      -> prepare -> checkpoint -> deliver
```

Deploy the packaged worker and authenticated application callback endpoint.
celld owns all attention persistence; the application and example need no
PostgreSQL pool, schema, or worker.

Choose it when distributed per-correlation-key workflows and durable alarms
fit the operating environment better than a database poller.

## External ingress pipelines

Kafka, SQS, a webhook service, or a domain-specific rules engine may own source
delivery before Eve Ambient. Their retention and duplicate suppression are
implementation details. The integration must send the complete normalized
payload to `ambient.publish()` and retry ambiguous outcomes with the same
source identity.

The upstream system may acknowledge or commit its delivery only after
`publish()` returns successfully. That receipt means the complete frozen
fan-out has reached the selected attention backend. Eve Ambient does not add a
central event store or replay API around the upstream transport.

## Selection guidance

Start with the backend already operated and understood by the application.
Measure payload size, rule fan-out, correlation-key distribution, decision
latency, and retry rates before making throughput claims. An upstream log and
the choice between PostgreSQL and celld are independent decisions.
