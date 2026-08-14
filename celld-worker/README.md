# @ewhauser/eve-ambient celld mailbox worker

The correlation mailbox as a [celld](https://github.com/denoland/celld) fleet
application: one cell per correlation instance, running the package's own
lifecycle statechart, with the cell's durable alarm replacing the store tier's
due-scan. **Experimental** — see the limitations box in the package README
before deploying it.

The cell holds no monitor configuration and no provider credentials. It learns
`monitorId`, `definitionVersion`, and the buffer/cooldown/retention configuration
from its first append and pins them; evaluation is a callback into your application,
where the decision pipeline, budgets, and run records already live.

## Deploy

```bash
# from your application, after `npm i @ewhauser/eve-ambient`
cp -r node_modules/@ewhauser/eve-ambient/celld-worker ./mailbox
cd mailbox

# check the bundle before the fleet does; CELLD_ESBUILD should be the same
# esbuild binary your fleet deploys with
CELLD_ESBUILD=/path/to/esbuild node build.mjs

# edit wrangler.jsonc: EVALUATOR_URL and EVALUATOR_SECRET
celld deploy --config wrangler.jsonc
```

`index.ts` is a one-line re-export of `@ewhauser/eve-ambient/celld-worker`, so the copy
resolves the implementation through your application's `node_modules` and needs
nothing else from the package. Keep it beside a `node_modules` that has
`@ewhauser/eve-ambient` installed and both `build.mjs` and `celld deploy` will bundle it.

celld deploys are stop-the-world fleet restarts today (`rollout.percent` is
not exposed), and cells resume from durable storage afterwards. Treat a worker
change like a schema change: the pinned configuration in existing cells is not
rewritten by a deploy.

## Routes

| Route | Purpose |
|---|---|
| `GET /health` | liveness |
| `POST /cells/<instanceKey>/append` | the runtime's `CelldAppendRequest` |
| `GET /cells/<instanceKey>/state` | stored full-value instance, pin, alarm, resident bytes, branch keys, transition log |
| `POST /cells/<instanceKey>/rearm` | recompute `nextEvaluationAt` and re-arm the alarm |
| `GET /cells/<instanceKey>/whoami` | owning Durable Object id, for placement tracing |

Everything under `/cells` requires `authorization: Bearer $EVALUATOR_SECRET`.
If `EVALUATOR_SECRET` is absent or empty, those routes fail closed with `503`
and no request is forwarded to a cell.
celld's *internal* listener (`/shutdown`, `/evict`) is unauthenticated and must
be firewalled separately — that is a fleet configuration concern, not this
worker's.

## `rearm`

celld stops re-dispatching an alarm after six counted handler failures. Durable
runtime backoff is not a handler failure: a `retry` response carries `retryAt`,
and the cell moves its alarm there without closing the active run. A cell
that exhausts the ladder keeps its buffered events and its instance record but
has no timer left to evaluate them. `rearm` recomputes the due time from the
stored record with the same derivation the statechart uses and sets the alarm
again; a cell with an active run is re-armed for *now*, because the abandoned
alarm was that run's retry ticket.

Alert on runs stuck in `retry` status in your store, and on cells whose
`nextEvaluationAt` is in the past with no pending alarm in `/state`. `rearm` is
the remediation for both.

## Environment

| Var | Meaning |
|---|---|
| `EVALUATOR_URL` | where claimed batches are evaluated. Overrides the URL the runtime sends with each append. |
| `EVALUATOR_SECRET` | shared bearer secret, equal to the runtime's `mailbox.secret`. |
| `MAILBOX_MAX_EVENT_BYTES` | maximum serialized `BufferedEvent` envelope accepted by a cell. |
| `MAILBOX_MAX_BATCH_BYTES` | maximum serialized provisional or claimed batch. Must be at least the event limit. |
| `MAILBOX_MAX_RESIDENT_BYTES` | maximum serialized open, sealed, and claimed batches plus append receipts in one cell. Must be at least the batch limit. |

All three byte limits are required positive integers. Missing or inconsistent
limits fail appends closed with `503`. An event or batch that can never fit is
rejected with `413`; resident-cell pressure returns `429`, so the runtime keeps
the branch payload and retries with backoff. There is no reference-only
fallback.
