# Attention World integration

This fixture exercises Ambient against the first-class correlation World
contract. Its instrumented in-process World uses the same atomic reducer a
remote `world-celld` implementation can host.

The test makes the RPC rule executable:

- zero branches: zero append calls;
- one event and one correlation: one append call;
- a second event with that correlation: one more call to the same stream;
- one event spanning two correlations: two concurrent append calls; and
- duplicate events: another network call, rejected from buffering by the
  receiving stream's bounded recent-message ring.
