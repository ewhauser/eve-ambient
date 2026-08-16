# Ambient on a correlation World

This workspace contains two typechecked application shapes built entirely from
the checked-in `@ewhauser/eve-ambient` package:

- `application.ts` defines a small provider-independent support rule and
  exercises it with the in-memory reference engine;
- `slack-message-sequence.ts` defines a complete Slack message event, detects
  “message A” followed by “message B” per channel, and invokes an application-
  supplied durable turn sink.

Both can bind the local reference runtime with `memory()` or the production
runtime with `world()`.

The supplied client only needs `world.stream(key).append(input)`. Resolving the
stream handle is local; `append` is the single admission RPC. The supplied
World implementation owns the durable stream state and timers.

The same `AMBIENT_CALLBACK_SECRET` value must be available to the World runtime
and the application. The secret authenticates prepare and delivery callbacks.
