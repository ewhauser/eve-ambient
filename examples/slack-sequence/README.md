# Slack message sequence

This is the complete, typechecked example used by the repository README. It
normalizes verified Slack events, detects “message A” followed by “message B”
inside one channel, and sends one idempotent request to an application-owned
turn queue.

Use `memory()` for deterministic tests. Production applications use
`workflow()` and re-export `@ewhauser/eve-ambient/workflows` from their own
`workflows/` directory so the Workflow compiler discovers the packaged
correlation run and callback steps. The production factory accepts only
`WorkflowAmbientOptions`; Workflow selects and operates the configured standard
World.
