-- Private PostgreSQL storage for the durable AttentionEngine implementation.
-- These tables are not a public event, run, audit, history, or replay schema.

CREATE TABLE IF NOT EXISTS eve_ambient_event_coordinators (
  engine_id text NOT NULL,
  event_key text NOT NULL,
  state jsonb NOT NULL,
  expires_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (engine_id, event_key),
  CONSTRAINT eve_ambient_event_coordinator_state_object
    CHECK (jsonb_typeof(state) = 'object')
);

CREATE INDEX IF NOT EXISTS eve_ambient_event_coordinators_expiry_idx
  ON eve_ambient_event_coordinators (engine_id, expires_at)
  WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS eve_ambient_correlation_workflows (
  engine_id text NOT NULL,
  instance_key text NOT NULL,
  state jsonb NOT NULL,
  next_due_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (engine_id, instance_key),
  CONSTRAINT eve_ambient_correlation_workflow_state_object
    CHECK (jsonb_typeof(state) = 'object')
);

CREATE INDEX IF NOT EXISTS eve_ambient_correlation_workflows_due_idx
  ON eve_ambient_correlation_workflows (engine_id, next_due_at, instance_key)
  WHERE next_due_at IS NOT NULL;
