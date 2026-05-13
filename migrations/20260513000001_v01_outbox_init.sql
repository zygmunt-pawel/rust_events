DO $$ BEGIN
  IF current_setting('server_version_num')::int < 180000 THEN
    RAISE EXCEPTION 'rust_events requires PostgreSQL 18+ (uuidv7() native), got %',
      current_setting('server_version');
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS outbox;

CREATE FUNCTION outbox.set_updated_at() RETURNS TRIGGER LANGUAGE plpgsql AS $$
  BEGIN NEW.updated_at := now(); RETURN NEW; END $$;

CREATE FUNCTION outbox.deny_update() RETURNS TRIGGER LANGUAGE plpgsql AS $$
  BEGIN RAISE EXCEPTION 'updates are not allowed on table "%"', TG_TABLE_NAME; END $$;

-- ============================================================================
-- (1) outbox.events — Type B1, immutable.
-- ============================================================================
CREATE TABLE outbox.events (
    id           UUID        PRIMARY KEY,
    event_type   TEXT        COLLATE "C" NOT NULL,
    producer_bc  TEXT        COLLATE "C" NOT NULL DEFAULT '',
    tenant_id    TEXT        COLLATE "C" NOT NULL DEFAULT '',
    payload      BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Byte limits everywhere (not character length) — storage-bound, predictable
    -- across multi-byte UTF-8 inputs. Rust validates input.len() (bytes) too.
    CONSTRAINT events_event_type_bytes   CHECK (octet_length(event_type) BETWEEN 1 AND 128),
    CONSTRAINT events_producer_bc_bytes  CHECK (octet_length(producer_bc) <= 64),
    CONSTRAINT events_tenant_id_bytes    CHECK (octet_length(tenant_id) <= 64),
    CONSTRAINT events_payload_size       CHECK (octet_length(payload) <= 1048576),
    CONSTRAINT events_headers_object     CHECK (jsonb_typeof(headers) = 'object')
);

-- No listing index in initial migration. Operators add their own for their query
-- patterns (most common: tenant + event_type + recency). Keeping initial migration
-- write-cheap; documented in README.

CREATE TRIGGER deny_update_events
    BEFORE UPDATE ON outbox.events
    FOR EACH ROW EXECUTE FUNCTION outbox.deny_update();

-- ============================================================================
-- (2) outbox.dispatch_keys — composite PK, DEFERRABLE FK.
-- ============================================================================
CREATE TABLE outbox.dispatch_keys (
    tenant_id        TEXT        COLLATE "C" NOT NULL,
    idempotency_key  TEXT        COLLATE "C" NOT NULL,
    event_id         UUID        NOT NULL
                     REFERENCES outbox.events(id) ON DELETE CASCADE
                     DEFERRABLE INITIALLY DEFERRED,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, idempotency_key),

    CONSTRAINT dispatch_keys_tenant_bytes  CHECK (octet_length(tenant_id) <= 64),
    CONSTRAINT dispatch_keys_key_bytes     CHECK (octet_length(idempotency_key) BETWEEN 1 AND 128)
);

CREATE INDEX dispatch_keys_event_idx ON outbox.dispatch_keys (event_id);

-- Purge sweeps by created_at; without this index, full table scan per chunk.
CREATE INDEX dispatch_keys_created_at_idx ON outbox.dispatch_keys (created_at);

-- ============================================================================
-- (3) Delivery status enum + handler_deliveries.
--   lease_token mirrors pg_work_queue's fencing discipline:
--     after the first claim, all UPDATEs MUST match the stamped lease_token,
--     or they are silently rejected (mark_* with rows_affected=0 → fenced_out).
-- ============================================================================
CREATE TYPE outbox.delivery_status AS ENUM (
    'queued', 'running', 'awaiting_retry', 'sent', 'skipped', 'dead'
);

CREATE TABLE outbox.handler_deliveries (
    id                 BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id           UUID        NOT NULL REFERENCES outbox.events(id) ON DELETE CASCADE,
    handler_id         TEXT        COLLATE "C" NOT NULL,
    status             outbox.delivery_status NOT NULL DEFAULT 'queued',
    attempts           INTEGER     NOT NULL DEFAULT 0,
    last_error         TEXT,
    -- Fencing token: NULL when not running, set to JobContext.lease_token while
    -- in 'running'. Cleared on every transition out of 'running'. All mark_*
    -- helpers WHERE lease_token = $token; mismatched (stale-worker) UPDATE
    -- returns rows_affected=0 and the wrapper emits fenced_out tracing.
    lease_token        UUID,
    first_attempted_at TIMESTAMPTZ,
    last_attempted_at  TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT handler_deliveries_handler_bytes
        CHECK (octet_length(handler_id) BETWEEN 1 AND 128),
    CONSTRAINT handler_deliveries_attempts_nonneg
        CHECK (attempts >= 0),
    CONSTRAINT handler_deliveries_last_error_bytes
        CHECK (last_error IS NULL OR octet_length(last_error) <= 8192),
    CONSTRAINT handler_deliveries_temporal CHECK (
        (first_attempted_at IS NULL OR first_attempted_at >= created_at)
        AND (last_attempted_at IS NULL OR last_attempted_at >= COALESCE(first_attempted_at, created_at))
        AND (finished_at IS NULL OR finished_at >= COALESCE(last_attempted_at, created_at))
        AND updated_at >= created_at
    ),
    -- State machine invariant: lease_token NOT NULL iff status='running'.
    -- This is what makes the fencing-token guard in mark_* meaningful — any
    -- code path producing a logically impossible state fails loudly here.
    CONSTRAINT handler_deliveries_status_invariants CHECK (
        (status = 'queued'
            AND attempts = 0
            AND first_attempted_at IS NULL
            AND last_attempted_at IS NULL
            AND finished_at IS NULL
            AND lease_token IS NULL)
        OR (status = 'running'
            AND attempts > 0
            AND first_attempted_at IS NOT NULL
            AND last_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NOT NULL)
        OR (status = 'awaiting_retry'
            AND attempts > 0
            AND first_attempted_at IS NOT NULL
            AND last_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NULL)
        OR (status IN ('sent','dead','skipped')
            AND finished_at IS NOT NULL
            AND lease_token IS NULL)
    ),

    UNIQUE (event_id, handler_id)
);

CREATE INDEX handler_deliveries_event_idx
    ON outbox.handler_deliveries (event_id);

CREATE INDEX handler_deliveries_pending_idx
    ON outbox.handler_deliveries (status, created_at)
    WHERE status IN ('queued','running','awaiting_retry');

CREATE INDEX handler_deliveries_terminal_idx
    ON outbox.handler_deliveries (finished_at)
    WHERE status IN ('sent','dead','skipped');

CREATE TRIGGER touch_handler_deliveries
    BEFORE UPDATE ON outbox.handler_deliveries
    FOR EACH ROW EXECUTE FUNCTION outbox.set_updated_at();

ALTER TABLE outbox.handler_deliveries SET (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);
