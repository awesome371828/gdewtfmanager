-- GDE.WTF reliability primitives. Apply once before deploying a version that
-- consumes these tables. This migration is additive and preserves existing data.
BEGIN;

CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.bot_gdewtf_operations (
    operation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text NOT NULL UNIQUE,
    action text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','processing','success','failed','unknown')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);
CREATE INDEX IF NOT EXISTS bot_gdewtf_operations_pending_idx
    ON public.bot_gdewtf_operations (next_attempt_at, created_at)
    WHERE status IN ('pending','unknown');
CREATE INDEX IF NOT EXISTS bot_gdewtf_operations_lease_idx
    ON public.bot_gdewtf_operations (lease_until)
    WHERE status = 'processing';

CREATE TABLE IF NOT EXISTS public.bot_telegram_updates (
    update_id bigint PRIMARY KEY,
    status text NOT NULL DEFAULT 'processing'
        CHECK (status IN ('processing','processed','failed')),
    received_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    lease_until timestamptz,
    last_error text
);
ALTER TABLE public.bot_telegram_updates ADD COLUMN IF NOT EXISTS lease_until timestamptz;
CREATE INDEX IF NOT EXISTS bot_telegram_updates_received_idx
    ON public.bot_telegram_updates (received_at);

CREATE TABLE IF NOT EXISTS public.bot_reliability_events (
    event_id bigserial PRIMARY KEY,
    subsystem text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('debug','info','warning','error','critical')),
    event_code text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS bot_reliability_events_created_idx
    ON public.bot_reliability_events (created_at DESC);
CREATE INDEX IF NOT EXISTS bot_reliability_events_subsystem_idx
    ON public.bot_reliability_events (subsystem, created_at DESC);

INSERT INTO public.schema_migrations(version)
VALUES ('20261102_001_reliability_foundation')
ON CONFLICT (version) DO NOTHING;

COMMIT;
