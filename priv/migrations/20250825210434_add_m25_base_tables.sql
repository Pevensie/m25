create schema if not exists m25;

create table if not exists m25.job (
    id uuid not null primary key default gen_random_uuid(),
    queue_name text not null,
    created_at timestamptz not null default timezone('utc', now()),
    scheduled_at timestamptz,
    input jsonb not null,

    -- Status tracking
    reserved_at timestamptz,
    started_at timestamptz,
    cancelled_at timestamptz,
    finished_at timestamptz,
    output jsonb,
    deadline timestamptz,
    latest_heartbeat_at timestamptz,
    failure_reason text,
    error_data jsonb,

    status text not null generated always as (
        case
            when cancelled_at is not null then 'cancelled'
            when reserved_at is null then 'pending'
            when started_at is null then 'reserved'
            when finished_at is null then 'executing'
            when failure_reason is null then 'succeeded'
            else 'failed'
        end
    ) stored,

    -- Attempt tracking
    attempt integer not null,
    max_attempts integer not null default 10,
    original_attempt_id uuid references m25.job(id),
    previous_attempt_id uuid references m25.job(id),
    retry_delay interval not null default interval '0 sec',

    -- Job uniqueness
    unique_key text
);

create unique index job_unique_key_idx
on m25.job(unique_key)
where status not in ('failed', 'cancelled');

create table if not exists m25.version (
  version timestamptz not null primary key,
  created_at timestamptz not null default now()
);
