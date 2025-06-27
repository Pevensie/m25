create schema if not exists m25;

create table if not exists m25.job (
    id uuid not null primary key default gen_random_uuid(),
    queue_name text not null,
    created_at timestamptz not null default timezone('utc', now()),
    scheduled_at timestamptz,
    input jsonb not null,

    -- Status tracking
    status text not null default 'pending',
    started_at timestamptz,
    finished_at timestamptz,
    output jsonb,
    error jsonb,

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
