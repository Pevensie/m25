update m25.job
set
    failure_reason = 'job_timeout',
    finished_at = now()
where queue_name = $1
    and status = 'executing'
    and now() > deadline
returning
    id,
    status,
    input,
    attempt,
    max_attempts,
    original_attempt_id,
    previous_attempt_id,
    -- TODO: use duration once supported in Squirrel
    extract(epoch from retry_delay)::int as retry_delay,
    unique_key;
