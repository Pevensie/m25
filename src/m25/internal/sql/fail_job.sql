update m25.job
set
    status = 'failed',
    error = $2,
    finished_at = now()
where id = $1
returning
    id,
    queue_name,
    status,
    input,
    attempt,
    max_attempts,
    original_attempt_id,
    previous_attempt_id,
    -- TODO: use duration once supported in Squirrel
    extract(epoch from retry_delay)::int as retry_delay,
    unique_key;
