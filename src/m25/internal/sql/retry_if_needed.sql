insert into m25.job (
    queue_name,
    scheduled_at,
    input,
    timeout,
    attempt,
    max_attempts,
    original_attempt_id,
    previous_attempt_id,
    retry_delay,
    unique_key
) (
    select
        queue_name,
        now() + retry_delay as scheduled_at,
        input,
        timeout,
        attempt + 1 as attempt,
        max_attempts,
        coalesce(original_attempt_id, id) as original_attempt_id,
        id as previous_attempt_id,
        retry_delay,
        unique_key
    from m25.job
    where id = any($1)
        and attempt < max_attempts
);
