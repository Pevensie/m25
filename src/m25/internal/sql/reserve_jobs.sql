update m25.job
set
    reserved_at = now()
where id in (
    select id
    from m25.job
    where queue_name = $1
        and status = 'pending'
        and (scheduled_at <= now() or scheduled_at is null)
    order by created_at
    limit $2
    for update skip locked
)
returning
    id,
    queue_name,
    created_at::timestamp,
    scheduled_at::timestamp,
    input,
    reserved_at::timestamp,
    started_at::timestamp,
    cancelled_at::timestamp,
    finished_at::timestamp,
    extract(epoch from timeout)::int as timeout,
    status,
    output,
    deadline::timestamp,
    latest_heartbeat_at::timestamp,
    failure_reason,
    error_data,
    attempt,
    max_attempts,
    original_attempt_id,
    previous_attempt_id,
    extract(epoch from retry_delay)::int as retry_delay,
    unique_key;
