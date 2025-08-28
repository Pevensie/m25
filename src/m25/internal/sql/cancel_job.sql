-- Cancel a job only if it's pending, and distinguish outcomes in one query
with target as (
    select
        id,
        queue_name,
        created_at::timestamp,
        scheduled_at::timestamp,
        input,
        reserved_at::timestamp,
        started_at::timestamp,
        cancelled_at::timestamp,
        finished_at::timestamp,
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
        unique_key
    from m25.job where id = $1
),
updated as (
    update m25.job j
    set cancelled_at = now()
    from target t
    where j.id = t.id
      and t.status = 'pending'
    returning
        j.id,
        j.queue_name,
        j.created_at::timestamp,
        j.scheduled_at::timestamp,
        j.input,
        j.reserved_at::timestamp,
        j.started_at::timestamp,
        j.cancelled_at::timestamp,
        j.finished_at::timestamp,
        j.status,
        j.output,
        j.deadline::timestamp,
        j.latest_heartbeat_at::timestamp,
        j.failure_reason,
        j.error_data,
        j.attempt,
        j.max_attempts,
        j.original_attempt_id,
        j.previous_attempt_id,
        extract(epoch from j.retry_delay)::int as retry_delay,
        j.unique_key
)
select
    coalesce(u.id, t.id) as id,
    coalesce(u.queue_name, t.queue_name) as queue_name,
    coalesce(u.created_at, t.created_at)::timestamp as created_at,
    coalesce(u.scheduled_at, t.scheduled_at)::timestamp as scheduled_at,
    coalesce(u.input, t.input) as input,
    coalesce(u.reserved_at, t.reserved_at)::timestamp as reserved_at,
    coalesce(u.started_at, t.started_at)::timestamp as started_at,
    coalesce(u.cancelled_at, t.cancelled_at)::timestamp as cancelled_at,
    coalesce(u.finished_at, t.finished_at)::timestamp as finished_at,
    coalesce(u.status, t.status) as status,
    coalesce(u.output, t.output) as output,
    coalesce(u.deadline, t.deadline)::timestamp as deadline,
    coalesce(u.latest_heartbeat_at, t.latest_heartbeat_at)::timestamp as latest_heartbeat_at,
    coalesce(u.failure_reason, t.failure_reason) as failure_reason,
    coalesce(u.error_data, t.error_data) as error_data,
    coalesce(u.attempt, t.attempt) as attempt,
    coalesce(u.max_attempts, t.max_attempts) as max_attempts,
    coalesce(u.original_attempt_id, t.original_attempt_id) as original_attempt_id,
    coalesce(u.previous_attempt_id, t.previous_attempt_id) as previous_attempt_id,
    coalesce(u.retry_delay, t.retry_delay) as retry_delay,
    coalesce(u.unique_key, t.unique_key) as unique_key,
    case when u.id is not null then 'cancelled' else 'not_pending' end as cancel_outcome
from target t
left join updated u on u.id = t.id;
