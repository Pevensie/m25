select
    id,
    status,
    input,
    attempt,
    max_attempts,
    original_attempt_id,
    previous_attempt_id,
    -- TODO: use duration once supported in Squirrel
    extract(epoch from retry_delay)::int as retry_delay
from m25.job
where queue_name = $1
    and status = 'pending'
    and (scheduled_at <= now() or scheduled_at is null)
limit $2
for update skip locked
