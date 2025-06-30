insert into m25.job (
  id,
  queue_name,
  scheduled_at,
  input,
  attempt,
  max_attempts,
  original_attempt_id,
  previous_attempt_id,
  retry_delay,
  unique_key
) values (
  $1,
  $2,
  to_timestamp($3),
  $4::text::jsonb,
  $5,
  $6,
  $7,
  $8,
  make_interval(secs => $9),
  $10
) returning
    id,
    status,
    input,
    attempt,
    max_attempts,
    original_attempt_id,
    previous_attempt_id,
    -- TODO: use duration once supported in Squirrel
    extract(epoch from retry_delay)::int as retry_delay;
