insert into m25.job (
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
  to_timestamp($2),
  $3::text::jsonb,
  $4,
  $5,
  $6,
  $7,
  make_interval(secs => $8),
  $9
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
