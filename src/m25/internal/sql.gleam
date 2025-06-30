import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option}
import pog
import youid/uuid.{type Uuid}

/// Runs the `succeed_job` query
/// defined in `./src/m25/internal/sql/succeed_job.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn succeed_job(db, arg_1, arg_2) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "update m25.job
set
    output = $2,
    finished_at = now()
where id = $1;
"
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
  |> pog.parameter(pog.text(json.to_string(arg_2)))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `start_jobs` query
/// defined in `./src/m25/internal/sql/start_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.6 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type StartJobsRow {
  StartJobsRow(
    id: Uuid,
    queue_name: String,
    status: String,
    input: String,
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: Option(Uuid),
    previous_attempt_id: Option(Uuid),
    retry_delay: Int,
    unique_key: Option(String),
  )
}

/// Runs the `start_jobs` query
/// defined in `./src/m25/internal/sql/start_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn start_jobs(db, arg_1, arg_2) {
  let decoder = {
    use id <- decode.field(0, uuid_decoder())
    use queue_name <- decode.field(1, decode.string)
    use status <- decode.field(2, decode.string)
    use input <- decode.field(3, decode.string)
    use attempt <- decode.field(4, decode.int)
    use max_attempts <- decode.field(5, decode.int)
    use original_attempt_id <- decode.field(6, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(7, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(8, decode.int)
    use unique_key <- decode.field(9, decode.optional(decode.string))
    decode.success(StartJobsRow(
      id:,
      queue_name:,
      status:,
      input:,
      attempt:,
      max_attempts:,
      original_attempt_id:,
      previous_attempt_id:,
      retry_delay:,
      unique_key:,
    ))
  }

  "update m25.job
set
    started_at = now(),
    deadline = now() + make_interval(secs => $2)
where id = any($1)
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
"
  |> pog.query
  |> pog.parameter(
    pog.array(fn(value) { pog.text(uuid.to_string(value)) }, arg_1),
  )
  |> pog.parameter(pog.float(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `error_job` query
/// defined in `./src/m25/internal/sql/error_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.6 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ErrorJobRow {
  ErrorJobRow(
    id: Uuid,
    queue_name: String,
    status: String,
    input: String,
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: Option(Uuid),
    previous_attempt_id: Option(Uuid),
    retry_delay: Int,
    unique_key: Option(String),
  )
}

/// Runs the `error_job` query
/// defined in `./src/m25/internal/sql/error_job.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn error_job(db, arg_1, arg_2) {
  let decoder = {
    use id <- decode.field(0, uuid_decoder())
    use queue_name <- decode.field(1, decode.string)
    use status <- decode.field(2, decode.string)
    use input <- decode.field(3, decode.string)
    use attempt <- decode.field(4, decode.int)
    use max_attempts <- decode.field(5, decode.int)
    use original_attempt_id <- decode.field(6, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(7, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(8, decode.int)
    use unique_key <- decode.field(9, decode.optional(decode.string))
    decode.success(ErrorJobRow(
      id:,
      queue_name:,
      status:,
      input:,
      attempt:,
      max_attempts:,
      original_attempt_id:,
      previous_attempt_id:,
      retry_delay:,
      unique_key:,
    ))
  }

  "update m25.job
set
    failure_reason = 'error',
    error_data = $2,
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
"
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
  |> pog.parameter(pog.text(json.to_string(arg_2)))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `retry_if_needed` query
/// defined in `./src/m25/internal/sql/retry_if_needed.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn retry_if_needed(db, arg_1) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "insert into m25.job (
    queue_name,
    scheduled_at,
    input,
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
"
  |> pog.query
  |> pog.parameter(
    pog.array(fn(value) { pog.text(uuid.to_string(value)) }, arg_1),
  )
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `fetch_executable_jobs` query
/// defined in `./src/m25/internal/sql/fetch_executable_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.6 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type FetchExecutableJobsRow {
  FetchExecutableJobsRow(
    id: Uuid,
    status: String,
    input: String,
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: Option(Uuid),
    previous_attempt_id: Option(Uuid),
    retry_delay: Int,
  )
}

/// Runs the `fetch_executable_jobs` query
/// defined in `./src/m25/internal/sql/fetch_executable_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn fetch_executable_jobs(db, arg_1, arg_2) {
  let decoder = {
    use id <- decode.field(0, uuid_decoder())
    use status <- decode.field(1, decode.string)
    use input <- decode.field(2, decode.string)
    use attempt <- decode.field(3, decode.int)
    use max_attempts <- decode.field(4, decode.int)
    use original_attempt_id <- decode.field(5, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(6, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(7, decode.int)
    decode.success(FetchExecutableJobsRow(
      id:,
      status:,
      input:,
      attempt:,
      max_attempts:,
      original_attempt_id:,
      previous_attempt_id:,
      retry_delay:,
    ))
  }

  "select
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
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `insert_job` query
/// defined in `./src/m25/internal/sql/insert_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.6 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type InsertJobRow {
  InsertJobRow(
    id: Uuid,
    status: String,
    input: String,
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: Option(Uuid),
    previous_attempt_id: Option(Uuid),
    retry_delay: Int,
  )
}

/// Runs the `insert_job` query
/// defined in `./src/m25/internal/sql/insert_job.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn insert_job(
  db,
  arg_1,
  arg_2,
  arg_3,
  arg_4,
  arg_5,
  arg_6,
  arg_7,
  arg_8,
  arg_9,
  arg_10,
) {
  let decoder =
  {
    use id <- decode.field(0, uuid_decoder())
    use status <- decode.field(1, decode.string)
    use input <- decode.field(2, decode.string)
    use attempt <- decode.field(3, decode.int)
    use max_attempts <- decode.field(4, decode.int)
    use original_attempt_id <- decode.field(5, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(6, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(7, decode.int)
    decode.success(InsertJobRow(
      id:,
      status:,
      input:,
      attempt:,
      max_attempts:,
      original_attempt_id:,
      previous_attempt_id:,
      retry_delay:,
    ))
  }

  "insert into m25.job (
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
"
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
  |> pog.parameter(pog.text(arg_2))
  |> pog.parameter(pog.float(arg_3))
  |> pog.parameter(pog.text(arg_4))
  |> pog.parameter(pog.int(arg_5))
  |> pog.parameter(pog.int(arg_6))
  |> pog.parameter(pog.text(uuid.to_string(arg_7)))
  |> pog.parameter(pog.text(uuid.to_string(arg_8)))
  |> pog.parameter(pog.float(arg_9))
  |> pog.parameter(pog.text(arg_10))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `fail_job` query
/// defined in `./src/m25/internal/sql/fail_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.6 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type FailJobRow {
  FailJobRow(
    id: Uuid,
    queue_name: String,
    status: String,
    input: String,
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: Option(Uuid),
    previous_attempt_id: Option(Uuid),
    retry_delay: Int,
    unique_key: Option(String),
  )
}

/// Runs the `fail_job` query
/// defined in `./src/m25/internal/sql/fail_job.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn fail_job(db, arg_1, arg_2) {
  let decoder = {
    use id <- decode.field(0, uuid_decoder())
    use queue_name <- decode.field(1, decode.string)
    use status <- decode.field(2, decode.string)
    use input <- decode.field(3, decode.string)
    use attempt <- decode.field(4, decode.int)
    use max_attempts <- decode.field(5, decode.int)
    use original_attempt_id <- decode.field(6, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(7, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(8, decode.int)
    use unique_key <- decode.field(9, decode.optional(decode.string))
    decode.success(FailJobRow(
      id:,
      queue_name:,
      status:,
      input:,
      attempt:,
      max_attempts:,
      original_attempt_id:,
      previous_attempt_id:,
      retry_delay:,
      unique_key:,
    ))
  }

  "update m25.job
set
    failure_reason = $2,
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
"
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
  |> pog.parameter(pog.text(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `heartbeat` query
/// defined in `./src/m25/internal/sql/heartbeat.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.6 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type HeartbeatRow {
  HeartbeatRow(heartbeat_timed_out: Bool, deadline_passed: Bool)
}

/// Runs the `heartbeat` query
/// defined in `./src/m25/internal/sql/heartbeat.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn heartbeat(db, arg_1, arg_2, arg_3) {
  let decoder = {
    use heartbeat_timed_out <- decode.field(0, decode.bool)
    use deadline_passed <- decode.field(1, decode.bool)
    decode.success(HeartbeatRow(heartbeat_timed_out:, deadline_passed:))
  }

  "update m25.job
set
    latest_heartbeat_at = now()
where id = $1
returning
    (
        -- This is the only way to access the old value of the row
        select
            now() - coalesce(latest_heartbeat_at, now()) > $2::int * make_interval(secs => $3) as heartbeat_timed_out
        from m25.job
        where id = $1
    ) as heartbeat_timed_out,
    now() > deadline as deadline_passed;
"
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
  |> pog.parameter(pog.int(arg_2))
  |> pog.parameter(pog.float(arg_3))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `time_out_jobs` query
/// defined in `./src/m25/internal/sql/time_out_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.6 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type TimeOutJobsRow {
  TimeOutJobsRow(
    id: Uuid,
    status: String,
    input: String,
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: Option(Uuid),
    previous_attempt_id: Option(Uuid),
    retry_delay: Int,
    unique_key: Option(String),
  )
}

/// Runs the `time_out_jobs` query
/// defined in `./src/m25/internal/sql/time_out_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn time_out_jobs(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, uuid_decoder())
    use status <- decode.field(1, decode.string)
    use input <- decode.field(2, decode.string)
    use attempt <- decode.field(3, decode.int)
    use max_attempts <- decode.field(4, decode.int)
    use original_attempt_id <- decode.field(5, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(6, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(7, decode.int)
    use unique_key <- decode.field(8, decode.optional(decode.string))
    decode.success(TimeOutJobsRow(
      id:,
      status:,
      input:,
      attempt:,
      max_attempts:,
      original_attempt_id:,
      previous_attempt_id:,
      retry_delay:,
      unique_key:,
    ))
  }

  "update m25.job
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
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

// --- Encoding/decoding utils -------------------------------------------------

/// A decoder to decode `Uuid`s coming from a Postgres query.
///
fn uuid_decoder() {
  use bit_array <- decode.then(decode.bit_array)
  case uuid.from_bit_array(bit_array) {
    Ok(uuid) -> decode.success(uuid)
    Error(_) -> decode.failure(uuid.v7(), "Uuid")
  }
}
