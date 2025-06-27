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
    status = 'succeeded',
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
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.float(arg_2))
  |> pog.parameter(pog.text(arg_3))
  |> pog.parameter(pog.int(arg_4))
  |> pog.parameter(pog.int(arg_5))
  |> pog.parameter(pog.text(uuid.to_string(arg_6)))
  |> pog.parameter(pog.text(uuid.to_string(arg_7)))
  |> pog.parameter(pog.float(arg_8))
  |> pog.parameter(pog.text(arg_9))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `update_job_statuses` query
/// defined in `./src/m25/internal/sql/update_job_statuses.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.6 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn update_job_statuses(db, arg_1, arg_2) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "update m25.job
set status = $2
where id = any($1)
"
  |> pog.query
  |> pog.parameter(
    pog.array(fn(value) { pog.text(uuid.to_string(value)) }, arg_1),
  )
  |> pog.parameter(pog.text(arg_2))
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
"
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
  |> pog.parameter(pog.text(json.to_string(arg_2)))
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
