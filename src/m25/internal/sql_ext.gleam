//// Squirrel doesn't support everything we need it to be able to do, such as having
//// nullable columns in insert statements. This module has code copied from
//// `sql.gleam` and modified to support nullable columns.

import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option}
import gleam/time/timestamp.{type Timestamp}
import pog
import youid/uuid.{type Uuid}

/// A type for any SQL query that returns a full job record.
pub type JobRecordRow {
  JobRecordRow(
    id: Uuid,
    queue_name: String,
    created_at: Timestamp,
    scheduled_at: Option(Timestamp),
    input: String,
    reserved_at: Option(Timestamp),
    started_at: Option(Timestamp),
    cancelled_at: Option(Timestamp),
    finished_at: Option(Timestamp),
    status: String,
    output: Option(String),
    deadline: Option(Timestamp),
    latest_heartbeat_at: Option(Timestamp),
    failure_reason: Option(String),
    error_data: Option(String),
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: Option(Uuid),
    previous_attempt_id: Option(Uuid),
    retry_delay: Int,
    unique_key: Option(String),
  )
}

fn job_record_row_decoder() {
  use id <- decode.field(0, uuid_decoder())
  use queue_name <- decode.field(1, decode.string)
  use created_at <- decode.field(2, pog.timestamp_decoder())
  use scheduled_at <- decode.field(3, decode.optional(pog.timestamp_decoder()))
  use input <- decode.field(4, decode.string)
  use reserved_at <- decode.field(5, decode.optional(pog.timestamp_decoder()))
  use started_at <- decode.field(6, decode.optional(pog.timestamp_decoder()))
  use cancelled_at <- decode.field(7, decode.optional(pog.timestamp_decoder()))
  use finished_at <- decode.field(8, decode.optional(pog.timestamp_decoder()))
  use status <- decode.field(9, decode.string)
  use output <- decode.field(10, decode.optional(decode.string))
  use deadline <- decode.field(11, decode.optional(pog.timestamp_decoder()))
  use latest_heartbeat_at <- decode.field(
    12,
    decode.optional(pog.timestamp_decoder()),
  )
  use failure_reason <- decode.field(13, decode.optional(decode.string))
  use error_data <- decode.field(14, decode.optional(decode.string))
  use attempt <- decode.field(15, decode.int)
  use max_attempts <- decode.field(16, decode.int)
  use original_attempt_id <- decode.field(17, decode.optional(uuid_decoder()))
  use previous_attempt_id <- decode.field(18, decode.optional(uuid_decoder()))
  use retry_delay <- decode.field(19, decode.int)
  use unique_key <- decode.field(20, decode.optional(decode.string))
  decode.success(JobRecordRow(
    id:,
    queue_name:,
    created_at:,
    scheduled_at:,
    input:,
    reserved_at:,
    started_at:,
    cancelled_at:,
    finished_at:,
    status:,
    output:,
    deadline:,
    latest_heartbeat_at:,
    failure_reason:,
    error_data:,
    attempt:,
    max_attempts:,
    original_attempt_id:,
    previous_attempt_id:,
    retry_delay:,
    unique_key:,
  ))
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
    unique_key;
"
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
  |> pog.parameter(pog.text(arg_2))
  |> pog.parameter(pog.nullable(pog.float, arg_3))
  |> pog.parameter(pog.text(json.to_string(arg_4)))
  |> pog.parameter(pog.int(arg_5))
  |> pog.parameter(pog.int(arg_6))
  |> pog.parameter(pog.nullable(pog.text, option.map(arg_7, uuid.to_string)))
  |> pog.parameter(pog.nullable(pog.text, option.map(arg_8, uuid.to_string)))
  |> pog.parameter(pog.float(arg_9))
  |> pog.parameter(pog.nullable(pog.text, arg_10))
  |> pog.returning(job_record_row_decoder())
  |> pog.execute(db)
}

/// Runs the `reserve_jobs` query
/// defined in `./src/m25/internal/sql/reserve_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn reserve_jobs(db, arg_1, arg_2) {
  "update m25.job
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
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(job_record_row_decoder())
  |> pog.execute(db)
}

/// Runs the `get_job` query
/// defined in `./src/m25/internal/sql/get_job.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_job(db, arg_1) {
  "select
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
from m25.job where id = $1;
"
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
  |> pog.returning(job_record_row_decoder())
  |> pog.execute(db)
}

pub type CancelOutcome {
  Cancelled
  NotPending
}

/// Cancel a job only if it's pending, and distinguish outcomes in one query
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn cancel_job(db, arg_1) {
  let decoder = {
    use job_record <- decode.then(job_record_row_decoder())
    use cancel_outcome <- decode.field(21, decode.string)
    case cancel_outcome {
      "cancelled" -> decode.success(#(job_record, Cancelled))
      "not_pending" -> decode.success(#(job_record, NotPending))
      _ -> decode.failure(#(job_record, NotPending), "Cancel Outcome")
    }
  }

  "-- Cancel a job only if it's pending, and distinguish outcomes in one query
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
  "
  |> pog.query
  |> pog.parameter(pog.text(uuid.to_string(arg_1)))
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
