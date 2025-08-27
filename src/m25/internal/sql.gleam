//// This module contains the code to run the sql queries defined in
//// `./src/m25/internal/sql`.
//// > 🐿️ This module was generated automatically using v4.2.0 of
//// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
////

import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option}
import gleam/time/timestamp.{type Timestamp}
import pog
import youid/uuid.{type Uuid}

/// A row you get from running the `cleanup_stuck_reservations` query
/// defined in `./src/m25/internal/sql/cleanup_stuck_reservations.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type CleanupStuckReservationsRow {
  CleanupStuckReservationsRow(id: Uuid, queue_name: String)
}

/// Runs the `cleanup_stuck_reservations` query
/// defined in `./src/m25/internal/sql/cleanup_stuck_reservations.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn cleanup_stuck_reservations(db, arg_1, arg_2) {
  let decoder = {
    use id <- decode.field(0, uuid_decoder())
    use queue_name <- decode.field(1, decode.string)
    decode.success(CleanupStuckReservationsRow(id:, queue_name:))
  }

  "update m25.job
set
    reserved_at = null
where status = 'reserved'
    and queue_name = $1
    and reserved_at < now() - make_interval(secs => $2)
returning id, queue_name;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.float(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `error_job` query
/// defined in `./src/m25/internal/sql/error_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
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
/// > 🐿️ This function was generated automatically using v4.2.0 of
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

/// A row you get from running the `fail_job` query
/// defined in `./src/m25/internal/sql/fail_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
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
/// > 🐿️ This function was generated automatically using v4.2.0 of
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

/// A row you get from running the `finalize_job_reservations` query
/// defined in `./src/m25/internal/sql/finalize_job_reservations.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type FinalizeJobReservationsRow {
  FinalizeJobReservationsRow(successful_count: Int, failed_count: Int)
}

/// Promote successful reservations to executing, revert failures to pending
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn finalize_job_reservations(db, arg_1, arg_2, arg_3) {
  let decoder = {
    use successful_count <- decode.field(0, decode.int)
    use failed_count <- decode.field(1, decode.int)
    decode.success(FinalizeJobReservationsRow(successful_count:, failed_count:))
  }

  "-- Promote successful reservations to executing, revert failures to pending
with successful_jobs as (
    update m25.job
    set
        started_at = now(),
        deadline = now() + make_interval(secs => $2)
    where id = any($1)
        and status = 'reserved'
    returning id
),
failed_jobs as (
    update m25.job
    set
        reserved_at = null
    where id = any($3)
        and status = 'reserved'
    returning id
)
select
    (select count(*) from successful_jobs) as successful_count,
    (select count(*) from failed_jobs) as failed_count;
"
  |> pog.query
  |> pog.parameter(
    pog.array(fn(value) { pog.text(uuid.to_string(value)) }, arg_1),
  )
  |> pog.parameter(pog.float(arg_2))
  |> pog.parameter(
    pog.array(fn(value) { pog.text(uuid.to_string(value)) }, arg_3),
  )
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `get_job` query
/// defined in `./src/m25/internal/sql/get_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetJobRow {
  GetJobRow(
    id: Uuid,
    queue_name: String,
    created_at: Timestamp,
    scheduled_at: Timestamp,
    input: String,
    reserved_at: Timestamp,
    started_at: Timestamp,
    cancelled_at: Timestamp,
    finished_at: Timestamp,
    status: String,
    output: Option(String),
    deadline: Timestamp,
    latest_heartbeat_at: Timestamp,
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

/// Runs the `get_job` query
/// defined in `./src/m25/internal/sql/get_job.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_job(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, uuid_decoder())
    use queue_name <- decode.field(1, decode.string)
    use created_at <- decode.field(2, pog.timestamp_decoder())
    use scheduled_at <- decode.field(3, pog.timestamp_decoder())
    use input <- decode.field(4, decode.string)
    use reserved_at <- decode.field(5, pog.timestamp_decoder())
    use started_at <- decode.field(6, pog.timestamp_decoder())
    use cancelled_at <- decode.field(7, pog.timestamp_decoder())
    use finished_at <- decode.field(8, pog.timestamp_decoder())
    use status <- decode.field(9, decode.string)
    use output <- decode.field(10, decode.optional(decode.string))
    use deadline <- decode.field(11, pog.timestamp_decoder())
    use latest_heartbeat_at <- decode.field(12, pog.timestamp_decoder())
    use failure_reason <- decode.field(13, decode.optional(decode.string))
    use error_data <- decode.field(14, decode.optional(decode.string))
    use attempt <- decode.field(15, decode.int)
    use max_attempts <- decode.field(16, decode.int)
    use original_attempt_id <- decode.field(17, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(18, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(19, decode.int)
    use unique_key <- decode.field(20, decode.optional(decode.string))
    decode.success(GetJobRow(
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
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `heartbeat` query
/// defined in `./src/m25/internal/sql/heartbeat.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type HeartbeatRow {
  HeartbeatRow(heartbeat_timed_out: Bool, deadline_passed: Bool)
}

/// Runs the `heartbeat` query
/// defined in `./src/m25/internal/sql/heartbeat.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
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

/// A row you get from running the `insert_job` query
/// defined in `./src/m25/internal/sql/insert_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type InsertJobRow {
  InsertJobRow(
    id: Uuid,
    queue_name: String,
    created_at: Timestamp,
    scheduled_at: Timestamp,
    input: String,
    reserved_at: Timestamp,
    started_at: Timestamp,
    cancelled_at: Timestamp,
    finished_at: Timestamp,
    status: String,
    output: Option(String),
    deadline: Timestamp,
    latest_heartbeat_at: Timestamp,
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

/// Runs the `insert_job` query
/// defined in `./src/m25/internal/sql/insert_job.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
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
    use queue_name <- decode.field(1, decode.string)
    use created_at <- decode.field(2, pog.timestamp_decoder())
    use scheduled_at <- decode.field(3, pog.timestamp_decoder())
    use input <- decode.field(4, decode.string)
    use reserved_at <- decode.field(5, pog.timestamp_decoder())
    use started_at <- decode.field(6, pog.timestamp_decoder())
    use cancelled_at <- decode.field(7, pog.timestamp_decoder())
    use finished_at <- decode.field(8, pog.timestamp_decoder())
    use status <- decode.field(9, decode.string)
    use output <- decode.field(10, decode.optional(decode.string))
    use deadline <- decode.field(11, pog.timestamp_decoder())
    use latest_heartbeat_at <- decode.field(12, pog.timestamp_decoder())
    use failure_reason <- decode.field(13, decode.optional(decode.string))
    use error_data <- decode.field(14, decode.optional(decode.string))
    use attempt <- decode.field(15, decode.int)
    use max_attempts <- decode.field(16, decode.int)
    use original_attempt_id <- decode.field(17, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(18, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(19, decode.int)
    use unique_key <- decode.field(20, decode.optional(decode.string))
    decode.success(InsertJobRow(
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
  $4,
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
  |> pog.parameter(pog.float(arg_3))
  |> pog.parameter(pog.text(json.to_string(arg_4)))
  |> pog.parameter(pog.int(arg_5))
  |> pog.parameter(pog.int(arg_6))
  |> pog.parameter(pog.text(uuid.to_string(arg_7)))
  |> pog.parameter(pog.text(uuid.to_string(arg_8)))
  |> pog.parameter(pog.float(arg_9))
  |> pog.parameter(pog.text(arg_10))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `reserve_jobs` query
/// defined in `./src/m25/internal/sql/reserve_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ReserveJobsRow {
  ReserveJobsRow(
    id: Uuid,
    queue_name: String,
    created_at: Timestamp,
    scheduled_at: Timestamp,
    input: String,
    reserved_at: Timestamp,
    started_at: Timestamp,
    cancelled_at: Timestamp,
    finished_at: Timestamp,
    status: String,
    output: Option(String),
    deadline: Timestamp,
    latest_heartbeat_at: Timestamp,
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

/// Runs the `reserve_jobs` query
/// defined in `./src/m25/internal/sql/reserve_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn reserve_jobs(db, arg_1, arg_2) {
  let decoder = {
    use id <- decode.field(0, uuid_decoder())
    use queue_name <- decode.field(1, decode.string)
    use created_at <- decode.field(2, pog.timestamp_decoder())
    use scheduled_at <- decode.field(3, pog.timestamp_decoder())
    use input <- decode.field(4, decode.string)
    use reserved_at <- decode.field(5, pog.timestamp_decoder())
    use started_at <- decode.field(6, pog.timestamp_decoder())
    use cancelled_at <- decode.field(7, pog.timestamp_decoder())
    use finished_at <- decode.field(8, pog.timestamp_decoder())
    use status <- decode.field(9, decode.string)
    use output <- decode.field(10, decode.optional(decode.string))
    use deadline <- decode.field(11, pog.timestamp_decoder())
    use latest_heartbeat_at <- decode.field(12, pog.timestamp_decoder())
    use failure_reason <- decode.field(13, decode.optional(decode.string))
    use error_data <- decode.field(14, decode.optional(decode.string))
    use attempt <- decode.field(15, decode.int)
    use max_attempts <- decode.field(16, decode.int)
    use original_attempt_id <- decode.field(17, decode.optional(uuid_decoder()))
    use previous_attempt_id <- decode.field(18, decode.optional(uuid_decoder()))
    use retry_delay <- decode.field(19, decode.int)
    use unique_key <- decode.field(20, decode.optional(decode.string))
    decode.success(ReserveJobsRow(
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
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `retry_if_needed` query
/// defined in `./src/m25/internal/sql/retry_if_needed.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
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

/// Runs the `succeed_job` query
/// defined in `./src/m25/internal/sql/succeed_job.sql`.
///
/// > 🐿️ This function was generated automatically using v4.2.0 of
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

/// A row you get from running the `time_out_jobs` query
/// defined in `./src/m25/internal/sql/time_out_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.2.0 of the
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
/// > 🐿️ This function was generated automatically using v4.2.0 of
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
