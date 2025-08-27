import gleam/bool
import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/json
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/static_supervisor as sup
import gleam/otp/supervision
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import logging
import m25/internal/bimap
import m25/internal/cli
import m25/internal/sql
import m25/internal/sql_ext
import pog
import youid/uuid

@internal
pub fn main() {
  cli.run_cli()
}

/// A queue to be used by M25. All of the input, output and error values must
/// be serialisable to JSON so that they may be inserted into the database.
///
/// Note: the concurrency count is defined per running application using M25,
/// and does not apply to the cluster as a whole.
pub type Queue(input, output, error) {
  Queue(
    /// The name of the queue. This should be unique.
    name: String,
    /// The maximum number of jobs that can be running at once.
    /// Note: this is per application, not per cluster.
    max_concurrency: Int,
    /// A function to encode the input to JSON for input into the database.
    input_to_json: fn(input) -> json.Json,
    /// A decoder to decode the JSON for the job input stored in the database.
    input_decoder: decode.Decoder(input),
    /// A function to encode the successful output of the job to JSON for input into the database.
    output_to_json: fn(output) -> json.Json,
    /// A decoder to decode the JSON for the job output stored in the database.
    output_decoder: decode.Decoder(output),
    /// A function to encode the error of the job to JSON for input into the database.
    error_to_json: fn(error) -> json.Json,
    /// A decoder to decode the JSON for the job error stored in the database.
    error_decoder: decode.Decoder(error),
    /// The handler function to process the job.
    handler_function: fn(input) -> Result(output, error),
    /// How long a job may run in milliseconds before it is considered failed.
    job_timeout: Int,
    /// How frequently the queue manager should check the database for new jobs in milliseconds.
    poll_interval: Int,
    /// How frequently to send heartbeat to the individual job manager in milliseconds.
    heartbeat_interval: Int,
    /// How many times the heartbeat is allowed to be missed before the job is considered failed.
    allowed_heartbeat_misses: Int,
    /// How long a job executor actor may take to initialise in milliseconds.
    executor_init_timeout: Int,
    /// How long a job can stay in the 'reserved' state before it should be reverted to 'pending'.
    reserved_timeout: Int,
  )
}

@external(erlang, "m25_ffi", "coerce")
fn queue_to_dynamic_queue(
  queue: Queue(input, output, error),
) -> Queue(dynamic.Dynamic, dynamic.Dynamic, dynamic.Dynamic)

pub opaque type M25 {
  M25(
    conn: pog.Connection,
    queues: List(Queue(dynamic.Dynamic, dynamic.Dynamic, dynamic.Dynamic)),
  )
}

/// Create a new M25 instance. It's recommended that you use a supervised `pog`
/// connection.
///
/// ```gleam
/// let conn_name = process.new_name("db_connection")
///
/// let conn_child =
///   pog.default_config(conn_name)
///   |> pog.host("localhost")
///   |> pog.database("my_database")
///   |> pog.pool_size(15)
///   |> pog.supervised
///
/// // Create a connection that can be accessed by our queue handlers
/// let conn = pog.named_connection(conn_name)
///
/// let m25 = m25.new(conn)
/// ```
pub fn new(conn: pog.Connection) -> M25 {
  M25(conn:, queues: [])
}

/// Register a queue to be used by M25. All of the input, output and error values must
/// be serialisable to JSON so that they may be inserted into the database.
///
/// Returns `Error(Nil)` if a queue with the same name has already been registered.
///
/// ```gleam
/// pub fn main() {
///   let assert Ok(m25) = m25.new(conn)
///     |> m25.add_queue(queue1)
///     |> result.try(m25.add_queue(_, queue2))
///     |> result.try(m25.add_queue(_, queue3))
///
///   let assert Ok(_) = m25.start(m25)
/// }
/// ```
pub fn add_queue(
  m25: M25,
  queue: Queue(input, output, error),
) -> Result(M25, Nil) {
  case
    list.find(m25.queues, fn(existing_queue) {
      queue.name == existing_queue.name
    })
  {
    Ok(_) -> Error(Nil)
    Error(_) ->
      Ok(M25(..m25, queues: [queue_to_dynamic_queue(queue), ..m25.queues]))
  }
}

/// Start M25 in an unsupervised fashion. This is not recommended. You should prefer
/// using [`supervised`](#supervised) to start M25 as part of your supervision tree.
pub fn start(
  m25: M25,
  queue_init_timeout: Int,
) -> Result(actor.Started(sup.Supervisor), actor.StartError) {
  supervisor_spec(m25, queue_init_timeout)
  |> sup.start
}

/// Create a child spec for the M25 process, allowing it to be run as part of a
/// supervision tree.
pub fn supervised(
  m25: M25,
  queue_init_timeout: Int,
) -> supervision.ChildSpecification(sup.Supervisor) {
  supervision.supervisor(fn() { start(m25, queue_init_timeout) })
}

fn supervisor_spec(m25: M25, queue_init_timeout: Int) {
  let supervisor = sup.new(sup.OneForOne)

  m25.queues
  |> list.fold(supervisor, fn(supervisor, queue) {
    sup.add(
      supervisor,
      supervision.worker(fn() {
        queue_manager_spec(queue, m25.conn, queue_init_timeout)
        |> actor.start
      })
        |> supervision.restart(supervision.Transient),
    )
  })
}

// ----- Jobs ------ //

/// A background job to be executed.
pub opaque type Job(input) {
  Job(
    input: input,
    scheduled_at: option.Option(timestamp.Timestamp),
    max_attempts: Int,
    retry_delay: option.Option(duration.Duration),
    unique_key: option.Option(String),
  )
}

/// Create a new job with default values and the given input. The input must match the
/// input type of the queue you'll be enqueuing it to.
pub fn new_job(input: input) -> Job(input) {
  Job(
    input:,
    scheduled_at: option.None,
    max_attempts: 1,
    retry_delay: option.None,
    unique_key: option.None,
  )
}

/// Schedule a job to be executed at a specific time. If that time is in the past, the
/// job will be executed immediately.
pub fn schedule(job, at scheduled_at) {
  Job(..job, scheduled_at: option.Some(scheduled_at))
}

/// Configure retry behavior for a job. If no retry delay is provided, the job will be
/// retried immediately.
pub fn retry(job, max_attempts max_attempts, delay retry_delay) {
  Job(..job, max_attempts:, retry_delay:)
}

/// Set a unique key for a job. This will prevent the job being enqueued if it already
/// exists in a non-errored state. If the only matching attempts have failed or
/// crashed, the job can still be enqueued.
pub fn unique_key(job, key unique_key) {
  Job(..job, unique_key: option.Some(unique_key))
}

/// Enqueue a job to be executed as soon as a worker is available.
pub fn enqueue(conn, queue: Queue(input, output, error), job: Job(input)) {
  insert_job(
    conn,
    queue.name,
    job.scheduled_at,
    queue.input_to_json(job.input),
    1,
    job.max_attempts,
    option.None,
    option.None,
    option.map(job.retry_delay, duration_to_seconds)
      |> option.unwrap(0.0),
    job.unique_key,
  )
}

// Supervision structure:
//
//                     M25
//                      |
//                      |
//                      |
//                Queue manager (polls DB for jobs)
//                /     |     \
//               /      |      \
//              /       |       \
//                ...Executors (bidirectional monitoring with queue manager; spawns monitored process to run job)
//             |        |        |
//             |        |        |
//             |        |        |
//                  ...Workers (monitored by executor actor)

pub type JobStatus {
  Pending
  Reserved
  Executing
  Succeeded
  Failed
  Cancelled
}

fn job_status_from_string(maybe_status: String) -> Result(JobStatus, Nil) {
  case maybe_status {
    "pending" -> Ok(Pending)
    "reserved" -> Ok(Reserved)
    "executing" -> Ok(Executing)
    "succeeded" -> Ok(Succeeded)
    "failed" -> Ok(Failed)
    "cancelled" -> Ok(Cancelled)
    _ -> Error(Nil)
  }
}

pub type FailureReason {
  Errored
  HeartbeatTimeout
  JobTimeout
  Crash
}

fn failure_reason_to_string(failure_reason) {
  case failure_reason {
    Errored -> "error"
    Crash -> "crash"
    HeartbeatTimeout -> "heartbeat_timeout"
    JobTimeout -> "job_timeout"
  }
}

fn failure_reason_from_string(reason: String) -> Result(FailureReason, Nil) {
  case reason {
    "error" -> Ok(Errored)
    "crash" -> Ok(Crash)
    "heartbeat_timeout" -> Ok(HeartbeatTimeout)
    "job_timeout" -> Ok(JobTimeout)
    _ -> Error(Nil)
  }
}

// ----- SQL wrappers ----- //

pub type JobId {
  JobId(value: uuid.Uuid)
}

pub type JobRecord(input, output, error) {
  JobRecord(
    id: JobId,
    queue_name: String,
    created_at: timestamp.Timestamp,
    scheduled_at: option.Option(timestamp.Timestamp),
    input: input,
    reserved_at: option.Option(timestamp.Timestamp),
    started_at: option.Option(timestamp.Timestamp),
    cancelled_at: option.Option(timestamp.Timestamp),
    finished_at: option.Option(timestamp.Timestamp),
    status: JobStatus,
    output: option.Option(output),
    deadline: option.Option(timestamp.Timestamp),
    latest_heartbeat_at: option.Option(timestamp.Timestamp),
    failure_reason: option.Option(FailureReason),
    error_data: option.Option(error),
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: option.Option(JobId),
    previous_attempt_id: option.Option(JobId),
    retry_delay_seconds: Int,
    unique_key: option.Option(String),
  )
}

type JobRecordDecodeError {
  JobRecordFetchInvalidStatusError(JobId, String)
  JobRecordFetchInvalidFailureReason(JobId, String)
  JobRecordFetchJsonDecodeError(JobId, json.DecodeError)
}

type JobRecordFetchError {
  JobRecordFetchQueryError(pog.QueryError)
  JobRecordFetchDecodeErrors(List(JobRecordDecodeError))
}

fn job_record_decode_error_to_string(error: JobRecordDecodeError) -> String {
  case error {
    JobRecordFetchInvalidStatusError(job_id, status) ->
      "Job " <> uuid.to_string(job_id.value) <> " has invalid status " <> status
    JobRecordFetchInvalidFailureReason(job_id, reason) ->
      "Invalid failure reason for job "
      <> uuid.to_string(job_id.value)
      <> ": "
      <> reason
    JobRecordFetchJsonDecodeError(job_id, error) ->
      "Failed to decode job "
      <> uuid.to_string(job_id.value)
      <> ": "
      <> string.inspect(error)
  }
}

fn succeed_job(conn: pog.Connection, job_id: JobId, output: json.Json) {
  sql.succeed_job(conn, job_id.value, output)
}

fn error_job(conn: pog.Connection, job_id: JobId, error: json.Json) {
  sql.error_job(conn, job_id.value, error)
}

fn fail_job(conn: pog.Connection, job_id: JobId, reason: FailureReason) {
  sql.fail_job(conn, job_id.value, failure_reason_to_string(reason))
}

fn reserve_jobs(
  conn: pog.Connection,
  queue: Queue(input, output, error),
  limit: Int,
) {
  use jobs <- result.try(
    sql_ext.reserve_jobs(conn, queue.name, limit)
    |> result.map_error(JobRecordFetchQueryError),
  )

  let #(valid_jobs, invalid_jobs) =
    jobs.rows
    |> list.map(fn(job) {
      let job_id = JobId(job.id)
      use status <- result.try(
        job_status_from_string(job.status)
        |> result.replace_error(JobRecordFetchInvalidStatusError(
          job_id,
          job.status,
        )),
      )
      use input <- result.try(
        json.parse(job.input, queue.input_decoder)
        |> result.map_error(JobRecordFetchJsonDecodeError(job_id, _)),
      )

      let output_result = case job.output {
        option.None -> Ok(option.None)
        option.Some(output) -> {
          json.parse(output, queue.output_decoder)
          |> result.map(option.Some)
          |> result.map_error(JobRecordFetchJsonDecodeError(job_id, _))
        }
      }
      use output <- result.try(output_result)

      let error_result = case job.error_data {
        option.None -> Ok(option.None)
        option.Some(error) -> {
          json.parse(error, queue.error_decoder)
          |> result.map(option.Some)
          |> result.map_error(JobRecordFetchJsonDecodeError(job_id, _))
        }
      }
      use error_data <- result.try(error_result)

      let failure_reason_result = case job.failure_reason {
        option.None -> Ok(option.None)
        option.Some(reason) -> {
          failure_reason_from_string(reason)
          |> result.map(option.Some)
          |> result.replace_error(JobRecordFetchInvalidFailureReason(
            job_id,
            reason,
          ))
        }
      }
      use failure_reason <- result.try(failure_reason_result)

      JobRecord(
        id: job_id,
        queue_name: job.queue_name,
        created_at: job.created_at,
        scheduled_at: job.scheduled_at,
        input:,
        reserved_at: job.reserved_at,
        started_at: job.started_at,
        cancelled_at: job.cancelled_at,
        finished_at: job.finished_at,
        status:,
        output:,
        deadline: job.deadline,
        latest_heartbeat_at: job.latest_heartbeat_at,
        failure_reason:,
        error_data:,
        attempt: job.attempt,
        max_attempts: job.max_attempts,
        original_attempt_id: option.map(job.original_attempt_id, JobId),
        previous_attempt_id: option.map(job.previous_attempt_id, JobId),
        retry_delay_seconds: job.retry_delay,
        unique_key: job.unique_key,
      )
      |> Ok
    })
    |> result.partition
  // |> result.all
  // |> result.map_error(JobRecordFetchDecodeError)

  case invalid_jobs {
    [] -> Ok(valid_jobs)
    invalid -> Error(JobRecordFetchDecodeErrors(invalid))
  }
}

fn finalize_job_reservations(
  conn: pog.Connection,
  successful_job_ids: List(JobId),
  failed_job_ids: List(JobId),
  timeout: Int,
) {
  sql.finalize_job_reservations(
    conn,
    list.map(successful_job_ids, fn(id) { id.value }),
    int.to_float(timeout) /. 1000.0,
    list.map(failed_job_ids, fn(id) { id.value }),
  )
}

fn cleanup_stuck_reservations(
  conn: pog.Connection,
  queue_name: String,
  timeout: Int,
) {
  sql.cleanup_stuck_reservations(
    conn,
    queue_name,
    int.to_float(timeout) /. 1000.0,
  )
}

fn insert_job(
  conn: pog.Connection,
  queue_name: String,
  scheduled_at: option.Option(timestamp.Timestamp),
  input: json.Json,
  attempt: Int,
  max_attempts: Int,
  original_attempt_id: option.Option(uuid.Uuid),
  previous_attempt_id: option.Option(uuid.Uuid),
  retry_delay: Float,
  unique_key: option.Option(String),
) -> Result(pog.Returned(sql_ext.InsertJobRow), pog.QueryError) {
  sql_ext.insert_job(
    conn,
    uuid.v7(),
    queue_name,
    option.map(scheduled_at, timestamp_to_unix_seconds_float),
    input,
    attempt,
    max_attempts,
    original_attempt_id,
    previous_attempt_id,
    retry_delay,
    unique_key,
  )
}

fn time_out_jobs(conn: pog.Connection, queue_name: String) {
  sql.time_out_jobs(conn, queue_name)
}

/// Returns a boolean representing whether the job has hit a heartbeat timeout
fn execute_job_heartbeat(
  conn,
  job_id: JobId,
  allowed_misses: Int,
  heartbeat_interval: Int,
) {
  sql.heartbeat(
    conn,
    job_id.value,
    allowed_misses,
    int.to_float(heartbeat_interval) /. 1000.0,
  )
}

fn timestamp_to_unix_seconds_float(timestamp: timestamp.Timestamp) -> Float {
  let #(seconds, nanoseconds) =
    timestamp.to_unix_seconds_and_nanoseconds(timestamp)
  int.to_float(seconds) +. int.to_float(nanoseconds) /. 1_000_000_000.0
}

// ----- Operations ------ //

type JobUpdateError {
  JobNotFound(JobId)
  TooManyJobsReturned(List(JobId))
  JobUpdateQueryError(pog.QueryError)
}

fn handle_errored_job(
  conn: pog.Connection,
  queue: Queue(input, output, error),
  job_id: JobId,
  error: error,
) {
  use conn <- pog.transaction(conn)
  use failed_job <- result.try(
    error_job(conn, job_id, queue.error_to_json(error))
    |> result.map_error(JobUpdateQueryError),
  )
  case failed_job.rows {
    [] -> Error(JobNotFound(job_id))
    [row] ->
      retry_jobs_if_needed(conn, [row.id])
      |> result.map_error(JobUpdateQueryError)
    rows ->
      Error(TooManyJobsReturned(list.map(rows, fn(row) { JobId(row.id) })))
  }
}

fn handle_failed_job(
  conn: pog.Connection,
  job_id: JobId,
  failure_reason: FailureReason,
) {
  use conn <- pog.transaction(conn)
  use crashed_job <- result.try(
    fail_job(conn, job_id, failure_reason)
    |> result.map_error(JobUpdateQueryError),
  )
  case crashed_job.rows {
    [] -> Error(JobNotFound(job_id))
    [row] ->
      retry_jobs_if_needed(conn, [row.id])
      |> result.map_error(JobUpdateQueryError)
    rows ->
      Error(TooManyJobsReturned(list.map(rows, fn(row) { JobId(row.id) })))
  }
}

fn retry_jobs_if_needed(conn: pog.Connection, job_ids: List(uuid.Uuid)) {
  sql.retry_if_needed(conn, job_ids)
}

// ----- Queue manager ----- //

type QueueManagerMsg(input, output, error) {
  ProcessJobs
  WorkSucceeded(job_id: JobId, output: output)
  WorkFailed(job_id: JobId, error: error)
  JobExecutorDown(process.Down)
  JobWorkerDown(job_id: JobId)
  Shutdown
}

type QueueManagerState(input, output, error) {
  QueueManagerState(
    self: process.Subject(QueueManagerMsg(input, output, error)),
    // selector: process.Selector(QueueManagerMsg(input, output, error)),
    queue: Queue(input, output, error),
    conn: pog.Connection,
    running_jobs: bimap.Bimap(JobId, process.Pid),
  )
}

fn queue_manager_spec(
  queue: Queue(input, output, error),
  conn: pog.Connection,
  queue_init_timeout: Int,
) {
  actor.new_with_initialiser(queue_init_timeout, fn(self) {
    process.send(self, ProcessJobs)

    let selector =
      process.new_selector()
      |> process.select(self)
      |> process.select_monitors(JobExecutorDown)

    QueueManagerState(self, queue, conn, bimap.new())
    |> actor.initialised
    |> actor.selecting(selector)
    |> actor.returning(self)
    |> Ok
  })
  |> actor.on_message(handle_queue_message)
}

type ProcessJobsError {
  ProcessJobsQueryError(when: String, error: pog.QueryError)
  ProcessJobsFetchError(JobRecordFetchError)
}

fn handle_queue_message(
  state: QueueManagerState(input, output, error),
  message: QueueManagerMsg(input, output, error),
) -> actor.Next(
  QueueManagerState(input, output, error),
  QueueManagerMsg(input, output, error),
) {
  case message {
    ProcessJobs -> {
      let reserved_jobs_result =
        pog.transaction(state.conn, fn(conn) {
          // Clean up any stuck reservations first
          use _ <- result.try(
            cleanup_stuck_reservations(
              conn,
              state.queue.name,
              state.queue.reserved_timeout,
            )
            |> result.map_error(fn(err) {
              ProcessJobsQueryError(
                when: "cleaning up stuck reservations",
                error: err,
              )
            }),
          )

          use timed_out <- result.try(
            time_out_jobs(conn, state.queue.name)
            |> result.map_error(fn(err) {
              ProcessJobsQueryError(when: "timing out jobs", error: err)
            }),
          )

          use _ <- result.try(
            timed_out.rows
            |> list.map(fn(row) { row.id })
            |> retry_jobs_if_needed(conn, _)
            |> result.map_error(fn(err) {
              ProcessJobsQueryError(when: "retrying jobs", error: err)
            }),
          )

          let limit =
            int.max(
              state.queue.max_concurrency - bimap.size(state.running_jobs),
              0,
            )

          use <- bool.guard(when: limit == 0, return: Ok([]))

          use reserved_jobs <- result.try(
            reserve_jobs(conn, state.queue, limit)
            |> result.map_error(ProcessJobsFetchError),
          )

          Ok(reserved_jobs)
        })

      case reserved_jobs_result {
        Ok(reserved_jobs) -> {
          // Phase 2: Start actors outside transaction
          let #(started, start_errors) =
            list.map(reserved_jobs, fn(job) {
              job_executor_spec(
                state.conn,
                state.queue,
                job.id,
                state.queue.handler_function,
                job.input,
                state.self,
              )
              |> actor.start
              |> result.map(fn(started) { #(job.id, started.data) })
              |> result.map_error(fn(err) { #(job.id, err) })
            })
            |> result.partition

          // Phase 3: Finalize reservations in database
          let successful_ids = list.map(started, fn(s) { s.0 })
          let failed_ids = list.map(start_errors, fn(e) { e.0 })

          let finalize_result =
            pog.transaction(state.conn, fn(conn) {
              finalize_job_reservations(
                conn,
                successful_ids,
                failed_ids,
                state.queue.job_timeout,
              )
              |> result.map_error(fn(err) {
                ProcessJobsQueryError(
                  when: "finalizing job reservations",
                  error: err,
                )
              })
            })

          case finalize_result {
            Ok(_) -> {
              let running_jobs =
                list.fold(started, state.running_jobs, fn(running, job_data) {
                  process.monitor(job_data.1.1)
                  process.send(job_data.1.0, StartWork)
                  bimap.insert(running, job_data.0, job_data.1.1)
                })

              case start_errors {
                [] -> Nil
                errors -> {
                  let failure_reason =
                    list.map(errors, fn(error) {
                      let error_string = case error.1 {
                        actor.InitExited(reason) ->
                          "Init exited: " <> string.inspect(reason)
                        actor.InitFailed(reason) -> "Init failed: " <> reason
                        actor.InitTimeout -> "Init timeout"
                      }
                      uuid.to_string({ error.0 }.value) <> ": " <> error_string
                    })
                    |> string.join("\n")

                  logging.log(
                    logging.Warning,
                    "Some actors failed to start (jobs reset to pending) for queue "
                      <> state.queue.name
                      <> ": \n"
                      <> failure_reason,
                  )
                }
              }

              process.send_after(
                state.self,
                state.queue.poll_interval,
                ProcessJobs,
              )
              actor.continue(QueueManagerState(..state, running_jobs:))
            }
            Error(finalize_error) -> {
              logging.log(
                logging.Error,
                "Failed to finalize job reservations for queue "
                  <> state.queue.name
                  <> ": "
                  <> string.inspect(finalize_error),
              )

              // Kill all started executors - they won't have started work yet as we
              // haven't sent the `StartWork` messages.
              list.each(started, fn(job_data) { process.kill(job_data.1.1) })

              process.send_after(
                state.self,
                state.queue.poll_interval,
                ProcessJobs,
              )
              actor.continue(state)
            }
          }
        }
        Error(reserve_error) -> {
          case reserve_error {
            pog.TransactionQueryError(query_error) ->
              logging.log(
                logging.Error,
                "Failed to reserve jobs for queue "
                  <> state.queue.name
                  <> ": "
                  <> string.inspect(query_error),
              )
            pog.TransactionRolledBack(ProcessJobsQueryError(when:, error:)) ->
              logging.log(
                logging.Error,
                "Transaction rolled back for queue "
                  <> state.queue.name
                  <> " when "
                  <> when
                  <> ": "
                  <> string.inspect(error),
              )
            pog.TransactionRolledBack(ProcessJobsFetchError(fetch_error)) ->
              case fetch_error {
                JobRecordFetchQueryError(query_error) ->
                  logging.log(
                    logging.Error,
                    "Query error when reserving jobs for queue "
                      <> state.queue.name
                      <> ": "
                      <> string.inspect(query_error),
                  )
                JobRecordFetchDecodeErrors(decode_errors) ->
                  logging.log(
                    logging.Error,
                    "Invalid data for multiple jobs: \n"
                      <> {
                      list.map(decode_errors, job_record_decode_error_to_string)
                      |> string.join("\n")
                    },
                  )
              }
          }

          process.send_after(state.self, state.queue.poll_interval, ProcessJobs)
          actor.continue(state)
        }
      }
    }
    WorkSucceeded(job_id:, output:) -> {
      case
        retry_exponential(3, fn() {
          succeed_job(state.conn, job_id, state.queue.output_to_json(output))
        })
      {
        Ok(_) -> Nil
        Error(query_error) ->
          logging.log(
            logging.Error,
            "Query error when succeeding job"
              <> uuid.to_string(job_id.value)
              <> ": "
              <> string.inspect(query_error),
          )
      }

      let running_jobs = bimap.delete_by_key(state.running_jobs, job_id)
      actor.continue(QueueManagerState(..state, running_jobs:))
    }
    WorkFailed(job_id:, error:) -> {
      case
        retry_exponential(3, fn() {
          handle_errored_job(state.conn, state.queue, job_id, error)
        })
      {
        Ok(_) -> Nil
        Error(query_error) ->
          logging.log(
            logging.Error,
            "Query error when failing job due to failed work for job: "
              <> uuid.to_string(job_id.value)
              <> " error: "
              <> string.inspect(query_error),
          )
      }

      let running_jobs = bimap.delete_by_key(state.running_jobs, job_id)
      actor.continue(QueueManagerState(..state, running_jobs:))
    }
    JobExecutorDown(down) -> {
      // We don't monitor ports
      let assert process.ProcessDown(pid:, reason:, ..) = down

      use <- bool.guard(
        when: reason == process.Normal,
        return: actor.continue(state),
      )

      case bimap.get_by_value(state.running_jobs, pid) {
        Error(_) -> actor.continue(state)
        Ok(job_id) -> {
          case
            retry_exponential(3, fn() {
              handle_failed_job(state.conn, job_id, Crash)
            })
          {
            Ok(_) -> Nil
            Error(query_error) ->
              logging.log(
                logging.Error,
                "Query error when failing job due to downed executor for job "
                  <> uuid.to_string(job_id.value)
                  <> ": "
                  <> string.inspect(query_error),
              )
          }

          let running_jobs = bimap.delete_by_key(state.running_jobs, job_id)
          actor.continue(QueueManagerState(..state, running_jobs:))
        }
      }
    }
    JobWorkerDown(job_id:) -> {
      case
        retry_exponential(3, fn() {
          handle_failed_job(state.conn, job_id, Crash)
        })
      {
        Ok(_) -> Nil
        Error(query_error) ->
          logging.log(
            logging.Error,
            "Query error when failing job due to downed worker for job "
              <> uuid.to_string(job_id.value)
              <> ": "
              <> string.inspect(query_error),
          )
      }

      let running_jobs = bimap.delete_by_key(state.running_jobs, job_id)
      actor.continue(QueueManagerState(..state, running_jobs:))
    }
    Shutdown -> {
      bimap.to_list(state.running_jobs)
      |> list.each(fn(job) { process.kill(job.1) })

      actor.stop()
    }
  }
}

// ----- Job executor ----- //

type JobExecutorMessage(output, error) {
  Heartbeat
  StartWork
  ExecutionSucceeded(output: output)
  ExecutionFailed(error: error)
  WorkerDown(process.ExitMessage)
  ManagerDown(process.Down)
}

type JobExecutorState(input, output, error) {
  JobExecutorState(
    self: process.Subject(JobExecutorMessage(output, error)),
    conn: pog.Connection,
    queue: Queue(input, output, error),
    job_id: JobId,
    worker_pid: option.Option(process.Pid),
    manager: process.Subject(QueueManagerMsg(input, output, error)),
    work_func: fn(input) -> Result(output, error),
    input: input,
  )
}

fn job_executor_spec(
  conn: pog.Connection,
  queue: Queue(input, output, error),
  job_id: JobId,
  work_func: fn(input) -> Result(output, error),
  input,
  manager_subject: process.Subject(QueueManagerMsg(input, output, error)),
) {
  actor.new_with_initialiser(queue.executor_init_timeout, fn(self) {
    process.trap_exits(True)

    use manager_pid <- result.try(
      process.subject_owner(manager_subject)
      |> result.replace_error(
        "Failed to get queue manager PID for job ID: "
        <> uuid.format(job_id.value, uuid.String),
      ),
    )

    let manager_monitor = process.monitor(manager_pid)

    let selector =
      process.new_selector()
      |> process.select(self)
      |> process.select_trapped_exits(WorkerDown)
      |> process.select_specific_monitor(manager_monitor, ManagerDown)

    // Subject was not created from a PID
    let assert Ok(self_pid) = process.subject_owner(self)

    JobExecutorState(
      self:,
      conn:,
      queue:,
      job_id:,
      worker_pid: option.None,
      manager: manager_subject,
      work_func:,
      input:,
    )
    |> actor.initialised
    |> actor.selecting(selector)
    |> actor.returning(#(self, self_pid))
    |> Ok
  })
  |> actor.on_message(handle_job_executor_message)
}

fn handle_job_executor_message(
  state: JobExecutorState(input, output, error),
  message: JobExecutorMessage(output, error),
) {
  case message {
    // The worker is only spawned once we get the StartWork message to avoid the
    // database getting out of sync with reality if we fail to take jobs out of
    // the 'reserved' state.
    StartWork -> {
      let worker_function = fn() {
        let message = case state.work_func(state.input) {
          Ok(output) -> ExecutionSucceeded(output)
          Error(error) -> ExecutionFailed(error)
        }

        process.send(state.self, message)
      }

      let worker_pid = process.spawn(worker_function)

      process.send_after(state.self, state.queue.heartbeat_interval, Heartbeat)

      actor.continue(
        JobExecutorState(..state, worker_pid: option.Some(worker_pid)),
      )
    }
    Heartbeat -> {
      case
        retry_exponential(3, fn() {
          execute_job_heartbeat(
            state.conn,
            state.job_id,
            state.queue.allowed_heartbeat_misses,
            state.queue.heartbeat_interval,
          )
        })
      {
        Ok(timed_out) -> {
          case timed_out.rows {
            [sql.HeartbeatRow(deadline_passed: True, ..)] -> {
              // If past deadline, just kill - queue manager will handle marking as timed
              // out and retrying
              option.map(state.worker_pid, process.kill)
              actor.stop()
            }
            [sql.HeartbeatRow(heartbeat_timed_out: False, ..)] -> {
              process.send_after(
                state.self,
                state.queue.heartbeat_interval,
                Heartbeat,
              )
              actor.continue(state)
            }
            [sql.HeartbeatRow(heartbeat_timed_out: True, ..)] -> {
              option.map(state.worker_pid, process.kill)
              case
                retry_exponential(3, fn() {
                  fail_job(state.conn, state.job_id, HeartbeatTimeout)
                })
              {
                Ok(_) -> Nil
                Error(query_error) ->
                  logging.log(
                    logging.Error,
                    "Query error when failing job due to heartbeat timeout for job "
                      <> uuid.to_string(state.job_id.value)
                      <> ": "
                      <> string.inspect(query_error),
                  )
              }
              actor.stop()
            }
            [] -> actor.continue(state)
            _ -> panic as "This should never return more than one row!"
          }
        }
        Error(query_error) -> {
          logging.log(
            logging.Error,
            "Query error when checking heartbeat for job "
              <> uuid.to_string(state.job_id.value)
              <> ": "
              <> string.inspect(query_error),
          )
          actor.stop()
        }
      }
    }
    ExecutionSucceeded(output:) -> {
      process.send(state.manager, WorkSucceeded(state.job_id, output))
      actor.stop()
    }
    ExecutionFailed(error:) -> {
      process.send(state.manager, WorkFailed(state.job_id, error))
      actor.stop()
    }
    WorkerDown(exit_message) -> {
      case exit_message.reason {
        process.Normal | process.Killed -> actor.stop()
        process.Abnormal(_) -> {
          process.send(state.manager, JobWorkerDown(state.job_id))
          actor.stop()
        }
      }
    }
    ManagerDown(_) -> {
      option.map(state.worker_pid, process.kill)
      case
        retry_exponential(3, fn() {
          handle_failed_job(state.conn, state.job_id, Crash)
        })
      {
        Ok(_) -> Nil
        Error(query_error) ->
          logging.log(
            logging.Error,
            "Query error when failing job due to downed manager for job "
              <> uuid.to_string(state.job_id.value)
              <> ": "
              <> string.inspect(query_error),
          )
      }

      actor.stop()
    }
  }
}

// ----- Utils ----- //

fn retry_exponential(total_attempts: Int, func: fn() -> Result(a, b)) {
  do_retry_exponential(1, total_attempts, func)
}

fn do_retry_exponential(
  attempt: Int,
  max_attempts: Int,
  func: fn() -> Result(a, b),
) {
  case func() {
    Ok(val) -> Ok(val)
    Error(error) -> {
      case attempt > max_attempts {
        True -> Error(error)
        False -> {
          // This is always 2 ** int, so assertion is okay
          let assert Ok(multiplier) = int.power(2, int.to_float(attempt))
          process.sleep(1000 * float.round(multiplier))
          do_retry_exponential(attempt + 1, max_attempts, func)
        }
      }
    }
  }
}

fn duration_to_seconds(duration: duration.Duration) -> Float {
  let #(seconds, nanoseconds) = duration.to_seconds_and_nanoseconds(duration)
  int.to_float(seconds)
  +. int.to_float(nanoseconds)
  /. int.to_float(1_000_000_000)
}
