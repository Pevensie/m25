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
import gleam/time/timestamp
import logging
import m25/internal/bimap
import m25/internal/sql
import m25/internal/sql_ext
import pog
import youid/uuid

/// A queue to be used by M25. All of the input, output and error values must
/// be serialisable to JSON so that they may be inserted into the database.
///
/// Note: the concurrency count is defined per running application using M25,
/// and does not apply to the cluster as a whole.
pub type Queue(input, output, error) {
  Queue(
    name: String,
    max_concurrency: Int,
    input_decoder: decode.Decoder(input),
    input_to_json: fn(input) -> json.Json,
    output_to_json: fn(output) -> json.Json,
    error_to_json: fn(error) -> json.Json,
    handler_function: fn(input) -> Result(output, error),
    /// How long a job may run in milliseconds before it is considered failed.
    job_timeout: Int,
    /// How frequently the queue manager should check the database for new jobs in milliseconds.
    poll_interval: Int,
    /// How frequently to send heartbeat to the individual job manager in milliseconds.
    heartbeat_interval: Int,
    /// How long a job executor actor may take to initialise in milliseconds.
    executor_init_timeout: Int,
  )
}

@external(erlang, "m25_ffi", "coerce")
fn queue_to_dynamic_queue(
  queue: Queue(input, output, error),
) -> Queue(dynamic.Dynamic, dynamic.Dynamic, dynamic.Dynamic)

pub type TableConfig {
  TableConfig(schema: String, jobs_table: String)
}

/// Creates a new `TableConfig` with the default values.
pub fn default_table_config() -> TableConfig {
  TableConfig(schema: "m25", jobs_table: "job")
}

pub opaque type M25 {
  M25(
    conn: pog.Connection,
    table_config: TableConfig,
    queues: List(Queue(dynamic.Dynamic, dynamic.Dynamic, dynamic.Dynamic)),
  )
}

pub fn new(conn: pog.Connection) -> M25 {
  M25(conn:, table_config: default_table_config(), queues: [])
}

pub fn with_table_config(m25: M25, table_config: TableConfig) -> M25 {
  M25(..m25, table_config:)
}

/// Register a queue to be used by M25. All of the input, output and error values must
/// be serialisable to JSON so that they may be inserted into the database.
///
/// Returns `Error(Nil)` if a queue with the same name has already been registered.
///
/// ```gleam
/// pub fn main() {
///   let assert Ok(m25) = m25.new(config)
///     |> m25.with_table_config(table_config)
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

pub opaque type Job(input) {
  Job(
    input: input,
    scheduled_at: option.Option(timestamp.Timestamp),
    max_attempts: Int,
    retry_delay: option.Option(Int),
    unique_key: option.Option(String),
  )
}

pub fn new_job(input: input) -> Job(input) {
  Job(
    input:,
    scheduled_at: option.None,
    max_attempts: 1,
    retry_delay: option.None,
    unique_key: option.None,
  )
}

pub fn scheduled(job, scheduled_at) {
  Job(..job, scheduled_at: option.Some(scheduled_at))
}

pub fn retry(job, max_attempts, retry_delay) {
  Job(..job, max_attempts:, retry_delay:)
}

pub fn unique_key(job, unique_key) {
  Job(..job, unique_key: option.Some(unique_key))
}

/// Queue a job to be executed as soon as a worker is available.
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
    option.unwrap(job.retry_delay, 0)
      |> int.to_float
      |> float.divide(1000.0)
      |> result.unwrap(0.0),
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

type JobStatus {
  Pending
  Executing
  Succeeded
  Failed
  Cancelled
}

fn job_status_from_string(maybe_status: String) -> Result(JobStatus, Nil) {
  case maybe_status {
    "pending" -> Ok(Pending)
    "executing" -> Ok(Executing)
    "succeeded" -> Ok(Succeeded)
    "failed" -> Ok(Failed)
    "cancelled" -> Ok(Cancelled)
    _ -> Error(Nil)
  }
}

type FailureReason {
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

// ----- SQL wrappers ----- //

type JobId {
  JobId(value: uuid.Uuid)
}

type ExecutableJob(input, output, error) {
  ExecutableJob(
    id: JobId,
    status: JobStatus,
    input: input,
    attempt: Int,
    max_attempts: Int,
    original_attempt_id: option.Option(JobId),
    previous_attempt_id: option.Option(JobId),
    retry_delay_seconds: Int,
  )
}

type ExecutableJobFetchError {
  ExecutableJobFetchQueryError(pog.QueryError)
  ExecutableJobFetchDecodeError(json.DecodeError)
}

fn fetch_executable_jobs(
  conn: pog.Connection,
  queue: Queue(input, output, error),
  limit: Int,
) -> Result(List(ExecutableJob(input, d, e)), ExecutableJobFetchError) {
  use jobs <- result.try(
    sql.fetch_executable_jobs(conn, queue.name, limit)
    |> result.map_error(ExecutableJobFetchQueryError),
  )

  jobs.rows
  |> list.filter_map(fn(job) {
    case job_status_from_string(job.status) {
      Ok(status) -> {
        let job_id = JobId(value: job.id)
        job.input
        |> json.parse(queue.input_decoder)
        |> result.map(ExecutableJob(
          id: job_id,
          status:,
          input: _,
          attempt: job.attempt,
          max_attempts: job.max_attempts,
          original_attempt_id: option.map(job.original_attempt_id, JobId),
          previous_attempt_id: option.map(job.previous_attempt_id, JobId),
          retry_delay_seconds: job.retry_delay,
        ))
        |> Ok
      }
      Error(_) -> {
        logging.log(
          logging.Error,
          "Invalid job status \"" <> job.status <> "\", skipping job",
        )
        Error(Nil)
      }
    }
  })
  |> result.all
  |> result.map_error(ExecutableJobFetchDecodeError)
}

fn start_jobs(conn: pog.Connection, jobs: List(JobId), timeout: Int) {
  sql.start_jobs(
    conn,
    list.map(jobs, fn(job_id) { job_id.value }),
    int.to_float(timeout) /. 1000.0,
  )
}

fn set_jobs_to_pending(conn: pog.Connection, job_ids: List(JobId)) {
  sql.set_jobs_to_pending(conn, list.map(job_ids, fn(job_id) { job_id.value }))
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
    json.to_string(input),
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
  ProcessJobsFetchError(ExecutableJobFetchError)
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
      let start_process_result =
        pog.transaction(state.conn, fn(conn) {
          use timed_out <- result.try(
            time_out_jobs(state.conn, state.queue.name)
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

          use <- bool.guard(when: limit == 0, return: Ok(state))

          use executable_jobs <- result.try(
            fetch_executable_jobs(conn, state.queue, limit)
            |> result.map_error(ProcessJobsFetchError),
          )

          use _ <- result.try(
            start_jobs(
              conn,
              list.map(executable_jobs, fn(job) { job.id }),
              state.queue.job_timeout,
            )
            |> result.map_error(fn(err) {
              ProcessJobsQueryError(when: "marking jobs as started", error: err)
            }),
          )

          let #(started, start_errors) =
            list.map(executable_jobs, fn(job) {
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

          case start_errors {
            [] -> Nil
            // If all failed, return an error
            start_errors -> {
              let failure_reason =
                list.map(start_errors, fn(error) {
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
                logging.Error,
                "Failed to start actor(s):\n" <> failure_reason,
              )

              let job_ids = list.map(start_errors, fn(error) { error.0 })

              // TODO: figure out a nicer way of handling this
              case set_jobs_to_pending(conn, job_ids) {
                Ok(_) -> Nil
                Error(_) ->
                  logging.log(
                    logging.Error,
                    "Failed to reset jobs to pending status: "
                      <> list.map(job_ids, fn(job_id) {
                      uuid.to_string(job_id.value)
                    })
                    |> string.join(", "),
                  )
              }
            }
          }

          let running_jobs =
            list.fold(started, state.running_jobs, fn(running, job_data) {
              process.monitor(job_data.1)
              bimap.insert(running, job_data.0, job_data.1)
            })

          Ok(QueueManagerState(..state, running_jobs:))
        })

      case start_process_result {
        Ok(new_state) -> {
          process.send_after(state.self, state.queue.poll_interval, ProcessJobs)
          actor.continue(new_state)
        }
        Error(start_process_error) -> {
          case start_process_error {
            pog.TransactionQueryError(query_error) ->
              logging.log(
                logging.Error,
                "Failed to run transaction to start jobs: "
                  <> string.inspect(query_error),
              )
            pog.TransactionRolledBack(ProcessJobsQueryError(when:, error:)) ->
              logging.log(
                logging.Error,
                "Postgres query failed when "
                  <> when
                  <> ": "
                  <> string.inspect(error),
              )
            pog.TransactionRolledBack(ProcessJobsFetchError(error)) ->
              logging.log(
                logging.Error,
                "Postgres query failed when fetching executable jobs: "
                  <> case error {
                  ExecutableJobFetchDecodeError(decode_error) ->
                    "Decode error: " <> string.inspect(decode_error)
                  ExecutableJobFetchQueryError(query_error) ->
                    "Query error: " <> string.inspect(query_error)
                },
              )
          }

          process.send_after(state.self, state.queue.poll_interval, ProcessJobs)
          actor.continue(state)
        }
      }
    }
    WorkSucceeded(job_id:, output:) -> {
      let assert Ok(_) =
        retry_exponential(3, fn() {
          succeed_job(state.conn, job_id, state.queue.output_to_json(output))
        })

      let running_jobs = bimap.delete_by_key(state.running_jobs, job_id)
      actor.continue(QueueManagerState(..state, running_jobs:))
    }
    WorkFailed(job_id:, error:) -> {
      let assert Ok(_) =
        handle_errored_job(state.conn, state.queue, job_id, error)

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
          let assert Ok(_) = handle_failed_job(state.conn, job_id, Crash)

          let running_jobs = bimap.delete_by_key(state.running_jobs, job_id)
          actor.continue(QueueManagerState(..state, running_jobs:))
        }
      }
    }
    JobWorkerDown(job_id:) -> {
      let assert Ok(_) = handle_failed_job(state.conn, job_id, Crash)

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
    worker_pid: process.Pid,
    manager: process.Subject(QueueManagerMsg(input, output, error)),
    heartbeat_interval: Int,
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

    let worker_function = fn() {
      // Wait a second to let the queue manager start monitoring the executor
      process.sleep(1000)

      let message = case work_func(input) {
        Ok(output) -> ExecutionSucceeded(output)
        Error(error) -> ExecutionFailed(error)
      }

      process.send(self, message)
    }

    let worker_pid = process.spawn(worker_function)

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

    process.send_after(self, queue.heartbeat_interval, Heartbeat)

    JobExecutorState(
      self:,
      conn:,
      queue:,
      job_id:,
      worker_pid:,
      manager: manager_subject,
      heartbeat_interval: queue.heartbeat_interval,
    )
    |> actor.initialised
    |> actor.selecting(selector)
    |> actor.returning(self_pid)
    |> Ok
  })
  |> actor.on_message(handle_job_executor_message)
}

fn handle_job_executor_message(
  state: JobExecutorState(input, output, error),
  message,
) {
  case message {
    Heartbeat -> {
      let assert Ok(timed_out) =
        execute_job_heartbeat(
          state.conn,
          state.job_id,
          // TODO: custom allowed misses
          3,
          state.heartbeat_interval,
        )

      case timed_out.rows {
        [sql.HeartbeatRow(deadline_passed: True, ..)] -> {
          // If past deadline, just kill - queue manager will handle marking as timed
          // out and retrying
          process.kill(state.worker_pid)
          actor.stop()
        }
        [sql.HeartbeatRow(heartbeat_timed_out: False, ..)] -> {
          process.send_after(state.self, state.heartbeat_interval, Heartbeat)
          actor.continue(state)
        }
        [sql.HeartbeatRow(heartbeat_timed_out: True, ..)] -> {
          process.kill(state.worker_pid)
          let assert Ok(_) =
            fail_job(state.conn, state.job_id, HeartbeatTimeout)
          actor.stop()
        }
        [] -> actor.continue(state)
        _ -> panic as "This should never return more than one row!"
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
      process.kill(state.worker_pid)
      let assert Ok(_) = handle_failed_job(state.conn, state.job_id, Crash)
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
