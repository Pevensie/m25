import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
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
    config: pog.Config,
    table_config: TableConfig,
    queues: List(Queue(dynamic.Dynamic, dynamic.Dynamic, dynamic.Dynamic)),
  )
}

pub fn new(config: pog.Config) -> M25 {
  M25(config:, table_config: default_table_config(), queues: [])
}

pub fn with_table_config(m25: M25, table_config: TableConfig) -> M25 {
  M25(..m25, table_config:)
}

/// Register a queue to be used by M25. All of the input, output and error values must
/// be serialisable to JSON so that they may be inserted into the database.
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
) -> Result(actor.Started(sup.Supervisor), actor.StartError) {
  supervisor_spec(m25)
  |> sup.start
}

/// Create a child spec for the M25 process, allowing it to be run as part of a
/// supervision tree.
pub fn supervised(m25: M25) -> supervision.ChildSpecification(sup.Supervisor) {
  supervision.supervisor(fn() { start(m25) })
}

fn supervisor_spec(m25: M25) {
  let supervisor = sup.new(sup.OneForOne)

  let conn = pog.connect(m25.config)

  m25.queues
  |> list.fold(supervisor, fn(supervisor, queue) {
    sup.add(
      supervisor,
      supervision.worker(fn() {
        queue_manager_spec(queue, conn)
        |> actor.start
      })
        |> supervision.restart(supervision.Transient),
    )
  })
}

/// Queue a job to be executed as soon as a worker is available.
pub fn send(
  queue: Queue(input, output, error),
  input: input,
) -> Result(Nil, Nil) {
  todo
}

/// Schedule a job to be executed once as soon after a specific time as a worker is available.
pub fn schedule_once(
  queue: Queue(input, output, error),
  input: input,
  timestamp: timestamp.Timestamp,
) -> Result(Nil, Nil) {
  todo
}

/// Schedule a job to be executed repeatedly on a cron schedule.
// pub fn schedule_recurring(
//   queue: Queue(input, output, error),
//   input: input,
//   cron: String,
// ) -> Result(Nil, Nil) {
//   todo
// }

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
  Crashed
}

fn job_status_from_string(maybe_status: String) -> Result(JobStatus, Nil) {
  case maybe_status {
    "pending" -> Ok(Pending)
    "executing" -> Ok(Executing)
    "succeeded" -> Ok(Succeeded)
    "failed" -> Ok(Failed)
    "cancelled" -> Ok(Cancelled)
    "crashed" -> Ok(Crashed)
    _ -> Error(Nil)
  }
}

fn job_status_to_string(status: JobStatus) -> String {
  case status {
    Pending -> "pending"
    Executing -> "executing"
    Succeeded -> "succeeded"
    Failed -> "failed"
    Cancelled -> "cancelled"
    Crashed -> "crashed"
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

fn fetch_executable_jobs(
  conn: pog.Connection,
  queue: Queue(input, output, error),
  limit: Int,
) -> Result(List(ExecutableJob(input, d, e)), json.DecodeError) {
  let assert Ok(jobs) = sql.fetch_executable_jobs(conn, queue.name, limit)

  jobs.rows
  |> list.map(fn(job) {
    let assert Ok(status) = job_status_from_string(job.status)
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
  })
  |> result.all
}

fn update_job_statuses(
  conn: pog.Connection,
  jobs: List(JobId),
  status: JobStatus,
) {
  sql.update_job_statuses(
    conn,
    list.map(jobs, fn(job_id) { job_id.value }),
    job_status_to_string(status),
  )
}

fn succeed_job(conn: pog.Connection, job_id: JobId, output: json.Json) {
  sql.succeed_job(conn, job_id.value, output)
}

fn fail_job(conn: pog.Connection, job_id: JobId, error: json.Json) {
  sql.fail_job(conn, job_id.value, error)
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

fn timestamp_to_unix_seconds_float(timestamp: timestamp.Timestamp) -> Float {
  let #(seconds, nanoseconds) =
    timestamp.to_unix_seconds_and_nanoseconds(timestamp)
  int.to_float(seconds) +. int.to_float(nanoseconds) /. 1_000_000_000.0
}

// ----- Operations ------ //

fn handle_failed_job(
  conn: pog.Connection,
  queue: Queue(input, output, error),
  job_id: JobId,
  error: error,
) {
  let assert Ok(failed_job) = fail_job(conn, job_id, queue.error_to_json(error))
  let assert [row] = failed_job.rows

  case row.attempt < row.max_attempts {
    // No need to retry, continue
    False -> Ok(Nil)
    True -> {
      use conn <- pog.transaction(conn)
      let assert Ok(parsed_input) = json.parse(row.input, queue.input_decoder)

      let scheduled_at = case row.retry_delay {
        delay_seconds if delay_seconds > 0 ->
          option.Some(
            timestamp.system_time()
            |> timestamp.add(duration.seconds(delay_seconds)),
          )
        _ -> option.None
      }

      let assert Ok(_) =
        insert_job(
          conn,
          row.queue_name,
          scheduled_at,
          queue.input_to_json(parsed_input),
          row.attempt + 1,
          row.max_attempts,
          option.or(row.original_attempt_id, option.Some(row.id)),
          option.Some(row.id),
          row.retry_delay |> int.to_float,
          row.unique_key,
        )

      Ok(Nil)
    }
  }
}

// ----- Queue manager ----- //

type QueueManagerMsg(output, error) {
  ProcessJobs
  WorkSucceeded(job_id: JobId, output: output)
  WorkFailed(job_id: JobId, error: error)
  JobExecutorDown(process.Down)
  JobWorkerDown(process.Down)
  Shutdown
}

type QueueManagerState(input, output, error) {
  QueueManagerState(
    self: process.Subject(QueueManagerMsg(output, error)),
    // selector: process.Selector(QueueManagerMsg(output, error)),
    queue: Queue(input, output, error),
    conn: pog.Connection,
    running_jobs: bimap.Bimap(
      JobId,
      process.Subject(JobExecutorMessage(output, error)),
    ),
  )
}

fn queue_manager_spec(queue: Queue(input, output, error), conn: pog.Connection) {
  actor.new_with_initialiser(
    // TODO: Implement timeout logic
    10_000,
    fn(self) {
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
    },
  )
  |> actor.on_message(handle_queue_message)
}

fn handle_queue_message(
  state: QueueManagerState(input, output, error),
  message: QueueManagerMsg(output, error),
) -> actor.Next(
  QueueManagerState(input, output, error),
  QueueManagerMsg(output, error),
) {
  case message {
    ProcessJobs -> {
      let start_process_result =
        pog.transaction(state.conn, fn(conn) {
          // TODO: calculate max jobs to fetch
          let assert Ok(executable_jobs) =
            fetch_executable_jobs(conn, state.queue, 10)

          let assert Ok(_) =
            update_job_statuses(
              conn,
              list.map(executable_jobs, fn(job) { job.id }),
              Executing,
            )

          let assert Ok(started) =
            list.try_map(executable_jobs, fn(job) {
              job_executor_spec(
                job.id,
                state.queue.handler_function,
                job.input,
                state.self,
              )
              |> actor.start
              |> result.map(fn(started) { #(job.id, started.data) })
            })

          let running_jobs =
            list.fold(started, state.running_jobs, fn(running, job_data) {
              bimap.insert(running, job_data.0, job_data.1)
            })

          Ok(QueueManagerState(..state, running_jobs:))
        })
        |> echo

      // TODO: use start_process_result
      let assert Ok(new_state) = start_process_result

      // TODO: allow configurable poll interval
      process.send_after(state.self, 3000, ProcessJobs)
      actor.continue(new_state)
    }
    WorkSucceeded(job_id:, output:) -> {
      let assert Ok(_) =
        retry_exponential(3, fn() {
          succeed_job(state.conn, job_id, state.queue.output_to_json(output))
        })

      let running_jobs = bimap.delete_by_key(state.running_jobs, job_id)

      io.println(
        "Job with ID "
        <> uuid.format(job_id.value, uuid.String)
        <> " succeeded with value "
        <> string.inspect(output),
      )
      actor.continue(QueueManagerState(..state, running_jobs:))
    }
    WorkFailed(job_id:, error:) -> {
      let assert Ok(_) =
        handle_failed_job(state.conn, state.queue, job_id, error)

      let running_jobs = bimap.delete_by_key(state.running_jobs, job_id)

      actor.continue(QueueManagerState(..state, running_jobs:))
    }
    JobExecutorDown(..) -> todo as "Handle job executor down"
    JobWorkerDown(..) -> todo as "Handle job worker down"
    Shutdown -> {
      todo as "Handle shutdown message"
    }
  }
}

// ----- Job executor ----- //

type JobExecutorMessage(output, error) {
  ExecutionSucceeded(output: output)
  ExecutionFailed(error: error)
  WorkerDown(process.Down)
  ManagerDown(process.Down)
}

type JobExecutorState(output, error) {
  JobExecutorState(
    manager: process.Subject(QueueManagerMsg(output, error)),
    job_id: JobId,
  )
}

fn job_executor_spec(
  job_id: JobId,
  work_func: fn(input) -> Result(output, error),
  input,
  manager_subject: process.Subject(QueueManagerMsg(output, error)),
) {
  actor.new_with_initialiser(
    // TODO: custom timeout
    1000,
    fn(self) {
      io.println(
        "Starting executor for job ID: "
        <> uuid.format(job_id.value, uuid.String),
      )

      let worker_function = fn() {
        let message = case work_func(input) {
          Ok(output) -> ExecutionSucceeded(output)
          Error(error) -> ExecutionFailed(error)
        }

        process.send(self, message)
      }

      let worker_pid = process.spawn_unlinked(worker_function)
      let worker_monitor = process.monitor(worker_pid)

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
        |> process.select_specific_monitor(worker_monitor, WorkerDown)
        |> process.select_specific_monitor(manager_monitor, ManagerDown)

      JobExecutorState(job_id:, manager: manager_subject)
      |> actor.initialised
      |> actor.selecting(selector)
      |> actor.returning(self)
      |> Ok
    },
  )
  |> actor.on_message(handle_job_executor_message)
}

fn handle_job_executor_message(state: JobExecutorState(output, error), message) {
  case message {
    ExecutionSucceeded(output:) -> {
      process.send(state.manager, WorkSucceeded(state.job_id, output))
      actor.stop()
    }
    ExecutionFailed(error:) -> {
      process.send(state.manager, WorkFailed(state.job_id, error))
      actor.stop()
    }
    ManagerDown(_) -> todo
    WorkerDown(down) -> {
      process.send(state.manager, JobWorkerDown(down))
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
