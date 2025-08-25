import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/option
import gleam/result
import m25
import pog

type JobInput {
  JobInput(value: String)
}

fn job_input_to_json(job_input: JobInput) -> json.Json {
  let JobInput(value:) = job_input
  json.object([#("value", json.string(value))])
}

fn job_input_decoder() -> decode.Decoder(JobInput) {
  use value <- decode.field("value", decode.string)
  decode.success(JobInput(value:))
}

pub fn main() -> Nil {
  let pool_name = process.new_name("pog")
  let assert Ok(started) =
    pog.default_config(pool_name)
    |> pog.user("postgres")
    |> pog.password(option.Some("postgres"))
    |> pog.database("postgres")
    |> pog.ssl(pog.SslDisabled)
    |> pog.start

  let conn = started.data

  let success_queue =
    m25.Queue(
      name: "success-queue",
      max_concurrency: 10,
      input_decoder: job_input_decoder(),
      input_to_json: job_input_to_json,
      output_to_json: json.string,
      error_to_json: json.string,
      handler_function: fn(input) {
        io.println("Running job with input: " <> input.value)
        Ok("Ok")
      },
      job_timeout: 60 * 60 * 1000,
      poll_interval: 5000,
      heartbeat_interval: 3000,
      allowed_heartbeat_misses: 3,
      executor_init_timeout: 1000,
      reserved_timeout: 300_000,
    )
  let fail_queue =
    m25.Queue(
      ..success_queue,
      name: "fail-queue",
      handler_function: fn(input: JobInput) {
        Error("Failed with input: " <> input.value)
      },
    )
  let crash_queue =
    m25.Queue(..success_queue, name: "crash-queue", handler_function: fn(_) {
      panic as "Crashed!"
    })
  let long_queue =
    m25.Queue(
      ..success_queue,
      name: "long-queue",
      handler_function: fn(input: JobInput) {
        process.sleep(20_000)
        Ok("Long job completed with input: " <> input.value)
      },
    )

  let assert Ok(m25) =
    m25.new(conn)
    |> m25.add_queue(success_queue)
    |> result.try(m25.add_queue(_, fail_queue))
    |> result.try(m25.add_queue(_, crash_queue))
    |> result.try(m25.add_queue(_, long_queue))

  let success_job = m25.new_job(JobInput("Success!"))
  let fail_job =
    m25.new_job(JobInput("Fail!"))
    |> m25.retry(3, option.Some(3000))
  let crash_job =
    m25.new_job(JobInput("Crash!"))
    |> m25.retry(3, option.Some(3000))
  let long_job = m25.new_job(JobInput("Long!"))

  let assert Ok(_) = m25.enqueue(conn, success_queue, success_job)
  let assert Ok(_) = m25.enqueue(conn, fail_queue, fail_job)
  let assert Ok(_) = m25.enqueue(conn, crash_queue, crash_job)
  let assert Ok(_) = m25.enqueue(conn, long_queue, long_job)

  let assert Ok(_supervisor) = m25.start(m25, 10_000)
  process.sleep_forever()
}
