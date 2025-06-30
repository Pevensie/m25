import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/option
import gleam/result
import m25
import pog

pub fn main() -> Nil {
  let conn =
    pog.default_config()
    |> pog.user("postgres")
    |> pog.password(option.Some("postgres"))
    |> pog.database("postgres")
    |> pog.ssl(pog.SslDisabled)
    |> pog.connect

  let success_queue =
    m25.Queue(
      name: "success-queue",
      max_concurrency: 10,
      input_decoder: decode.string,
      input_to_json: json.string,
      output_to_json: json.string,
      error_to_json: json.string,
      handler_function: fn(input) {
        io.println("Running job with input: " <> input)
        Ok("Ok")
      },
    )
  let fail_queue =
    m25.Queue(..success_queue, name: "fail-queue", handler_function: fn(input) {
      Error("Failed with input: " <> input)
    })
  let crash_queue =
    m25.Queue(..success_queue, name: "crash-queue", handler_function: fn(_) {
      panic as "Crashed!"
    })
  let long_queue =
    m25.Queue(..success_queue, name: "long-queue", handler_function: fn(input) {
      process.sleep(20_000)
      Ok("Long job completed with input: " <> input)
    })

  let assert Ok(m25) =
    m25.new(conn)
    |> m25.add_queue(success_queue)
    |> result.try(m25.add_queue(_, fail_queue))
    |> result.try(m25.add_queue(_, crash_queue))
    |> result.try(m25.add_queue(_, long_queue))

  let success_job = m25.new_job("Success!")
  let fail_job =
    m25.new_job("Fail!")
    |> m25.retry(3, option.Some(3000))
  let crash_job =
    m25.new_job("Crash!")
    |> m25.retry(3, option.Some(3000))
  let long_job = m25.new_job("Long!")

  let assert Ok(_) = m25.enqueue(conn, success_queue, success_job)
  let assert Ok(_) = m25.enqueue(conn, fail_queue, fail_job)
  let assert Ok(_) = m25.enqueue(conn, crash_queue, crash_job)
  let assert Ok(_) = m25.enqueue(conn, long_queue, long_job)

  let assert Ok(_supervisor) = m25.start(m25)
  process.sleep_forever()
}
