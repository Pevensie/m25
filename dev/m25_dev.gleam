import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/option
import gleam/result
import m25
import pog

pub fn main() -> Nil {
  let config =
    pog.default_config()
    |> pog.user("postgres")
    |> pog.password(option.Some("postgres"))
    |> pog.database("postgres")
    |> pog.ssl(pog.SslDisabled)

  let default_queue =
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

  let assert Ok(m25) =
    m25.new(config)
    |> m25.add_queue(default_queue)
    |> result.try(m25.add_queue(
      _,
      m25.Queue(
        ..default_queue,
        name: "fail-queue",
        handler_function: fn(input) { Error("Failed with input: " <> input) },
      ),
    ))

  let assert Ok(_supervisor_pid) = m25.start(m25)
  process.sleep_forever()
}
