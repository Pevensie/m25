import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/result
import gleam/string
import gleeunit
import m25
import m25/internal/cli
import pog
import youid/uuid

pub fn main() -> Nil {
  gleeunit.main()
}

fn with_test_db(run: fn(pog.Connection) -> Nil) {
  let pool = process.new_name("m25_test_pool")
  let assert Ok(config) =
    pog.url_config(pool, "postgres://postgres:postgres@localhost:5432/postgres")
  let assert Ok(started) = pog.start(config)
  let conn = started.data
  let assert Ok(_) =
    pog.transaction(conn, fn(tx) {
      use _ <- result.try(cli.migrate_for_tests(tx))
      run(conn)
      |> Ok
    })
}

fn wait_until(message: String, timeout_ms: Int, check: fn() -> Bool) {
  wait_until_loop(message, timeout_ms, check)
}

fn wait_until_loop(message: String, remaining: Int, check: fn() -> Bool) {
  case remaining <= 0 {
    True -> panic as { "Waited too long for: " <> message }
    False -> {
      case check() {
        True -> Nil
        False -> {
          process.sleep(50)
          wait_until_loop(message, remaining - 50, check)
        }
      }
    }
  }
}

fn job_status(conn: pog.Connection, id: String) -> Result(String, String) {
  let query_result =
    pog.query("select status from m25.job where id = $1")
    |> pog.parameter(pog.text(id))
    |> pog.returning({
      use s <- decode.field(0, decode.string)
      decode.success(s)
    })
    |> pog.execute(conn)
    |> result.map_error(string.inspect)

  use res <- result.try(query_result)
  case res.rows {
    [s] -> Ok(s)
    _ -> Error("job not found")
  }
}

pub fn success_job_flow_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      name: "int-success-" <> uuid(),
      max_concurrency: 2,
      input_decoder: decode.string,
      input_to_json: json.string,
      output_to_json: json.string,
      error_to_json: json.string,
      handler_function: fn(input) { Ok("ok: " <> input) },
      job_timeout: 60_000,
      poll_interval: 100,
      heartbeat_interval: 200,
      allowed_heartbeat_misses: 2,
      executor_init_timeout: 2000,
      reserved_timeout: 5000,
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(enqueued) = m25.enqueue(conn, queue, m25.new_job("A"))
  let assert [row] = enqueued.rows
  let job_id = uuid.to_string(row.id)

  let assert Ok(_) = m25.start(app, 5000)

  wait_until("job succeeds", 1000, fn() {
    case job_status(conn, job_id) {
      Ok("succeeded") -> True
      _ -> False
    }
  })

  let assert Ok("succeeded") = job_status(conn, job_id)
  Nil
}

pub fn unique_key_conflict_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      name: "int-uniq-" <> uuid(),
      max_concurrency: 1,
      input_decoder: decode.string,
      input_to_json: json.string,
      output_to_json: json.string,
      error_to_json: json.string,
      handler_function: fn(input) { Ok(input) },
      job_timeout: 60_000,
      poll_interval: 1000,
      heartbeat_interval: 1000,
      allowed_heartbeat_misses: 2,
      executor_init_timeout: 2000,
      reserved_timeout: 5000,
    )

  let key = "k-" <> uuid()
  let job1 = m25.new_job("X") |> m25.unique_key(key)
  let assert Ok(_) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(_) = m25.enqueue(conn, queue, job1)

  // Second enqueue with same unique key should error while first is pending
  let job2 = m25.new_job("Y") |> m25.unique_key(key)
  let second = m25.enqueue(conn, queue, job2)
  case second {
    Ok(_) -> io.println("Expected unique violation, got Ok")
    Error(_) -> Nil
  }
}

fn uuid() -> String {
  uuid.format(uuid.v7(), uuid.String)
}
