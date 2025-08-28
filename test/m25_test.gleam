import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/option
import gleam/result
import gleam/string
import gleam/time/timestamp
import gleeunit
import m25
import m25/internal/cli
import pog
import tempo/duration
import youid/uuid

pub fn main() -> Nil {
  let assert Ok(_) = with_test_db(cli.migrate_for_tests)
  gleeunit.main()
}

fn with_test_db(run: fn(pog.Connection) -> a) {
  let pool = process.new_name("m25_test_pool")
  let assert Ok(config) =
    pog.url_config(pool, "postgres://postgres:postgres@localhost:5432/postgres")
  let assert Ok(started) = pog.start(config)
  let conn = started.data
  let now = timestamp.system_time()
  let result = run(conn)

  let assert Ok(_) =
    pog.query("delete from m25.job where created_at > $1")
    |> pog.parameter(pog.timestamp(now))
    |> pog.execute(conn)

  process.send_exit(started.pid)

  result
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

fn default_test_queue() {
  m25.Queue(
    name: "",
    max_concurrency: 2,
    input_decoder: decode.string,
    input_to_json: json.string,
    output_decoder: decode.string,
    output_to_json: json.string,
    error_decoder: decode.string,
    error_to_json: json.string,
    handler_function: fn(_) { Ok("") },
    default_job_timeout: duration.seconds(5),
    poll_interval: 50,
    heartbeat_interval: 500,
    allowed_heartbeat_misses: 10,
    executor_init_timeout: 2000,
    reserved_timeout: 5000,
  )
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

fn chain_completed_attempt_count(
  conn: pog.Connection,
  original_id: String,
) -> Result(Int, String) {
  let query_result =
    pog.query(
      "select count(*)::int from m25.job where coalesce(original_attempt_id, id) = $1 and finished_at is not null",
    )
    |> pog.parameter(pog.text(original_id))
    |> pog.returning({
      use c <- decode.field(0, decode.int)
      decode.success(c)
    })
    |> pog.execute(conn)
    |> result.map_error(string.inspect)

  use res <- result.try(query_result)
  case res.rows {
    [c] -> Ok(c)
    _ -> Error("bad row count")
  }
}

fn last_failure_reason(
  conn: pog.Connection,
  original_id: String,
) -> Result(option.Option(String), String) {
  let query_result =
    pog.query(
      "select failure_reason from m25.job where coalesce(original_attempt_id, id) = $1 order by attempt desc limit 1",
    )
    |> pog.parameter(pog.text(original_id))
    |> pog.returning({
      use reason <- decode.field(0, decode.optional(decode.string))
      decode.success(reason)
    })
    |> pog.execute(conn)
    |> result.map_error(string.inspect)

  use res <- result.try(query_result)
  case res.rows {
    [r] -> Ok(r)
    _ -> Error("no rows")
  }
}

fn job_output_text(
  conn: pog.Connection,
  id: String,
) -> Result(option.Option(String), String) {
  let query_result =
    pog.query("select output::text from m25.job where id = $1")
    |> pog.parameter(pog.text(id))
    |> pog.returning({
      use s <- decode.field(0, decode.optional(decode.string))
      decode.success(s)
    })
    |> pog.execute(conn)
    |> result.map_error(string.inspect)

  use res <- result.try(query_result)
  case res.rows {
    [s] -> Ok(s)
    _ -> Error("no rows")
  }
}

fn last_error_data_text(
  conn: pog.Connection,
  original_id: String,
) -> Result(option.Option(String), String) {
  let query_result =
    pog.query(
      "select error_data::text from m25.job where coalesce(original_attempt_id, id) = $1 order by attempt desc limit 1",
    )
    |> pog.parameter(pog.text(original_id))
    |> pog.returning({
      use s <- decode.field(0, decode.optional(decode.string))
      decode.success(s)
    })
    |> pog.execute(conn)
    |> result.map_error(string.inspect)

  use res <- result.try(query_result)
  case res.rows {
    [s] -> Ok(s)
    _ -> Error("no rows")
  }
}

fn attempt_times(
  conn: pog.Connection,
  original_id: String,
) -> Result(
  List(#(Int, option.Option(Int), option.Option(Int), option.Option(Int))),
  String,
) {
  // Returns list of #(attempt, finished_at_sec, started_at_sec, scheduled_at_sec)
  let query_result =
    pog.query(
      "select attempt, extract(epoch from finished_at)::int, extract(epoch from started_at)::int, extract(epoch from scheduled_at)::int from m25.job where coalesce(original_attempt_id, id) = $1 order by attempt",
    )
    |> pog.parameter(pog.text(original_id))
    |> pog.returning({
      use attempt <- decode.field(0, decode.int)
      use finished <- decode.field(1, decode.optional(decode.int))
      use started <- decode.field(2, decode.optional(decode.int))
      use scheduled <- decode.field(3, decode.optional(decode.int))
      decode.success(#(attempt, finished, started, scheduled))
    })
    |> pog.execute(conn)
    |> result.map_error(string.inspect)

  result.map(query_result, fn(res) { res.rows })
}

pub fn success_job_flow_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-success-" <> uuid(),
      handler_function: fn(input) { Ok("ok: " <> input) },
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("A"))
  let job_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("job succeeds", 1000, fn() {
    case job_status(conn, job_id) {
      Ok("succeeded") -> True
      _ -> False
    }
  })

  process.send_exit(started.pid)
}

pub fn unique_key_conflict_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-uniq-" <> uuid(),
      handler_function: fn(input) { Ok(input) },
    )

  let key = "k-" <> uuid()
  let job1 = m25.new_job("X") |> m25.unique_key(key)
  let assert Ok(_) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(_) = m25.enqueue(conn, queue, job1)

  // Second enqueue with same unique key should error while first is pending
  let job2 = m25.new_job("Y") |> m25.unique_key(key)
  let assert Error(_) = m25.enqueue(conn, queue, job2)
    as "Expected unique violation"
}

pub fn unique_key_after_failure_allows_enqueue_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-uniq-after-fail-" <> uuid(),
      handler_function: fn(_) { Error("boom") },
    )

  let key = "k-" <> uuid()
  let job1 =
    m25.new_job("X") |> m25.unique_key(key) |> m25.retry(1, option.None)
  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, job1)
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  // Wait for failure
  wait_until("job fails", 10_000, fn() {
    case last_failure_reason(conn, original_id) {
      Ok(option.Some("error")) -> True
      _ -> False
    }
  })

  // Now enqueue with same unique key should be allowed (previous attempts failed)
  let job2 = m25.new_job("Y") |> m25.unique_key(key)
  let assert Ok(_) = m25.enqueue(conn, queue, job2)

  process.send_exit(started.pid)
}

pub fn unique_key_after_success_blocks_enqueue_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-uniq-after-success-" <> uuid(),
      handler_function: fn(input) { Ok(input) },
    )

  let key = "k-" <> uuid()
  let job1 = m25.new_job("X") |> m25.unique_key(key)
  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, job1)
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("job succeeds", 5000, fn() {
    case job_status(conn, original_id) {
      Ok("succeeded") -> True
      _ -> False
    }
  })

  // Second enqueue with same unique key should now fail due to success
  let job2 = m25.new_job("Y") |> m25.unique_key(key)
  let assert Error(_) = m25.enqueue(conn, queue, job2)

  process.send_exit(started.pid)
}

pub fn failing_job_retries_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-retry-fail-" <> uuid(),
      handler_function: fn(_) { Error("boom") },
    )

  let job = m25.new_job("X") |> m25.retry(3, option.Some(duration.seconds(1)))
  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, job)
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("3 attempts appear", 10_000, fn() {
    case chain_completed_attempt_count(conn, original_id) {
      Ok(count) -> count >= 3
      _ -> False
    }
  })

  let assert Ok(3) = chain_completed_attempt_count(conn, original_id)
  let assert Ok(option.Some("error")) = last_failure_reason(conn, original_id)

  process.send_exit(started.pid)
}

pub fn retry_immediate_delay_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-retry-immediate-" <> uuid(),
      handler_function: fn(_) { Error("boom") },
    )

  let job = m25.new_job("X") |> m25.retry(2, option.None)
  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, job)
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("2 attempts appear quickly", 5000, fn() {
    case chain_completed_attempt_count(conn, original_id) {
      Ok(count) -> count >= 2
      _ -> False
    }
  })

  process.send_exit(started.pid)
}

pub fn retry_delay_spacing_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-retry-spacing-" <> uuid(),
      handler_function: fn(_) { Error("boom") },
    )

  let delay = duration.seconds(2)
  let job = m25.new_job("X") |> m25.retry(2, option.Some(delay))
  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, job)
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  // Wait until 2 attempts completed
  wait_until("2 attempts appear", 10_000, fn() {
    case chain_completed_attempt_count(conn, original_id) {
      Ok(count) -> count >= 2
      _ -> False
    }
  })

  let assert Ok(times) = attempt_times(conn, original_id)
  // Expect at least two attempts
  let assert [first, second, ..] = times
  let assert #(1, option.Some(finished1), _, _) = first
  let assert #(2, _, option.Some(started2), _) = second
  assert started2 - finished1 >= 2

  process.send_exit(started.pid)
}

pub fn crash_job_retries_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-retry-crash-" <> uuid(),
      handler_function: fn(_) { panic as "crash" },
    )

  let job =
    m25.new_job("X")
    |> m25.retry(max_attempts: 2, delay: option.Some(duration.milliseconds(1)))
  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, job)
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("2 attempts appear", 10_000, fn() {
    case chain_completed_attempt_count(conn, original_id) {
      Ok(count) -> count >= 2
      _ -> False
    }
  })

  let assert Ok(option.Some("crash")) = last_failure_reason(conn, original_id)

  process.send_exit(started.pid)
}

pub fn queue_timeout_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-queue-timeout-" <> uuid(),
      handler_function: fn(_) {
        process.sleep(3000)
        Ok("done")
      },
      default_job_timeout: duration.seconds(1),
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("X"))
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("job times out", 10_000, fn() {
    case last_failure_reason(conn, original_id) {
      Ok(option.Some("job_timeout")) -> True
      _ -> False
    }
  })

  process.send_exit(started.pid)
}

/// Test that a job's timeout will override the queue's timeout
pub fn job_timeout_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-job-timeout-" <> uuid(),
      handler_function: fn(_) {
        process.sleep(3000)
        Ok("done")
      },
      default_job_timeout: duration.hours(1),
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) =
    m25.enqueue(
      conn,
      queue,
      m25.new_job("X") |> m25.timeout(duration.milliseconds(100)),
    )
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("job times out", 10_000, fn() {
    case last_failure_reason(conn, original_id) {
      Ok(option.Some("job_timeout")) -> True
      _ -> False
    }
  })

  process.send_exit(started.pid)
}

fn executing_count(
  conn: pog.Connection,
  queue_name: String,
) -> Result(Int, String) {
  let query_result =
    pog.query(
      "select count(*)::int from m25.job where queue_name = $1 and status = 'executing'",
    )
    |> pog.parameter(pog.text(queue_name))
    |> pog.returning({
      use c <- decode.field(0, decode.int)
      decode.success(c)
    })
    |> pog.execute(conn)
    |> result.map_error(string.inspect)

  use res <- result.try(query_result)
  case res.rows {
    [c] -> Ok(c)
    _ -> Error("no count")
  }
}

fn sample_max_executing(
  conn: pog.Connection,
  queue_name: String,
  remaining_ms: Int,
  current_max: Int,
) -> Result(Int, String) {
  use count <- result.try(executing_count(conn, queue_name))
  let new_max = case count > current_max {
    True -> count
    False -> current_max
  }
  case remaining_ms <= 0 {
    True -> Ok(new_max)
    False -> {
      process.sleep(100)
      sample_max_executing(conn, queue_name, remaining_ms - 100, new_max)
    }
  }
}

pub fn heartbeat_timeout_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-heartbeat-" <> uuid(),
      handler_function: fn(_) {
        process.sleep(5000)
        Ok("done")
      },
      heartbeat_interval: 100,
      allowed_heartbeat_misses: 0,
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("X"))
  let original_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("final failure reason=heartbeat_timeout", 10_000, fn() {
    case last_failure_reason(conn, original_id) {
      Ok(option.Some("heartbeat_timeout")) -> True
      _ -> False
    }
  })

  process.send_exit(started.pid)
}

pub fn heartbeat_no_timeout_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-heartbeat-ok-" <> uuid(),
      handler_function: fn(_) {
        process.sleep(800)
        Ok("done")
      },
      heartbeat_interval: 100,
      allowed_heartbeat_misses: 10,
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(_) = m25.enqueue(conn, queue, m25.new_job("X"))

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("job succeeds without heartbeat timeout", 5000, fn() {
    // Query for any succeeded job in the queue
    case executing_count(conn, queue.name) {
      Ok(_) -> {
        let query_result =
          pog.query(
            "select count(*)::int from m25.job where queue_name = $1 and status = 'succeeded'",
          )
          |> pog.parameter(pog.text(queue.name))
          |> pog.returning({
            use c <- decode.field(0, decode.int)
            decode.success(c)
          })
          |> pog.execute(conn)
          |> result.map_error(string.inspect)
        case query_result {
          Ok(res) -> {
            case res.rows {
              [c] -> c > 0
              _ -> False
            }
          }
          Error(_) -> False
        }
      }
      _ -> False
    }
  })

  process.send_exit(started.pid)
}

pub fn max_concurrency_cap_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-concurrency-" <> uuid(),
      max_concurrency: 2,
      handler_function: fn(_) {
        process.sleep(1500)
        Ok("done")
      },
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let job = m25.new_job("X")
  let assert Ok(_) = m25.enqueue(conn, queue, job)
  let assert Ok(_) = m25.enqueue(conn, queue, job)
  let assert Ok(_) = m25.enqueue(conn, queue, job)
  let assert Ok(_) = m25.enqueue(conn, queue, job)
  let assert Ok(_) = m25.enqueue(conn, queue, job)

  let assert Ok(started) = m25.start(app, 5000)

  // Wait until at least one job is executing, then sample for 2 seconds
  wait_until("some executing", 5000, fn() {
    case executing_count(conn, queue.name) {
      Ok(c) -> c > 0
      _ -> False
    }
  })
  let assert Ok(max_seen) = sample_max_executing(conn, queue.name, 2000, 0)
  assert max_seen <= 2

  process.send_exit(started.pid)
}

pub fn deadline_started_at_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-deadline-" <> uuid(),
      handler_function: fn(_) {
        process.sleep(3000)
        Ok("done")
      },
      default_job_timeout: duration.seconds(5),
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("X"))
  let job_id = uuid.to_string(job.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  // Wait for executing status
  wait_until("job executing", 5000, fn() {
    case job_status(conn, job_id) {
      Ok("executing") -> True
      _ -> False
    }
  })

  // Check deadline-started_at difference is ~ 5 seconds
  let query_result =
    pog.query(
      "select extract(epoch from (deadline - started_at))::int from m25.job where id = $1",
    )
    |> pog.parameter(pog.text(job_id))
    |> pog.returning({
      use s <- decode.field(0, decode.int)
      decode.success(s)
    })
    |> pog.execute(conn)
    |> result.map_error(string.inspect)

  let assert Ok(res) = query_result
  let assert [seconds] = res.rows
  assert seconds == 5

  process.send_exit(started.pid)
}

pub fn scheduled_future_and_past_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-schedule-" <> uuid(),
      handler_function: fn(input) { Ok("ok: " <> input) },
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)

  // Future scheduled job shouldn't execute before time
  let future_time = timestamp.add(timestamp.system_time(), duration.seconds(2))
  let job_future = m25.new_job("F") |> m25.schedule(at: future_time)
  let assert Ok(job_future) = m25.enqueue(conn, queue, job_future)
  let future_id = uuid.to_string(job_future.id.value)

  // Past scheduled job should run immediately
  let past_time = timestamp.add(timestamp.system_time(), duration.seconds(-1))
  let job_past = m25.new_job("P") |> m25.schedule(at: past_time)
  let assert Ok(job_past) = m25.enqueue(conn, queue, job_past)
  let past_id = uuid.to_string(job_past.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  // Ensure future job remains pending for ~1s
  process.sleep(1000)
  let assert Ok("pending") = job_status(conn, future_id)

  // Past job should succeed quickly
  wait_until("past job succeeds", 1000, fn() {
    case job_status(conn, past_id) {
      Ok("succeeded") -> True
      _ -> False
    }
  })

  // Future job should eventually succeed too
  wait_until("future job succeeds", 5000, fn() {
    case job_status(conn, future_id) {
      Ok("succeeded") -> True
      _ -> False
    }
  })

  process.send_exit(started.pid)
}

pub fn persistence_output_and_error_test() {
  use conn <- with_test_db

  let q_ok = m25.Queue(..default_test_queue(), name: "int-output-" <> uuid())

  let q_err =
    m25.Queue(
      ..default_test_queue(),
      name: "int-error-" <> uuid(),
      handler_function: fn(_) { Error("boom") },
    )

  let assert Ok(app) =
    m25.new(conn) |> m25.add_queue(q_ok) |> result.try(m25.add_queue(_, q_err))

  let assert Ok(job_1) = m25.enqueue(conn, q_ok, m25.new_job("A"))
  let ok_id = uuid.to_string(job_1.id.value)

  let job2 = m25.new_job("B") |> m25.retry(1, option.None)
  let assert Ok(job_2) = m25.enqueue(conn, q_err, job2)
  let err_original_id = uuid.to_string(job_2.id.value)

  let assert Ok(started) = m25.start(app, 5000)

  wait_until("ok job succeeds", 5000, fn() {
    case job_status(conn, ok_id) {
      Ok("succeeded") -> True
      _ -> False
    }
  })

  let assert Ok(option.Some("\"\"")) = job_output_text(conn, ok_id)

  wait_until("error job fails", 5000, fn() {
    case last_failure_reason(conn, err_original_id) {
      Ok(option.Some("error")) -> True
      _ -> False
    }
  })

  let assert Ok(option.Some(error_text)) =
    last_error_data_text(conn, err_original_id)
  assert error_text == "\"boom\""

  process.send_exit(started.pid)
}

pub fn reserved_timeout_recovery_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-reserved-cleanup-" <> uuid(),
      reserved_timeout: 100,
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("X"))
  let job_id = uuid.to_string(job.id.value)

  // Simulate a stuck reservation from an old process
  let assert Ok(_) =
    pog.query(
      "update m25.job set reserved_at = now() - interval '5 seconds' where id = $1",
    )
    |> pog.parameter(pog.text(job_id))
    |> pog.execute(conn)

  let assert Ok(started) = m25.start(app, 5000)

  // The cleanup step should revert to pending and the job should complete
  wait_until("reserved job is processed", 10_000, fn() {
    case job_status(conn, job_id) {
      Ok("succeeded") -> True
      _ -> False
    }
  })

  process.send_exit(started.pid)
}

pub fn add_queue_duplicate_name_test() {
  use conn <- with_test_db

  let queue = m25.Queue(..default_test_queue(), name: "int-dupq-" <> uuid())

  let m = m25.new(conn)
  let assert Ok(m) = m25.add_queue(m, queue)
  let assert Error(_) = m25.add_queue(m, queue)
}

pub fn decode_failure_insert_test() {
  use conn <- with_test_db

  let test_process = process.self()

  // Intentionally mismatch encoder/decoder: encode as string but decode as int
  let bad_queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-decode-fail-" <> uuid(),
      input_decoder: decode.int,
      input_to_json: fn(_) { json.string("not-an-int") },
      handler_function: fn(_) {
        // This shouldn't run
        process.kill(test_process)
        Error("unreachable")
      },
    )

  let assert Error(m25.JobRecordFetchDecodeErrors(_)) =
    m25.enqueue(conn, bad_queue, m25.new_job(123))
}

pub fn get_job_test() {
  use conn <- with_test_db

  // Intentionally mismatch encoder/decoder: encode as string but decode as int
  let queue = m25.Queue(..default_test_queue(), name: "int-get-job-" <> uuid())

  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("Hello, Joe!"))

  let assert Ok(m25.JobRecord(id: job_id, ..)) =
    m25.get_job(conn, queue, job.id)
  assert job_id == job.id
}

pub fn cancel_job_pending_succeeds_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(..default_test_queue(), name: "int-cancel-ok-" <> uuid())

  // Enqueue but do not start workers so it stays pending
  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("X"))
  let id_str = uuid.to_string(job.id.value)

  // Cancel should succeed and set status to cancelled
  let assert Ok(_) = m25.cancel_job(conn, queue, job.id)

  let assert Ok("cancelled") = job_status(conn, id_str)
}

pub fn cancel_job_non_pending_fails_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(..default_test_queue(), name: "int-cancel-fail-" <> uuid())

  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("Y"))
  let id_str = uuid.to_string(job.id.value)

  // Move job to a non-pending state (reserved)
  let assert Ok(_) =
    pog.query("update m25.job set reserved_at = now() where id = $1")
    |> pog.parameter(pog.text(id_str))
    |> pog.execute(conn)

  // Cancel should fail with InvalidState(Reserved)
  let assert Error(m25.InvalidState(m25.Reserved)) =
    m25.cancel_job(conn, queue, job.id)
}

pub fn cancelled_job_is_not_executed_test() {
  use conn <- with_test_db

  let queue =
    m25.Queue(
      ..default_test_queue(),
      name: "int-cancel-no-exec-" <> uuid(),
      handler_function: fn(_) {
        // If this runs, something went wrong; but we also assert status later
        Ok("done")
      },
    )

  let assert Ok(app) = m25.new(conn) |> m25.add_queue(queue)
  let assert Ok(job) = m25.enqueue(conn, queue, m25.new_job("Z"))
  let id_str = uuid.to_string(job.id.value)

  // Cancel while pending
  let assert Ok(_) = m25.cancel_job(conn, queue, job.id)
  let assert Ok("cancelled") = job_status(conn, id_str)

  // Start workers; the cancelled job must not be picked up/executed
  let assert Ok(started) = m25.start(app, 5000)
  process.sleep(600)

  // Ensure it remains cancelled after workers have been running
  let assert Ok("cancelled") = job_status(conn, id_str)

  process.send_exit(started.pid)
}

fn uuid() -> String {
  uuid.v4()
  |> uuid.to_string
}
