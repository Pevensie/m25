# M25

[![Package Version](https://img.shields.io/hexpm/v/m25)](https://hex.pm/packages/m25)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/m25/)

A background job queueing and scheduling library for Gleam!

## Getting Started

Install M25:

```sh
gleam add m25
```

Create a `pog` connection. You should generally start both `pog` and `m25` in a
supervised manner.

```gleam
import m25

pub fn main() -> Nil {
  let conn_name = process.new_name("db_connection")

  let conn_child =
    pog.default_config(conn_name)
    |> pog.host("localhost")
    |> pog.database("my_database")
    |> pog.pool_size(15)
    |> pog.supervised

  // Create a connection that can be accessed by our queue handlers
  let conn = pog.named_connection(conn_name)

  let assert Ok(m25) =
    m25.new(conn)
    |> m25.add_queue(email_queue())
    |> result.try(m25.add_queue(_, sms_queue()))

  let m25_child = m25.supervised(m25, 1000)

  let assert Ok(started) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(conn_child)
    |> static_supervisor.add(m25_child)
    |> static_supervisor.start

  // Your queues will now be set up and jobs will start executing
  // in the background!
}
```

You can then enqueue jobs using the [`m25.enqueue`](https://hexdocs.pm/m25/m25.html#enqueue)
function.

```gleam
let job =
  m25.new_job(job_input)
  |> m25.schedule(at: future_time)
  |> m25.retry(max_attempts: 3, delay: option.Some(duration.seconds(3)))

let assert Ok(_) = m25.enqueue(conn, my_queue, job)
```

Further documentation can be found at <https://hexdocs.pm/m25>. You will also find a
runnable example in [`dev/m25_dev.gleam`](./dev/m25_dev.gleam). This will run against
the Postgres database configured in the [`compose.yml`](./compose.yml) file.

## Delivery Semantics

M25 aims to deliver jobs at least once. If a job fails, either by returning an error,
crashing, timing out, or failing to respond to heartbeats, M25 will mark the job as
failed and retry according to the configured retry policy.

Jobs that are stuck in an executing state will be cleaned up after a configurable
timeout, and can be retried.

Queue handlers receive a `JobContext` containing the job ID, queue name,
attempt, maximum attempts, configured retry delay, and unique key:

```gleam
handler_function: fn(context, input) {
  io.debug(#(m25.job_id_to_string(context.id), context.attempt))
  process(input)
}
```

When a handler returns `Error`, the queue's `retry_policy` can select a dynamic
delay or stop the chain:

```gleam
retry_policy: fn(context, failure) {
  case failure {
    m25.HandlerError(PermanentFailure) -> m25.Terminal
    _ -> m25.Retry(after: exponential_delay(context.attempt))
  }
}
```

Handlers have the same context, so applications can record the expected
retry/terminal decision before returning the error. M25 persists the error and
then applies the queue policy. `Terminal` leaves the failed attempt without a
child; reaching `max_attempts` also remains terminal.

Worker crashes and heartbeat timeouts cannot return through the handler. M25
converts them to `ProcessFailure(Crash)` and
`ProcessFailure(HeartbeatTimeout)` respectively, then applies the same queue
policy. This allows either lifecycle failure to stop without a child attempt or
to use a dynamically selected delay. Previously heartbeat timeouts were marked
failed without entering the retry transition.

Job deadlines can be observed by the executor heartbeat or by queue polling.
Failure finalization updates only an executing row, making those paths
idempotent. When polling wins, M25 removes the timed-out executor from its
in-memory running set before reserving replacement work, so the expired job
cannot leak a concurrency slot. Deadline failures use
`ProcessFailure(JobTimeout)` and the queue retry policy.

## Tables

This driver creates tables in the `m25` schema to store jobs.

The current tables created by this driver are:

| Table Name | Description                                                             |
| ---------- | ----------------------------------------------------------------------- |
| `job`      | Stores jobs, both executing and executed.                               |
| `version`  | Stores the latest version of the M25 migrations that have been applied. |

## Migrations

You can run migrations against your
database using the provided CLI:

```sh
gleam run -m m25 migrate --addr=<connection_string>
```

The required SQL statements will be printed to the console for use with other migration
tools like [`dbmate`](https://github.com/amacneil/dbmate). Alternatively, you can
apply the migrations directly using the `--apply` flag.

Support for [Cigogne](https://github.com/Billuc/cigogne) is in the works.

## Development

```sh
docker compose up -d  # Start the local Postgres instance
gleam dev             # Run the example
gleam test            # Run the tests
```

## Why 'M25'?

This is the M25.

![Hundreds of cars queueing on the M25 motorway in England](https://i2-prod.mylondon.news/incoming/article28007160.ece/ALTERNATES/s1200/1_m25-traffic.jpg)
