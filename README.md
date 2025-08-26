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
