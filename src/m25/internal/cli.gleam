import argv
import cigogne
import cigogne/config
import gleam/bool
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/option
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/timestamp
import glint
import pog
import snag

fn add_error_context(error: String, context: String) {
  context <> ": " <> error
}

fn print(message: String, silent: Bool) {
  case silent {
    False -> io.print_error(message)
    True -> Nil
  }
}

fn println(message: String, silent: Bool) {
  print(message <> "\n", silent)
}

fn check_m25_schema_exists(tx: pog.Connection) -> Result(Bool, String) {
  let query_result =
    pog.query(
      "select schema_name from information_schema.schemata where schema_name = 'm25'",
    )
    |> pog.returning({
      use value <- decode.field(0, decode.string)
      decode.success(value)
    })
    |> pog.execute(tx)
    |> result.map_error(string.inspect)

  use response <- result.try(query_result)
  case response.rows {
    [_] -> Ok(True)
    [] -> Ok(False)
    _ -> Error("Too many rows")
  }
}

fn get_current_version(
  tx: pog.Connection,
) -> Result(option.Option(timestamp.Timestamp), String) {
  let query_result =
    pog.query(
      "select version::timestamp from m25.version order by version desc limit 1",
    )
    |> pog.returning({
      use timestamp <- decode.field(0, pog.timestamp_decoder())
      decode.success(timestamp)
    })
    |> pog.execute(tx)
    |> result.map_error(string.inspect)

  use response <- result.try(query_result)
  case response.rows {
    [] -> Ok(option.None)
    [version] -> Ok(option.Some(version))
    _ -> Error("Too many rows")
  }
}

fn get_migrations_to_apply(
  current_version: option.Option(timestamp.Timestamp),
) -> Result(option.Option(String), String) {
  use config <- result.try(
    config.get("m25")
    |> result.map_error(fn(err) {
      err
      |> string.inspect
      |> add_error_context("Failed to get migrations configuration")
    }),
  )

  use engine <- result.try(
    cigogne.create_engine(config)
    |> result.map_error(fn(err) {
      err
      |> string.inspect
      |> add_error_context("Failed to create migrations engine")
    }),
  )

  let migrations = cigogne.get_all_migrations(engine)

  let migrations_to_apply = case current_version {
    option.None -> migrations
    option.Some(current_version) -> {
      migrations
      |> list.filter(fn(migration) {
        timestamp.compare(migration.timestamp, current_version) == order.Gt
      })
    }
  }

  // Check if there are files to apply
  use <- bool.guard(list.is_empty(migrations_to_apply), Ok(option.None))

  let migration_sql =
    migrations_to_apply
    |> list.flat_map(fn(m) { m.queries_up })
    |> string.join("\n")

  let assert Ok(latest_migration) = list.last(migrations_to_apply)
  let new_version = latest_migration.timestamp

  let new_version_sql = "insert into m25.version (version)
values (timestamptz '" <> {
      new_version |> timestamp.to_rfc3339(calendar.utc_offset)
    } <> "');\n"

  Ok(option.Some(migration_sql <> "\n" <> new_version_sql))
}

fn apply_migrations(
  tx: pog.Connection,
  migration_sql: String,
  silent: Bool,
) -> Result(Nil, String) {
  print("Applying migrations for m25... ", silent)

  use last_char <- result.try(
    string.last(migration_sql |> string.trim_end)
    |> result.replace_error("Empty migration file"),
  )

  let sql = case last_char {
    ";" -> migration_sql
    _ -> migration_sql <> ";"
  }
  // pog doesn't allow executing multiple statements, so
  // wrap in a do block
  let sql = "do $m25migration$ begin " <> sql <> " end;$m25migration$"

  let query_result =
    pog.query(sql)
    |> pog.execute(tx)
    |> result.map_error(string.inspect)

  use _ <- result.try(query_result)
  println("ok.", silent)
  Ok(Nil)
}

fn handle_migration(
  tx: pog.Connection,
  apply: Bool,
  silent: Bool,
) -> Result(Nil, String) {
  print("Checking schema... ", silent)
  use schema_exists <- result.try(
    check_m25_schema_exists(tx)
    |> result.map_error(add_error_context(
      _,
      "Unable to check if 'm25' schema exists",
    )),
  )
  println("ok.", silent)

  print("Checking current version of m25 tables... ", silent)
  let version_result = case schema_exists {
    False -> Ok(option.None)
    True ->
      get_current_version(tx)
      |> result.map_error(add_error_context(
        _,
        "Unable to check m25 table version",
      ))
  }

  use current_version <- result.try(version_result)
  println(
    "ok. Current version: "
      <> current_version
    |> option.map(timestamp.to_rfc3339(_, calendar.utc_offset))
    |> option.unwrap("none"),
    silent,
  )

  print("Getting migrations for m25 tables... ", silent)
  use migration_sql <- result.try(
    get_migrations_to_apply(current_version)
    |> result.map_error(add_error_context(
      _,
      "Failed to get migrations to apply for m25 tables",
    )),
  )
  println("ok.", silent)

  case migration_sql {
    option.None -> {
      println("No migrations to apply for m25 tables\n", silent)
      Ok(Nil)
    }
    option.Some(migration_sql) -> {
      case apply {
        True ->
          apply_migrations(tx, migration_sql, silent)
          |> result.map_error(add_error_context(
            _,
            "Failed to apply migrations for m25 tables",
          ))
        False -> {
          io.println(migration_sql)
          Ok(Nil)
        }
      }
    }
  }
}

pub fn migrate_for_tests(tx: pog.Connection) -> Result(Nil, String) {
  handle_migration(tx, True, True)
}

fn connection_string_flag() -> glint.Flag(String) {
  glint.string_flag("addr")
  |> glint.flag_help("The connection string for your Postgres database")
  |> glint.flag_default(
    "postgresql://postgres:postgres@localhost:5432/postgres",
  )
}

fn apply_flag() -> glint.Flag(Bool) {
  glint.bool_flag("apply")
  |> glint.flag_help("Apply migrations instead of just printing them")
  |> glint.flag_default(False)
}

fn silent_flag() -> glint.Flag(Bool) {
  glint.bool_flag("silent")
  |> glint.flag_help("Don't print messages to stderr")
  |> glint.flag_default(False)
}

fn migrate_command() {
  use <- glint.command_help("Migrate M25 modules in your Postgres database")
  use connection_string_arg <- glint.flag(connection_string_flag())
  use apply_flag <- glint.flag(apply_flag())
  use silent_flag <- glint.flag(silent_flag())

  use _, _, flags <- glint.command()

  use connection_string <- result.try(connection_string_arg(flags))
  use apply <- result.try(apply_flag(flags))
  use silent <- result.try(silent_flag(flags))

  let pool_name = process.new_name("m25_migrations")

  use config <- result.try(
    pog.url_config(pool_name, connection_string)
    |> result.replace_error(
      snag.Snag(issue: "Invalid Postgres connection string", cause: [
        "invalid connection string",
      ]),
    ),
  )
  let assert Ok(actor.Started(_, conn)) = pog.start(config)
  let transaction_result =
    pog.transaction(conn, fn(tx) {
      use _ <- result.try(handle_migration(tx, apply, silent))

      case apply {
        True -> println("\nSuccess! Applied migrations for m25", silent)
        False -> Nil
      }
      Ok(Nil)
    })
  case transaction_result {
    Ok(_) -> Ok(Nil)
    Error(pog.TransactionRolledBack(msg)) ->
      Error(snag.Snag(issue: msg, cause: ["transaction rolled back"]))
    Error(pog.TransactionQueryError(err)) ->
      Error(
        snag.Snag(issue: "Query error: " <> string.inspect(err), cause: [
          "query error",
        ]),
      )
  }
}

pub fn run_cli() {
  let cli =
    glint.new()
    |> glint.with_name("m25")
    |> glint.as_module
    |> glint.pretty_help(glint.default_pretty_help())
    |> glint.add(at: ["migrate"], do: migrate_command())

  use cli_result <- glint.run_and_handle(cli, argv.load().arguments)
  use errors <- result.map_error(cli_result)
  println("Command failed with an error: " <> errors.issue, False)
}
