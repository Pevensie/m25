import argv
import filepath
import gleam/bool
import gleam/dynamic/decode
import gleam/erlang/application
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
import simplifile
import snag
import tempo
import tempo/datetime

fn add_error_context(error: String, context: String) {
  context <> ": " <> error
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

fn get_migrations() -> Result(List(String), String) {
  use priv <- result.try(
    application.priv_directory("m25")
    |> result.replace_error("Couldn't get priv directory"),
  )

  let assert Ok(directory) =
    [priv, "migrations"]
    |> list.reduce(filepath.join)

  simplifile.get_files(directory)
  |> result.map_error(fn(err) {
    err
    |> string.inspect
    |> add_error_context("Unable to get files in priv directory")
  })
}

fn version_from_filename(filename: String) -> timestamp.Timestamp {
  let assert Ok(filename) = filename |> filepath.split |> list.last
  let assert Ok(version_string) = filename |> string.split("_") |> list.first
  // gtempo requires an offset to be specified, so add one manually
  let assert Ok(version) =
    datetime.parse(version_string <> "Z", tempo.Custom("YYYYMMDDHHmmssZ"))
  datetime.to_timestamp(version)
}

fn get_migrations_to_apply(
  current_version: option.Option(timestamp.Timestamp),
) -> Result(option.Option(String), String) {
  use files <- result.try(
    get_migrations()
    |> result.map_error(add_error_context(
      _,
      "Failed to get migrations for m25 tables",
    )),
  )

  let files_to_apply = case current_version {
    option.None -> files
    option.Some(current_version) -> {
      files
      |> list.filter(fn(file) {
        timestamp.compare(version_from_filename(file), current_version)
        == order.Gt
      })
    }
  }

  // Check if there are files to apply
  use <- bool.guard(
    case files_to_apply {
      [] -> True
      _ -> False
    },
    Ok(option.None),
  )
  use migration_sql <- result.try(
    files_to_apply
    |> list.try_map(simplifile.read)
    |> result.map(string.join(_, "\n"))
    |> result.map_error(fn(err) {
      add_error_context(string.inspect(err), "Failed to read migrations")
    }),
  )

  let assert Ok(latest_migration) = list.last(files_to_apply)
  let new_version = version_from_filename(latest_migration)

  let new_version_sql = "insert into m25.version (version)
values (timestamptz '" <> {
      new_version |> timestamp.to_rfc3339(calendar.utc_offset)
    } <> "');\n"

  Ok(option.Some(migration_sql <> "\n" <> new_version_sql))
}

fn apply_migrations(
  tx: pog.Connection,
  migration_sql: String,
) -> Result(Nil, String) {
  io.print_error("Applying migrations for m25... ")

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
  io.println_error("ok.")
  Ok(Nil)
}

fn handle_migration(tx: pog.Connection, apply: Bool) -> Result(Nil, String) {
  io.print_error("Checking schema... ")
  use schema_exists <- result.try(
    check_m25_schema_exists(tx)
    |> result.map_error(add_error_context(
      _,
      "Unable to check if 'm25' schema exists",
    )),
  )
  io.println_error("ok.")

  io.print_error("Checking current version of m25 tables... ")
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
  io.println_error(
    "ok. Current version: "
    <> current_version
    |> option.map(timestamp.to_rfc3339(_, calendar.utc_offset))
    |> option.unwrap("none"),
  )

  io.print_error("Getting migrations for m25 tables... ")
  use migration_sql <- result.try(
    get_migrations_to_apply(current_version)
    |> result.map_error(add_error_context(
      _,
      "Failed to get migrations to apply for m25 tables",
    )),
  )
  io.println_error("ok.")

  case migration_sql {
    option.None -> {
      io.println_error("No migrations to apply for m25 tables\n")
      Ok(Nil)
    }
    option.Some(migration_sql) -> {
      case apply {
        True ->
          apply_migrations(tx, migration_sql)
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

fn migrate_command() {
  use <- glint.command_help("Migrate M25 modules in your Postgres database")
  use connection_string_arg <- glint.flag(connection_string_flag())
  use apply_flag <- glint.flag(apply_flag())

  use _, _, flags <- glint.command()

  use connection_string <- result.try(connection_string_arg(flags))
  use apply <- result.try(apply_flag(flags))

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
      use _ <- result.try(handle_migration(tx, apply))

      case apply {
        True -> io.println_error("\nSuccess! Applied migrations for m25")
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
  io.println_error("Command failed with an error: " <> errors.issue)
}
