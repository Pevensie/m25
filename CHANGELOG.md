# Changelog

## Unreleased

- Pass per-attempt `JobContext` metadata to queue handlers.
- Add queue-defined dynamic retry delays and terminal decisions for handler
  errors.
- Apply queue retry policies to worker crashes and heartbeat timeouts.
- Retry heartbeat timeouts instead of leaving the first failed attempt without
  a retry transition.
- Apply queue retry policies to job deadline timeouts.
- Finalize concurrent deadline detection idempotently and release timed-out
  in-memory concurrency slots.

## 1.0.0 - 2025-08-28

- Initial release of M25!
