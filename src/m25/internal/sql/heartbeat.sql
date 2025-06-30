update m25.job
set
    latest_heartbeat_at = now()
where id = $1
returning
    (
        -- This is the only way to access the old value of the row
        select
            now() - coalesce(latest_heartbeat_at, now()) > $2::int * make_interval(secs => $3) as heartbeat_timed_out
        from m25.job
        where id = $1
    ) as heartbeat_timed_out,
    now() > deadline as deadline_passed;
