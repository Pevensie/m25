-- Promote successful reservations to executing, revert failures to pending
with successful_jobs as (
    update m25.job
    set
        started_at = now(),
        deadline = now() + timeout
    where id = any($1)
        and status = 'reserved'
    returning id
),
failed_jobs as (
    update m25.job
    set
        reserved_at = null
    where id = any($2)
        and status = 'reserved'
    returning id
)
select
    (select count(*) from successful_jobs) as successful_count,
    (select count(*) from failed_jobs) as failed_count;
