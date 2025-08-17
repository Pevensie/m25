update m25.job
set
    started_at = null,
    deadline = null
where id = any($1);
