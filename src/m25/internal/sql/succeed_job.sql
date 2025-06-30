update m25.job
set
    output = $2,
    finished_at = now()
where id = $1;
