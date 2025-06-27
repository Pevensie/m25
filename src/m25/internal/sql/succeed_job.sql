update m25.job
set
    status = 'succeeded',
    output = $2,
    finished_at = now()
where id = $1;
