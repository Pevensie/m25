update m25.job
set status = $2
where id = any($1)
