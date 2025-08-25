update m25.job
set
    reserved_at = null
where status = 'reserved'
    and queue_name = $1
    and reserved_at < now() - make_interval(secs => $2)
returning id, queue_name;
