update m25.job
set
    reserved_at = null
where status = 'reserved'
    and reserved_at < now() - make_interval(secs => $1)
returning id, queue_name;
