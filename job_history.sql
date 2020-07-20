use	msdb
go
if	OBJECT_ID('#tmp') is not null
	drop	table #tmp

SELECT	sj.name, 
	sja.run_requested_date, 
	CONVERT(VARCHAR(12), sja.stop_execution_date-sja.start_execution_date, 114) Duration
into	#tmp
FROM	msdb.dbo.sysjobactivity sja
	INNER	JOIN msdb.dbo.sysjobs sj ON sja.job_id = sj.job_id
	inner join sysjobschedules s on s.job_id = sja.job_id
WHERE	sja.run_requested_date IS NOT NULL
	and sj.enabled = 1
	and s.next_run_date != 0
ORDER	BY sja.run_requested_date desc

select	m.*
from	(
	select	mp.name, max(mp.run_requested_date) as run_requested_date
	from	#tmp mp
	group	by mp.name
	) g inner join #tmp m on m.name = g.name 
		and m.run_requested_date = g.run_requested_date
order	by 3 desc