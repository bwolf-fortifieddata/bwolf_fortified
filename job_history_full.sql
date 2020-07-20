select job_name, run_datetime, run_duration
from
(
    select job_name, run_datetime,
        SUBSTRING(run_duration, 1, 2) + ':' + SUBSTRING(run_duration, 3, 2) + ':' +
        SUBSTRING(run_duration, 5, 2) AS run_duration
    from
    (
        select DISTINCT
            j.name as job_name, 
            run_datetime = CONVERT(DATETIME, RTRIM(run_date)) +  
                (run_time * 9 + run_time % 10000 * 6 + run_time % 100 * 10) / 216e4,
            run_duration = RIGHT('000000' + CONVERT(varchar(6), run_duration), 6)
        from msdb..sysjobhistory h
        inner join msdb..sysjobs j
        on h.job_id = j.job_id
    ) t
) t
order by job_name, run_datetime



----------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
drop	table #tmp

select	job_name, run_datetime, run_duration
into	#tmp
from	(
	    select job_name, run_datetime,
		SUBSTRING(run_duration, 1, 2) + ':' + SUBSTRING(run_duration, 3, 2) + ':' +
		SUBSTRING(run_duration, 5, 2) AS run_duration
	    from
	    (
		select DISTINCT
		    j.name as job_name, 
		    run_datetime = CONVERT(DATETIME, RTRIM(run_date)) +  
			(run_time * 9 + run_time % 10000 * 6 + run_time % 100 * 10) / 216e4,
		    run_duration = RIGHT('000000' + CONVERT(varchar(6), run_duration), 6)
		from msdb..sysjobhistory h
		inner join msdb..sysjobs j
		on h.job_id = j.job_id
	    ) t
	) t

order	by job_name, run_datetime



select	a.job_name, avg(duration_minutes) as avg_duration_minutes, MAX(run_datetime) as run_datetime,
	DATEADD(ss, avg(duration_minutes), MAX(run_datetime)) as avg_finish_time
from	(
	select	((
		convert(float, (substring(run_duration, 1, 2)*60.00)*60.00)	+
		convert(float, (substring(run_duration, 4, 2)*60.00))		+
		convert(float, (substring(run_duration, 7, 2)))
		)/60.00) as duration_minutes
		, *
	from	#tmp
	where	isnumeric((substring(run_duration, 7, 2))) = 1
	) a
group	by a.job_name
order	by 4 desc





























select job_name, run_datetime, run_duration
from
(
    select job_name, run_datetime,
        SUBSTRING(run_duration, 1, 2) + ':' + SUBSTRING(run_duration, 3, 2) + ':' +
        SUBSTRING(run_duration, 5, 2) AS run_duration
    from
    (
        select DISTINCT
            j.name as job_name, 
            run_datetime = CONVERT(DATETIME, RTRIM(run_date)) +  
                (run_time * 9 + run_time % 10000 * 6 + run_time % 100 * 10) / 216e4,
            run_duration = RIGHT('000000' + CONVERT(varchar(6), run_duration), 6)
        from msdb..sysjobhistory h
        inner join msdb..sysjobs j
        on h.job_id = j.job_id
    ) t
) t
order by job_name, run_datetime



----------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
drop	table #tmp

select	job_name, run_datetime, run_duration
into	#tmp
from	(
	    select job_name, run_datetime,
		SUBSTRING(run_duration, 1, 2) + ':' + SUBSTRING(run_duration, 3, 2) + ':' +
		SUBSTRING(run_duration, 5, 2) AS run_duration
	    from
	    (
		select DISTINCT
		    j.name as job_name, 
		    run_datetime = CONVERT(DATETIME, RTRIM(run_date)) +  
			(run_time * 9 + run_time % 10000 * 6 + run_time % 100 * 10) / 216e4,
		    run_duration = RIGHT('000000' + CONVERT(varchar(6), run_duration), 6)
		from msdb..sysjobhistory h
		inner join msdb..sysjobs j
		on h.job_id = j.job_id
	    ) t
	) t

order	by job_name, run_datetime


drop    table #tmp2


select  *, 
        case    datepart(weekday, run_datetime)
                when    1 then 'Sunday'
                when    2 then 'Monday'
                when    3 then 'Tuesday'
                when    4 then 'Wednesday'
                when    5 then 'Thursday'
                when    6 then 'Friday'
                when    7 then 'Saturday'
        end     as day_of_week,
        datepart(week, run_datetime) as week_number,
        DATEADD(DAY, 2 - DATEPART(WEEKDAY, run_datetime), CAST(run_datetime AS DATE)) Week_Start_Date,
        ((
	convert(int, (substring(run_duration, 1, 2)*60)*60)	+
	convert(int, (substring(run_duration, 4, 2)*60))		+
	convert(int, (substring(run_duration, 7, 2)))
	)/60) as duration_minutes
into    #tmp2
from    #tmp
where   job_name = 'TMPHCSQLDE - 02 Archiving'
order   by 3 desc


select  min(run_datetime) as min_run_datetime, max(run_datetime) as max_run_datetime
from    #tmp2

select  day_of_week, avg(duration_minutes) as avg_duration_minutes_day_of_week
from    #tmp2
group   by day_of_week
order   by 1 desc

select  week_number, week_start_date, avg(duration_minutes) as avg_duration_minutes_week_number
from    #tmp2
group   by week_number, week_start_date
order   by 3 desc


select  *
from    #tmp2
order   by 3 desc


select	a.job_name, avg(duration_minutes) as avg_duration_minutes, MAX(run_datetime) as run_datetime,
	DATEADD(ss, avg(duration_minutes), MAX(run_datetime)) as avg_finish_time
from	(
	select	((
		convert(float, (substring(run_duration, 1, 2)*60.00)*60.00)	+
		convert(float, (substring(run_duration, 4, 2)*60.00))		+
		convert(float, (substring(run_duration, 7, 2)))
		)/60.00) as duration_minutes
		, *
	from	#tmp
	where	isnumeric((substring(run_duration, 7, 2))) = 1
	) a
where   a.job_name = 'TMPHCSQLDE - 02 Archiving'
group	by a.job_name
order	by 4 desc