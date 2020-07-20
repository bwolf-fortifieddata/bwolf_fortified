-------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------

declare @rundate        int             = ''        ,
        @job_name       varchar(256)    = 'FD - Gather File IO Stats'

if      nullif(@rundate, '') is null
        select  @rundate = convert(char(8), dateadd(dd, -1, cast(current_timestamp as date)), 112)
        
select  @job_name = nullif(@job_name, '')
-------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------

SELECT	SJ.name 'job_name'									,
	--
	REPLACE(CONVERT(varchar,convert(datetime,convert(varchar,run_date)),102),'.','-')+' '+
	SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_time),6),1,2)+':'+
	SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_time),6),3,2)+':'+
	SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_time),6),5,2) 'start_date_time'		,
	--
	SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_duration),6),1,2)+':'+
	SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_duration),6),3,2)+':'+
	SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_duration),6),5,2) 'Duration'		,
	--
	CASE	run_status
		WHEN	1 THEN '1-SUCCESS'
		WHEN	0 THEN '0-FAILED'
		ELSE	CONVERT(varchar,run_status)
	END	AS 'Status'									,
	Step_id											,
	[Message]										,
	[Server]
into	#tmp
FROM	MSDB..SysJobHistory SJH
	RIGHT JOIN MSDB..SysJobs SJ ON SJ.Job_Id = SJH.job_id
where	run_date > @rundate
        and (sj.name = @job_name or @job_name is null)
	--AND Step_ID = 0 --Comments this line if you want to see the status of each step of the job
ORDER	BY run_date DESC, run_time DESC, step_ID DESC
-------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------
        select	*
        from	(
	        select	[Server], [job_name], [start_date_time], Duration, [Status], [message],
		        ROW_NUMBER() over(partition by [job_name] order by [start_date_time] desc) as rn
	        from	#tmp
	        where	[step_id] != 0
	        ) a
        --where	a.rn = 1
-------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------
