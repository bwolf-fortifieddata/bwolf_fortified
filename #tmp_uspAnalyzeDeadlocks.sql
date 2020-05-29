drop    table if exists ##tmp_results
go
drop    table if exists #tmp_Results_o
go

/*************************************************************************************************
**  purpose:
**	report on deadlocks
**  author:
**      William J. Wolf
**
**  date created:
**      2014-09-11
**
**  parameter description:
**      @startDate
**	@endDate
**	@reportOption
**	@obfuscate

**  date modified/reason:
**
**  Notes:
DISCLAIMER:  This is written by William Wolf(@sqlwarewolf).  William is not responsible for any
adverse affects this may cause.  Always test thoroughly on a development system before implementing in production

1. Something to analyze the deadlocks (past hour/day/week/month/etc) and produce two lists. 
Independent of each other. It’s just which object is involved in a deadlock and wins/loses the most. 


One list of the deadlock winner by order of most frequent to least frequent. (which objects win in a deadlock – they get to keep running) 
One list of the deadlock loser by order of most frequent to least frequent. (which objects loses out to a deadlock the most) 

2. Something to analyze the deadlocks (past hour/day/week/month/etc) and produce one list. 
This list shows the object combinations that are involved in deadlocks – what two objects collide the most often – by frequency. 

exec	#tmp_uspAnalyzeDeadlocks @reportOption = 2, @startDate = '12/01/2019', @endDate = '01/08/2020'
exec	uspAnalyzeDeadlocks @reportOption = 2
exec	uspAnalyzeDeadlocks @reportOption = 1

*************************************************************************************************/
alter   procedure #tmp_uspAnalyzeDeadlocks
	@startDate		datetime	= null		,
	@endDate		datetime	= null		,
	@reportOption		tinyint		= 1		,
	@obfuscate		bit		= 1		,
	@drmID			int		= null		,
	@objectName		varchar(256)	= null		,
	@includeLineNumbers	bit		= 0
as
/*******************************************************************************
**
*******************************************************************************/
	set	nocount on
        set     transaction isolation level read uncommitted


 --       declare
	--@startDate		datetime	= null		,
	--@endDate		datetime	= null		,
	--@reportOption		tinyint		= 2		,
	--@obfuscate		bit		= 1		,
	--@drmID			int		= null		,
	--@objectName		varchar(256)	= null		,
	--@includeLineNumbers	bit		= 0


	if	object_id('tempdb..#tmpDRM') is not null
		drop	table #tmpDRM
	
	create	 table #tmpDRM
		(
		drmID		int	,
		event_data	xml
		)
		
	create	clustered index #ix#tmpDRM on #tmpDRM(drmID)
	
	if	object_id('tempdb..#tmpDRD') is not null
		drop	table #tmpDRD
	
	CREATE	TABLE #tmpDRD
		(
		drmID		int		,
		PagelockObject	varchar(200)	,
		DeadlockObject	varchar(200)	,
		Victim		int		,
		[Procedure]	varchar(200)	,
		LockMode	varchar(10)	,
		Code		varchar(1000)	,
		ClientApp	varchar(100)	,
		HostName	varchar(20)	,
		LoginName	varchar(20)	,
		TransactionTime	datetime	,
		InputBuffer	varchar(max)	,
		event_data	xml		,
		obj_name	varchar(1024)	,
		lines		varchar(1024)	,
		processID	varchar(50)	,
		dbName		varchar(256)    ,
                sqlhandle       varchar(1000)
		)
		
	create	clustered index #ix#tmpDRD on #tmpDRD(drmID)
	
	
	if	object_id('tempdb..#tmpDRD_obfuscate') is not null
		drop	table #tmpDRD_obfuscate
	
	CREATE	TABLE #tmpDRD_obfuscate
		(
		drmID		int		,
		PagelockObject	varchar(200)	,
		DeadlockObject	varchar(200)	,
		Victim		int		,
		[Procedure]	varchar(200)	,
		LockMode	varchar(10)	,
		Code		varchar(1000)	,
		ClientApp	varchar(100)	,
		HostName	varchar(20)	,
		LoginName	varchar(20)	,
		TransactionTime	datetime	,
		InputBuffer	varchar(max)	,
		event_data	xml		,
		obj_name	varchar(1024)	,
		lines		varchar(1024)	,
		processID	varchar(50)	,
		dbName		varchar(256)    ,
                sqlhandle       varchar(1000)
		)
		
	create	clustered index #ix#tmpDRD_obfuscate on #tmpDRD_obfuscate(drmID)
	
	if	object_id('tempdb..#tmpDRD_Rep') is not null
		drop	table #tmpDRD_Rep
			
	CREATE	TABLE #tmpDRD_Rep
		(
		drmID			int				,
		nv_PagelockObject	varchar(200)			,
		nv_DeadlockObject	varchar(200)			,
		nv_Victim		int				,
		nv_Procedure		varchar(200)			,
		nv_LockMode		varchar(10)			,
		nv_Code			varchar(1000)			,
		nv_ClientApp		varchar(100)			,
		nv_HostName		varchar(20)			,
		nv_LoginName		varchar(20)			,
		nv_TransactionTime	datetime			,
		nv_InputBuffer		varchar(max)			,
		nv_obj_name		varchar(1024)			,
		nv_lines		varchar(1024)			,
		cs_nv_InputBuffer	as checksum(nv_InputBuffer)	,
		v_PagelockObject	varchar(200)			,
		v_DeadlockObject	varchar(200)			,
		v_Victim		int				,
		v_Procedure		varchar(200)			,
		v_LockMode		varchar(10)			,
		v_Code			varchar(1000)			,
		v_ClientApp		varchar(100)			,
		v_HostName		varchar(20)			,
		v_LoginName		varchar(20)			,
		v_TransactionTime	datetime			,
		v_InputBuffer		varchar(max)			,
		v_obj_name		varchar(1024)			,
		v_lines			varchar(1024)			,
		cs_v_InputBuffer	as checksum(nv_InputBuffer)	,
		dbName			varchar(256)                    ,
                v_sqlhandle             varchar(1000)                   ,
                nv_sqlhandle            varchar(1000)
		)
		
	create	clustered index #ix#tmpDRD_Rep on #tmpDRD_Rep(nv_DeadlockObject, v_DeadlockObject)
	create	index #ix#tmpDRD_Rep002 on #tmpDRD_Rep(cs_nv_InputBuffer, cs_v_InputBuffer)
	
	
	if	object_id('tempdb..#tmpRes') is not null
		drop	table #tmpRes

	create	table #tmpRes
		(
		drmID		int		,
		processID	varchar(50)	,
		origInputBuffer	varchar(max)	,
		inputbuffer	varchar(max)
		)
		
		
	if	object_id('tempdb..#tmpRes2') is not null
		drop	table #tmpRes2

	create	table #tmpRes2
		(
		drmID		int		,
		processID	varchar(50)	,
		origInputBuffer	varchar(max)	,
		inputbuffer	varchar(max)
		)
	
/*******************************************************************************
**
*******************************************************************************/
	insert	into #tmpDRM(drmID, event_data)
	select	drmID, event_data
	from	##tmp_Deadlock_Report_Main m
	where	exists(select 1 from ##tmp_Deadlock_Report_Detail d)

	--where	drmDate between @startDate and @endDate

	
	insert	into #tmpDRD
		(
		drmID		,
		PagelockObject	,
		DeadlockObject	,
		Victim		,
		[Procedure]	,
		LockMode	,
		Code		,
		ClientApp	,
		HostName	,
		LoginName	,
		TransactionTime	,
		InputBuffer	,
		event_data	,
		obj_name	,
		lines		,
		processID	,
		dbName          ,
                sqlhandle
		)
	select	distinct
		d.drmID				,
		d.PagelockObject		,
		d.DeadlockObject		,
		d.Victim			,
		d.[Procedure]			,
		d.LockMode			,
		d.Code				,
		d.ClientApp			,
		d.HostName			,
		d.LoginName			,
		d.TransactionTime		,
		ltrim(rtrim(
		replace(replace(replace(replace(d.InputBuffer, char(13) + char(10), ' '), char(10), ' '), char(13), ' '), '	', ' ')
		))				,
		null,--m.event_data			,
		d.obj_name			,
		case	@includeLineNumbers
			when	1 then d.lines
			else	null
		end				,
		d.processID			,
		d.dbName                        ,
                d.sqlhandle
	from	#tmpDRM m
		inner join ##tmp_Deadlock_Report_Detail d on d.drmID = m.drmID


	insert	into #tmpDRD_Rep
		(
		drmID			,	nv_PagelockObject	,
		nv_DeadlockObject	,	nv_Victim		,
		nv_Procedure		,	nv_LockMode		,
		nv_Code			,	nv_ClientApp		,
		nv_HostName		,	nv_LoginName		,
		nv_TransactionTime	,	nv_InputBuffer		,
		nv_obj_name		,	nv_lines		,
		v_PagelockObject	,	v_DeadlockObject	,
		v_Victim		,	v_Procedure		,
		v_LockMode		,	v_Code			,
		v_ClientApp		,	v_HostName		,
		v_LoginName		,	v_TransactionTime	,
		v_InputBuffer		,	v_obj_name		,
		v_lines			,	dbName                  ,
                v_sqlhandle             ,       nv_sqlhandle
		)
	select	distinct
		nv.drmID		as	drmID			,
		nv.PageLockObject	as	nv_PagelockObject	,
		nv.DeadlockObject	as	nv_DeadlockObject	,
		nv.Victim		as	nv_Victim		,
		nv.[Procedure]		as	nv_Procedure		,
		nv.LockMode		as	nv_LockMode		,
		nv.Code			as	nv_Code			,
		nv.ClientApp		as	nv_ClientApp		,
		nv.HostName		as	nv_HostName		,
		nv.LoginName		as	nv_LoginName		,
		nv.TransactionTime	as	nv_TransactionTime	,
		nv.InputBuffer		as	nv_InputBuffer		,
		nv.obj_name		as	nv_obj_name		,
		nv.lines		as	nv_lines		,
		v.PagelockObject	as	v_PagelockObject	,
		v.DeadlockObject	as	v_DeadlockObject	,
		v.Victim		as	v_Victim		,
		v.[Procedure]		as	v_Procedure		,
		v.LockMode		as	v_LockMode		,
		v.Code			as	v_Code			,
		v.ClientApp		as	v_ClientApp		,
		v.HostName		as	v_HostName		,
		v.LoginName		as	v_LoginName		,
		v.TransactionTime	as	v_TransactionTime	,
		v.InputBuffer		as	v_InputBuffer		,
		v.obj_name		as	v_obj_name		,
		v.lines			as	v_lines			,
		v.dbName		as	dbName                  ,
                v.sqlhandle                                             ,
                nv.sqlhandle
	from	#tmpDRD nv
		left join #tmpDRD v on v.drmID = nv.drmID
                        and v.Victim	= 1
	where	nv.Victim	= 0
				
				

/*******************************************************************************
** Winners/losers by object
*******************************************************************************/
set	@objectName = '%' + @objectName + '%'

        if	object_id('tempdb..#tmp_statement_text', 'U') is not null
	        drop	table #tmp_statement_text

        if	object_id('tempdb..#tmp_handle', 'U') is not null
	        drop	table #tmp_handle

        create  table #tmp_handle
                (
                sqlhandle       varbinary(64)   ,
                sqlhandle_o     varchar(max)
                )

        insert  into #tmp_handle
        select  distinct convert(varbinary(64), v_sqlhandle, 1), v_sqlhandle
        from    #tmpDRD_Rep
        where   v_sqlhandle is not null
        union
        select  distinct convert(varbinary(64), nv_sqlhandle, 1), nv_sqlhandle
        from    #tmpDRD_Rep
        where   nv_sqlhandle is not null

        SELECT	SUBSTRING(b.text, (a.statement_start_offset/2) + 1, 
		        ((CASE statement_end_offset
			        WHEN -1 THEN DATALENGTH(b.text)
			        ELSE	a.statement_end_offset
		        END -  a.statement_start_offset)/2) + 1)				AS statement_text               ,
                h.sqlhandle_o as sql_handle
        into	#tmp_statement_text
        FROM	sys.dm_exec_query_stats a  
	        CROSS APPLY sys.dm_exec_sql_text (a.sql_handle) AS b  
                inner join #tmp_handle h on h.sqlhandle = a.sql_handle
        --where   a.sql_handle in (select sqlhandle from #tmp_handle)
        option	(recompile)

if	@reportOption = 1
	BEGIN
/*******************************************************************************
** Winners/losers by query
*******************************************************************************/
	if	@objectName is null
		BEGIN
			select	isnull(nv_obj_name, nv_DeadlockObject)		as winner_object	,
				isnull(nullif(nv_Procedure, ''), nv_InputBuffer)		as 'winner procedure/query'		,
				nv_lines	as winner_lines	,
				max(nv_TransactionTime)	as LastTransactionTime	,
				count(drmID)		as Frequency		,
				dbName,
                                nv_sqlhandle as sqlhandle,
                                st.statement_text
			from	#tmpDRD_Rep r
                                left join #tmp_statement_text st on st.sql_handle = r.nv_sqlhandle
			where	nullif(isnull(nullif(nv_Procedure, ''), nv_InputBuffer), '') is not null
			group	by
				isnull(nv_obj_name, nv_DeadlockObject), isnull(nullif(nv_Procedure, ''), nv_InputBuffer), nv_lines, dbName, nv_sqlhandle, st.statement_text
			order	by 5 desc
			
			select	isnull(v_obj_name, v_DeadlockObject)		as loser_object	,
				isnull(nullif(v_Procedure, ''), v_InputBuffer)		as 'loser procedure/query'		,
				v_lines	as loser_lines,
				max(v_TransactionTime)	as LastTransactionTime	,
				count(drmID)		as Frequency		,
				dbName,
                                v_sqlhandle,
                                st.statement_text
			from	#tmpDRD_Rep r
                                left join #tmp_statement_text st on st.sql_handle = r.v_sqlhandle
			where	nullif(isnull(nullif(v_Procedure, ''), v_InputBuffer), '') is not null
			group	by
				isnull(v_obj_name, v_DeadlockObject), isnull(nullif(v_Procedure, ''), v_InputBuffer), v_lines, dbName, v_sqlhandle, st.statement_text
			order	by 5 desc
		END
	else
		BEGIN

		
			select	isnull(nv_obj_name, nv_DeadlockObject)  as winner_object	,
				isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer)		as 'winner procedure/query'		,
				nv_lines	as winner_lines	,
				max(nv_TransactionTime)	as LastTransactionTime	,
				count(drmID)		as Frequency		,
				dbName,
                                nv_sqlhandle as sqlhandle,
                                st.statement_text
			from	#tmpDRD_Rep r
                                left join #tmp_statement_text st on convert(varchar(max), st.sql_handle) = r.nv_sqlhandle
			where	nullif(isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer), '') like @objectName
				or isnull(nv_obj_name, nv_DeadlockObject) like @objectName 
			group	by
				isnull(nv_obj_name, nv_DeadlockObject), isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer), nv_lines, dbName, sqlhandle, nv_sqlhandle, st.statement_text
			order	by 5 desc
			
			select	isnull(v_obj_name, v_DeadlockObject)		as loser_object	,
				isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer)		as 'loser procedure/query'		,
				v_lines	as loser_lines,
				max(v_TransactionTime)	as LastTransactionTime	,
				count(drmID)		as Frequencey		,
				dbName,
                                v_sqlhandle,
                                st.statement_text
			from	#tmpDRD_Rep r
                                left join #tmp_statement_text st on convert(varchar(max), st.sql_handle) = r.nv_sqlhandle
			where	nullif(isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer), '') like @objectName
				or isnull(v_obj_name, v_DeadlockObject) like @objectName
			group	by
				isnull(v_obj_name, v_DeadlockObject), isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer), v_lines, dbName, v_sqlhandle, st.statement_text
			order	by 5 desc
		END
	------------------
	END
/*******************************************************************************
** both by object
*******************************************************************************/
else	if	@reportOption = 2
	BEGIN

	
	if	@objectName is null
		BEGIN
                        drop    table if exists ##tmpDRD_REP
                        drop    table if exists ##tmp_results

                        select  *
                        into    ##tmpDRD_REP
                        from	#tmpDRD_Rep

			select	distinct *
                        into    ##tmp_results
			from	(
				select	isnull(nv_obj_name, nv_DeadlockObject) as winner_object	,
					isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer)		as 'winner procedure/query'		,
					max(nv_TransactionTime)	as LastTransactionTime		,
					nv_lines	as winner_lines,
					isnull(v_obj_name, v_DeadlockObject)		as loser_object	,
					isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer)		as 'loser procedure/query'		,
					v_lines		as loser_lines,
					max(v_TransactionTime)	as loser_LastTransactionTime	,
					count(distinct drmID)		as Frequency			,
					dbName,
                                        nv_sqlhandle,
                                        v_sqlhandle,
                                        stnv.statement_text as nv_statement_text,
                                        stv.statement_text as v_statement_text,
                                        r.v_code,
                                        r.nv_code
                                
				from	#tmpDRD_Rep r
                                        left join #tmp_statement_text stnv on convert(varchar(max), stnv.sql_handle) = r.nv_sqlhandle
                                        left join #tmp_statement_text stv on convert(varchar(max), stv.sql_handle) = r.v_sqlhandle
				where	nullif(isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer), '') is not null
					--and nullif(isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer), '') is not null
				group	by
					isnull(nv_obj_name, nv_DeadlockObject),
					isnull(v_obj_name, v_DeadlockObject),
					isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer),
					isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer),
					nv_lines,
					v_lines,
					dbName,
                                        nv_sqlhandle,
                                        v_sqlhandle,
                                        stnv.statement_text,
                                        stv.statement_text,
                                        r.v_code,
                                        r.nv_code
				) a
			order	by 9 desc
		END
	else
		BEGIN
			select	distinct *
                        from	(
				select	isnull(nv_obj_name, nv_DeadlockObject) as winner_object	,
					isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer)		as 'winner procedure/query'		,
					max(nv_TransactionTime)	as LastTransactionTime		,
					nv_lines	as winner_lines,
					isnull(v_obj_name, v_DeadlockObject)		as loser_object	,
					isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer)		as 'loser procedure/query'		,
					v_lines		as loser_lines,
					max(v_TransactionTime)	as loser_LastTransactionTime	,
					count(drmID)		as Frequency			,
					dbName,
                                        nv_sqlhandle,
                                        v_sqlhandle,
                                        stnv.statement_text as nv_statement_text,
                                        stv.statement_text as v_statement_text,
                                        r.v_code,
                                        r.nv_code
                                
				from	#tmpDRD_Rep r
                                        left join #tmp_statement_text stnv on stnv.sql_handle = r.nv_sqlhandle
                                        left join #tmp_statement_text stv on stv.sql_handle = r.v_sqlhandle
				where	nullif(isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer), '') is not null
					--and nullif(isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer), '') is not null
				group	by
					isnull(nv_obj_name, nv_DeadlockObject) ,
					isnull(v_obj_name, v_DeadlockObject),
					isnull(nullif(cast(nv_Procedure as varchar(max)), ''), nv_InputBuffer),
					isnull(nullif(cast(v_Procedure as varchar(max)), ''), v_InputBuffer),
					nv_lines,
					v_lines,
					dbName,
                                        nv_sqlhandle,
                                        v_sqlhandle,
                                        stnv.statement_text,
                                        stv.statement_text,
                                        r.v_code,
                                        r.nv_code
				) a
			where	winner_object like @objectName
				or loser_object like @objectName
				or [winner procedure/query] like @objectName
				or [loser procedure/query] like @objectName
			order	by 9 desc
		END
		------------------
	END


GO

select  v_code, nv_code, winner_object,  loser_object, sum(frequency) as frequency
from    ##tmp_Results
where   winner_object != 'wmp.dbo.EIMREQUESTDETAILS'
        and loser_object != 'wmp.dbo.EIMREQUESTDETAILS'
group   by v_code, nv_code, winner_object,  loser_object
order   by Frequency desc


select  v_code, nv_code, winner_object,  loser_object, sum(frequency) as frequency
from    ##tmp_Results
where   winner_object = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'
        and loser_object = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'
group   by v_code, nv_code, winner_object,  loser_object
order   by Frequency desc

select  r.*,
        a.creation_time,
        v_handle.query_plan as victim_queryplan,
        nv_handle.query_plan as nvictim_queryplan
from    ##tmp_Results r
        left join sys.dm_exec_query_stats a on convert(varchar(max), a.sql_handle) = convert(varchar(max), r.v_sqlhandle)
        outer apply sys.dm_exec_query_plan(a.plan_handle) v_handle
        left join sys.dm_exec_query_stats a2 on a2.sql_handle = r.nv_sqlhandle
        outer apply sys.dm_exec_query_plan(a2.plan_handle) nv_handle
order   by r.Frequency desc


-- select  *
-- from    ##tmp_Results
-- where   winner_object = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'
        -- and loser_object = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'
-- order   by LastTransactionTime desc, frequency desc

-- select  v_code
-- from    ##tmp_Results
-- where   winner_object = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'
        -- and loser_object = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'
-- union
-- select  nv_code
-- from    ##tmp_Results
-- where   winner_object = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'
        -- and loser_object = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'



-- select  *
-- from    ##tmp_results
-- order   by Frequency desc


-- select  *
-- from    ##tmp_Deadlock_Report_Detail



-- select  count(distinct deadlockID)
-- from    fddba.dbo.wolf_deadlocks_CIN_POR_W_DB01_CRDB_20200108
-- where   lockedobject = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'


-- select  count(drmID), count(distinct drmID)
-- from    ##tmpDRD_REP
-- where   nv_Pagelockobject = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'
        -- or v_Pagelockobject = 'wmp.dbo.NETWORKAUTHENTICATIONLOG'