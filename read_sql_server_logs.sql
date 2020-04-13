/****************************************************************************************************
** SQL 2000
****************************************************************************************************/
-- if	object_id('tempdb..#tmpErrorLog') is not null
	-- drop table #tmpErrorLog
	
-- declare	@hours int
-- set	@hours = 24

-- create	table #tmpErrorLog
	-- (
	-- errorLogID	int identity(1,1)	,
	-- errorLogDate	datetime		,
	-- errorLogType	varchar(32)		,
	-- errorLog	varchar(1024)		,
	-- continuationRow	int 
	-- )
	
-- create	clustered index ix_tmpErrorLog_errorLogID on #tmpErrorLog(errorLogID)
-- insert	into #tmpErrorLog(errorLog, continuationRow)
-- exec	sp_readerrorlog 1, 'E:\Microsoft SQL Server\MSSQL\LOG\ERRORLOG.1'

-- update	e
-- set	e.errorLogDate	= left(e.errorLog, 22)      ,
	-- e.errorLogType	= replace(rtrim(ltrim(left(e.errorLog, charindex('  ', e.errorLog)-1))), left(e.errorLog, 23), '') ,
	-- e.errorLog	= ltrim(rtrim(replace(replace(e.errorLog, left(e.errorLog, 23), ''), replace(rtrim(ltrim(left(e.errorLog, charindex('  ', e.errorLog)-1))), left(e.errorLog, 23), ''), '')))
-- from	#tmpErrorLog e
-- where	e.continuationRow = 0
	-- and isdate(left(e.errorLog, 22)) = 1
-- create	index ix_tmpErrorLog_errorLogDate on #tmpErrorLog(errorLogDate)

-- select	*
-- from	#tmpErrorLog
-- where	errorLogDate is not null






/****************************************************************************************************
** SQL 2005 and up

You can read in any number of logs.  Just uncomment the amount of logs to read in.
****************************************************************************************************/


if	object_id('tempdb..#tmpErrorLog') is not null
	drop table #tmpErrorLog

create	table #tmpErrorLog
	(
	logDate		datetime	,
	processInfo	varchar(56)	,
	logTxt		varchar(max)	,
	elID		int identity(1,1)
	)
        
declare @nL     int             = 1     , --Logs to read
        @p1     int             = 0     , --log number
        @p2     int             = 1     , --Log File type.  1 or Null for error log.  2 for SQL Agent LOG
        @p3     varchar(255)    = null  , --string to search for
        @p4     varchar(255)    = null    --second string to further refine
	
        

        
create	clustered index ix_tmpErrorLog_logDate on #tmpErrorLog(logDate)


while   @p1 < @Nl
        BEGIN


                insert	into #tmpErrorLog(logDate, processInfo, logTxt)
                exec	sp_readerrorlog
			@p1		,
			@p2		,
			@p3		,
			@p4
                
                set     @p1 = @p1 + 1
                
        END



select	*
from	#tmpErrorLog
where	logTxt not like 'log was backed up%'
        and logTxt not like 'backup database%'
        and logTxt not like 'database backed up%'
        and logTxt not like 'database differential%'
        -- (@p3 is not null or logTxt like '%' + @p3 + '%')
        -- or (@p4 is not null or logTxt like '%' + @p4 + '%')
order	by 1 desc
