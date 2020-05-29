use     fddba
GO
/*
select  *
from    ##tmp_Deadlock_Report_Detail
*/
if      object_id('tempdb..##tmp_Deadlock_Report_Main', 'U') is not null
        drop    table ##tmp_Deadlock_Report_Main

if      object_id('tempdb..##tmp_Deadlock_Report_Detail', 'U') is not null
        drop    table ##tmp_Deadlock_Report_Detail

CREATE	TABLE ##tmp_Deadlock_Report_Main
	(
	[drmID]		[int] IDENTITY(1,1) NOT NULL	,
	[event_data]	[xml]				,
	[module_guid]	[uniqueidentifier]		,
	[package_guid]	[uniqueidentifier]		,
	[drmDate]	[datetime2](7)			,
	[file_name]	[varchar](256)			,
	[file_offset]	[int] NULL			,
	[File_Exists]	[int] NULL
	)

CREATE	TABLE ##tmp_Deadlock_Report_Detail
	(
	[drmID]			[int]		,
	[PagelockObject]	[varchar](200)	,
	[DeadlockObject]	[varchar](200)	,
	[Victim]		[int]		,
	[Procedure]		[varchar](200)	,
	[LockMode]		[varchar](10)	,
	[Code]			[varchar](1000)	,
	[ClientApp]		[varchar](100)	,
	[HostName]		[varchar](20)	,
	[LoginName]		[varchar](20)	,
	[TransactionTime]	[datetime]	,
	[InputBuffer]		[varchar](max)	,
	[obj_name]		[varchar](1024)	,
	[lines]			[varchar](1024)	,
	[processID]		[varchar](50)	,
	[proc_name]		[varchar](800)	,
	dbname			[varchar](256)	,
	sqlhandle		varchar(1000)
	)
GO

/****** Object:  StoredProcedure [dbo].[uspRecordDeadlocks]    Script Date: 10/3/2017 12:39:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*************************************************************************************************
**  purpose:
**	Read deadlock extended event xml and record the information
**  author:
**      William J. Wolf
**
**  date created:
**      2014-09-11
**
**  parameter description:
@lastOffset

**  date modified/reason:
**
**  Notes:
DISCLAIMER:  This is written by William Wolf(@sqlwarewolf).  William is not responsible for any
adverse affects this may cause.  Always test thoroughly on a development system before implementing in production

delete deadlock_report_main
**************************************************************************************************************/
declare	@lastOffset	bit = 1

set	quoted_identifier on
set	nocount on
/*********************************************************************************************
**
*********************************************************************************************/

	declare	@initial_file_name	nvarchar(60)	,
		@initial_offset		bigint		,
		@drmID			int		,
		@deadlock_xml_root	nvarchar(260)	,
		@filename		nvarchar(260)	,
		@metadatafile		nvarchar(260)
	

	if	object_id('tempdb..#tmpProcess') is not null
		drop	table #tmpProcess
		
	create	table #tmpProcess
		(
		ProcessID		varchar(50)		,
		Victim			varchar(50)		,
		PageLockObject		varchar(200)		,
		DeadLockObject		varchar(200)		,
		[Procedure]		varchar(800)		,
		obj_name		varchar(1500)		,
		LockMode		varchar(10)		,
		Code			varchar(1000)		,
		ClientApp		varchar(100)		,
		HostName		varchar(20)		,
		LoginName		varchar(20)		,
		TransactionTime		datetime		,
		InputBuffer		varchar(max)		,
		associatedObjectID	bigint			,
		[dbid]			int			,
		fileid			int			,
		pageid			int			,
		line			int			,
		stmtstart		int			,
		stmtend			int			,
		sqlhandle		varchar(1000)		,
		drmID			int			,
		tmpProcessID		int identity(1,1)
		)

	create	clustered index #ix#tmpProcess on #tmpProcess(drmID, tmpProcessID)


	if	object_id('tempdb..#tmpLine') is not null
		drop	table #tmpLine

	create	table #tmpLine
		(
		drmID		int		,
		victim		varchar(50)	,
		inputBuffer	varchar(max)	,
		processID	varchar(50)	,
		LineList	varchar(512)
		)
		
	create	clustered index #ix#tmpLine on #tmpLine(drmID)
		


	if	OBJECT_ID('tempdb..#cte', 'U') is not null
		drop	table #cte;
		
	create	table #cte
		(
		DeadlockID	INT	,
		DeadlockGraph	XML
		);
		
	create	clustered index #ix#cte on #cte(DeadlockID);

	if	OBJECT_ID('tempdb..#Victims', 'U') is not null
		drop	table #Victims;
		
	create	table #Victims
		(
		ID		varchar(50)	,
		DeadlockID	int
		);
		
	create	clustered index #ix#Victims on #Victims(DeadlockID);

	if	OBJECT_ID('tempdb..#Locks', 'U') is not null
		drop	table #Locks;
		
	create	table #Locks
		(
		DeadlockID		int		,
		LockID			varchar(100)	,
		LockProcessId		varchar(200)	,
		LockEvent		varchar(8000)	,
		ObjectName		varchar(256)	,
		LockMode		varchar(10)	,
		Database_id		int		,
		AssociatedObjectId	bigint		,
		WaitType		varchar(100)	,
		WaitProcessId		varchar(200)	,
		WaitMode		varchar(10)
		);
		
	create	clustered index #ix#Locks on #Locks(DeadlockID);
		
	if	OBJECT_ID('tempdb..#Lines', 'U') is not null
		drop	table #Lines;
		
	create	table #Lines
		(
		DeadlockID		int		,
		ProcessID		varchar(50)	,
		line			int		,
		stmtstart		int		,
		stmtend			int		,
		sqlhandle		varchar(1000)	,
		PageLockObject		varchar(200)	,
		DeadLockObject		varchar(200)	,
		[Procedure]		varchar(800)	,
		Code			varchar(max)	,
		[DBID]			int		,
		fileID			int		,
		pageID			int		,
		associatedObjectID	bigint
		);
		
	create	clustered index #ix#Lines on #Lines(DeadlockID);
		
	if	OBJECT_ID('tempdb..#Process', 'U') is not null
		drop	table #Process;
		
	create	table #Process
		(
		DeadlockID	int		,
		Victim		bit		,
		LockMode	varchar(10)	,
		ProcessID	varchar(50)	,
		KPID		int		,
		SPID		int		,
		SBID		int		,
		ECID		int		,
		IsolationLevel	varchar(200)	,
		WaitResource	varchar(200)	,
		LogUsed		int		,
		ClientApp	varchar(100)	,
		HostName	varchar(20)	,
		LoginName	varchar(20)	,
		TransactionTime	datetime	,
		BatchStarted	datetime	,
		BatchCompleted	datetime	,
		InputBuffer	varchar(max)	,
		DeadlockGraph	xml		,
		ExecutionStack	xml		,
		QueryStatement	varchar(max)	,
		ProcessQty	int		,
		TranCount	int
		);
		
	create	clustered index #ix#Process on #Process(DeadlockID);
/*********************************************************************************************
**
*********************************************************************************************/
	
	insert	into ##tmp_Deadlock_Report_Main(event_data)--, module_guid, package_guid, [file_name], file_offset)
        output	inserted.drmID, inserted.event_data
	into	#cte(DeadlockID, DeadlockGraph)
        select  deadlockGraph
        from    fddba.dbo.wolf_deadlocks_CIN_POR_W_DB01_CRDB_20200106
        --where   lockedobject = 'wmp.dbo.EIMREQUESTDETAILS'


        SELECT  deadlockxml
        FROM    dbo.PerformanceAnalysisTraceDeadlock p
        where   p.EventSourceConnectionID = 884 -- korewireless

--insert  into #cte
--select  drmID, event_data
--from    Deadlock_Report_Main



/*************************************************************************************************
**
*************************************************************************************************/



	insert	into #Lines
		(
		DeadlockID	,
		ProcessID	,
		line		,
		stmtstart	,
		stmtend		,
		sqlhandle	,
		PagelockObject	,
		DeadlockObject	,
		Code		,
		[Procedure]	,
		[DBID]		,
		fileID		,
		pageID		,
		associatedObjectID
		)
	select	DeadlockID,
		ProcessID		= Deadlock.Process.value('../../@id', 'varchar(50)')						,
		line			= Deadlock.Process.value('@line', 'int')							,
		stmtstart		= Deadlock.Process.value('@stmtstart', 'int')							,
		stmtend			= Deadlock.Process.value('@stmtend', 'int')							,
		sqlhandle		= Deadlock.Process.value('@sqlhandle', 'varchar(1000)')						,
		[PagelockObject]	= DeadlockGraph.value('/deadlock-list[1]/deadlock[1]/resource-list[1]/pagelock[1]/@objectname', 'varchar(200)')	,
		[DeadlockObject]	= DeadlockGraph.value('/deadlock-list[1]/deadlock[1]/resource-list[1]/@objectname', 'varchar(200)'),
		[Procedure]		= Deadlock.Process.value('../../executionStack[1]/frame[1]/@procname[1]', 'varchar(200)')	,
		Code			= Deadlock.Process.value('@code', 'varchar(MAX)')						,
		[dbid]			= COALESCE(
		Deadlock.Process.value('/deadlock-list[1]/deadlock[1]/resource-list[1]/keylock[1]/@dbid', 'int')				,
		Deadlock.Process.value('/deadlock-list[1]/deadlock[1]/resource-list[1]/pagelock[1]/@dbid', 'int')			,
		Deadlock.Process.value('/deadlock-list[1]/deadlock[1]/process-list[1]/process[1]/@currentdb', 'int'))			,
		[fileid]		= Deadlock.Process.value('/deadlock-list[1]/deadlock[1]/resource-list[1]/pagelock[1]/@fileid', 'int')		,
		[pageid]		= Deadlock.Process.value('/deadlock-list[1]/deadlock[1]/resource-list[1]/pagelock[1]/@fileid', 'int')		,
		[associatedObjectID]	= Deadlock.Process.value('/deadlock-list[1]/deadlock[1]/resource-list[1]/pagelock[1]/@associatedObjectId', 'bigint')
	FROM	#CTE CTE
		CROSS APPLY DeadlockGraph.nodes('/deadlock-list/deadlock/process-list/process/executionStack/frame')  AS Deadlock (Process)
	order	by 1
	
/*************************************************************************************************
**
*************************************************************************************************/
	insert	into #Victims
		(
		ID		,
		DeadlockID
		)
	SELECT	ID = Victims.List.value('@id', 'varchar(50)'),
		CTE.DeadlockID
	FROM	#CTE AS CTE
		CROSS APPLY CTE.DeadlockGraph.nodes('//deadlock-list/deadlock/victim-list/victimProcess') AS Victims (List)

/*************************************************************************************************
**
*************************************************************************************************/
	insert	into #Locks
		(
		DeadlockID		,
		LockID			,
		LockProcessId		,
		LockEvent		,
		ObjectName		,
		LockMode		,
		Database_id		,
		AssociatedObjectId	,
		WaitType		,
		WaitProcessId		,
		WaitMode
		)
	SELECT  CTE.DeadlockID,
		MainLock.Process.value('@id', 'varchar(100)') AS LockID,
		OwnerList.Owner.value('@id', 'varchar(200)') AS LockProcessId,
		REPLACE(MainLock.Process.value('local-name(.)', 'varchar(100)'), 'lock', '') AS LockEvent,
		MainLock.Process.value('@objectname', 'sysname') AS ObjectName,
		OwnerList.Owner.value('@mode', 'varchar(10)') AS LockMode,
		MainLock.Process.value('@dbid', 'INTEGER') AS Database_id,
		MainLock.Process.value('@associatedObjectId', 'BIGINT') AS AssociatedObjectId,
		MainLock.Process.value('@WaitType', 'varchar(100)') AS WaitType,
		WaiterList.Owner.value('@id', 'varchar(200)') AS WaitProcessId,
		WaiterList.Owner.value('@mode', 'varchar(10)') AS WaitMode
	FROM    #CTE CTE
		CROSS APPLY CTE.DeadlockGraph.nodes('//deadlock-list/deadlock/resource-list') AS Lock (list)
		CROSS APPLY Lock.list.nodes('*') AS MainLock (Process)
		OUTER APPLY MainLock.Process.nodes('owner-list/owner') AS OwnerList (Owner)
		CROSS APPLY MainLock.Process.nodes('waiter-list/waiter') AS WaiterList (Owner)

        
/*************************************************************************************************
**
*************************************************************************************************/
	insert	into #Process
		(
		DeadlockID	,
		Victim		,
		LockMode	,
		ProcessID	,
		KPID		,
		SPID		,
		SBID		,
		ECID		,
		IsolationLevel	,
		WaitResource	,
		LogUsed		,
		ClientApp	,
		HostName	,
		LoginName	,
		TransactionTime	,
		BatchStarted	,
		BatchCompleted	,
		InputBuffer	,
		DeadlockGraph	,
		ExecutionStack	,
		QueryStatement	,
		ProcessQty	,
		TranCount
		)
	SELECT  CTE.DeadlockID,
		[Victim] = CONVERT(BIT, CASE WHEN Deadlock.Process.value('@id', 'varchar(50)') = ISNULL(Deadlock.Process.value('../../@victim', 'varchar(50)'), v.ID) 
					     THEN 1
					     ELSE 0
					END),
		[LockMode] = Deadlock.Process.value('@lockMode', 'varchar(10)'), -- how is this different from in the resource-list section?
		[ProcessID] = Process.ID, --Deadlock.Process.value('@id', 'varchar(50)'),
		[KPID] = Deadlock.Process.value('@kpid', 'int'), -- kernel-process id / thread ID number
		[SPID] = Deadlock.Process.value('@spid', 'int'), -- system process id (connection to sql)
		[SBID] = Deadlock.Process.value('@sbid', 'int'), -- system batch id / request_id (a query that a SPID is running)
		[ECID] = Deadlock.Process.value('@ecid', 'int'), -- execution context ID (a worker thread running part of a query)
		[IsolationLevel] = Deadlock.Process.value('@isolationlevel', 'varchar(200)'),
		[WaitResource] = Deadlock.Process.value('@waitresource', 'varchar(200)'),
		[LogUsed] = Deadlock.Process.value('@logused', 'int'),
		[ClientApp] = Deadlock.Process.value('@clientapp', 'varchar(100)'),
		[HostName] = Deadlock.Process.value('@hostname', 'varchar(20)'),
		[LoginName] = Deadlock.Process.value('@loginname', 'varchar(20)'),
		[TransactionTime] = Deadlock.Process.value('@lasttranstarted', 'datetime'),
		[BatchStarted] = Deadlock.Process.value('@lastbatchstarted', 'datetime'),
		[BatchCompleted] = Deadlock.Process.value('@lastbatchcompleted', 'datetime'),
		--[InputBuffer] = Input.Buffer.query('.', 'varchar(max)'),
		Deadlock.Process.value('inputbuf[1]', 'varchar(max)'),
		CTE.[DeadlockGraph],
		es.ExecutionStack,
		[QueryStatement] = Execution.Frame.value('.', 'varchar(max)'),
		ProcessQty = SUM(1) OVER (PARTITION BY CTE.DeadlockID),
		TranCount = Deadlock.Process.value('@trancount', 'int')
	FROM    #CTE CTE
		CROSS APPLY CTE.DeadlockGraph.nodes('//deadlock-list/deadlock/process-list/process') AS Deadlock (Process)
		CROSS APPLY (SELECT Deadlock.Process.value('@id', 'varchar(50)') ) AS Process (ID)
		LEFT JOIN #Victims v ON Process.ID = v.ID
			and v.DeadlockID = CTE.DeadlockID
		CROSS APPLY Deadlock.Process.nodes('inputbuf') AS Input (Buffer)
		CROSS APPLY Deadlock.Process.nodes('executionStack') AS Execution (Frame)
	-- get the data from the executionStack node as XML
		CROSS APPLY (SELECT ExecutionStack = (SELECT   ProcNumber = ROW_NUMBER() 
									    OVER (PARTITION BY CTE.DeadlockID,
											       Deadlock.Process.value('@id', 'varchar(50)'),
											       Execution.Stack.value('@procname', 'sysname'),
											       Execution.Stack.value('@code', 'varchar(MAX)') 
										      ORDER BY (SELECT 1)),
								ProcName = Execution.Stack.value('@procname', 'sysname'),
								Line = Execution.Stack.value('@line', 'int'),
								SQLHandle = Execution.Stack.value('@sqlhandle', 'varchar(64)'),
								Code = LTRIM(RTRIM(Execution.Stack.value('.', 'varchar(MAX)')))
							FROM Execution.Frame.nodes('frame') AS Execution (Stack)
							ORDER BY ProcNumber
							FOR XML PATH('frame'), ROOT('executionStack'), TYPE )
			    ) es
        
/*************************************************************************************************
**
*************************************************************************************************/
	insert	into #tmpProcess
		(
		ProcessID		,	Victim			,	PageLockObject		,
		DeadLockObject		,	[Procedure]		,	LockMode		,
		Code			,	ClientApp		,	HostName		,
		LoginName		,	TransactionTime		,	InputBuffer		,
		associatedObjectID	,	[dbid]			,	fileid			,
		pageid			,	line			,	stmtstart		,
		stmtend			,	sqlhandle		,	drmID
		)
	select	p.ProcessID		,
		p.Victim		,
		n.PageLockObject	,
		--n.DeadLockObject	,
                l.ObjectName            ,
		n.[Procedure]		,
		p.LockMode		,
		n.Code			,
		p.ClientApp		,
		p.HostName		,
		p.LoginName		,
		isnull(p.TransactionTime, p.BatchCompleted)	,
		ltrim(rtrim(p.InputBuffer)) as InputBuffer,
		n.associatedObjectID	,
		n.[DBID]		,
		n.fileID		,
		n.pageID		,
		n.line			,
		n.stmtstart		,
		n.stmtend		,
		n.sqlhandle		,
		p.DeadlockID
	FROM    #Process p
		LEFT JOIN #Locks l ON p.DeadlockID = l.DeadlockID
		       AND p.ProcessID = l.LockProcessID
		left join #Lines n on n.DeadlockID = p.DeadlockID
			and p.ProcessID = n.ProcessID
	ORDER BY p.DeadlockId,
		p.Victim DESC,
		p.ProcessId;
/*************************************************************************************************
**
*************************************************************************************************/

	
	update	tp
	set	tp.[procedure] = (db_name(p.database_id) + '.' + OBJECT_SCHEMA_NAME ( p.obj_id, p.database_id ) + '.' + object_name( p.obj_id, p.database_id ))
	from	(
		select	distinct tmpProcessID, [procedure],
			ltrim(rtrim(left(right(ltrim(rtrim(InputBuffer)), (len(ltrim(rtrim(InputBuffer))) - (charindex('Database ID = ', ltrim(rtrim(InputBuffer))) + 13))), charindex(' ', right(ltrim(rtrim(InputBuffer)), (len(ltrim(rtrim(InputBuffer))) - (charindex('Database ID = ', ltrim(rtrim(InputBuffer))) + 13))))))) as database_id,
			ltrim(rtrim(left(right(ltrim(rtrim(InputBuffer)), (len(ltrim(rtrim(InputBuffer))) - (charindex('Object Id = ', ltrim(rtrim(InputBuffer))) + 11))), charindex(']', right(ltrim(rtrim(InputBuffer)), (len(ltrim(rtrim(InputBuffer))) - (charindex('Object Id = ', ltrim(rtrim(InputBuffer))) + 11))-1))))) as obj_id
		from	#tmpProcess p
		where	InputBuffer like '%Object ID%'
		) p
		inner join #tmpProcess tp on p.tmpProcessID = tp.tmpProcessID
	where	nullif(p.[procedure], '') is null

/*********************************************************************************************
**
*********************************************************************************************/
	insert	into #tmpLine
		(
		drmID		,	victim		,
		inputBuffer	,	processID	,
		LineList
		)
	SELECT	drmID				,
		victim				,
		ltrim(rtrim(inputBuffer))	,
		processID			,
		STUFF
			(
				(
				SELECT	distinct ',' + right(replicate('0', 5) + cast(line as varchar), 5)
				FROM	#tmpProcess
				WHERE	drmID = a.drmID
					and victim = a.victim
					and processID = a.processID
				order	by  ',' + right(replicate('0', 5) + cast(line as varchar), 5)
				FOR XML PATH ('')
				), 1, 1, ''
			)  AS LineList
	FROM	#tmpProcess AS a
	group	by
		drmID		,
		victim		,
		inputBuffer	,
		processID
/*********************************************************************************************
**
*********************************************************************************************/
	declare	@dbName			varchar(256)	,
		@dbID			varchar(20)	,
		@query			varchar(max)

		
	declare	c cursor for
	select	distinct
		db_name(p.[dbid]) as dbName	,
		p.[dbid]
	from	#tmpProcess p
	--
	open	c
	--
	fetch	next from c into @dbName, @dbID
		while	@@fetch_status != -1
			BEGIN
				set	@query = '
				update	p
				set	p.obj_name = (db_name(p.[dbid]) + ''.'' + OBJECT_SCHEMA_NAME ( t.object_id, p.[dbid] ) + ''.'' + object_name( t.object_id, p.[dbid] ) + isnull(''.'' + i.name, ''''))
				from	#tmpProcess p
					inner join ' + quotename(@dbname) + '.sys.partitions t	on t.partition_id = p.associatedObjectID
					left join ' + quotename(@dbname) + '.sys.indexes i	on i.object_id = t.object_id
						and i.index_id = t.index_id
				where	p.[dbid] = ' + char(39) + @dbid + char(39)
						
				exec	(@query)
				
				fetch	next from c into @dbName, @dbID
			END
	deallocate c

/*********************************************************************************************
**
*********************************************************************************************/
	--select	d.*
	--into	deadlock_report_detail_bak_20141204

	--delete	d
	--from	deadlock_report_detail d
	--	inner join #tmpProcess p on p.drmID = d.drmID

	--from	deadlock_report_detail d
	--	inner join #tmpProcess p on p.drmID = d.drmID


	insert	into ##tmp_deadlock_report_detail
		(
		dbName			,
		processID		,
		Victim			,
		pageLockObject		,
		deadLockObject		,
		[procedure]		,
		obj_name		,
		lockMode		,
		Code			,
		ClientApp		,
		HostName		,
		LoginName		,
		TransactionTime		,
		InputBuffer		,
		lines			,
		drmID                   ,
                sqlhandle
		)
	select	distinct
		DB_NAME(a.[dbid])	,
		a.processID		,
		a.victim		,
		a.pageLockObject	,
		a.deadLockObject	,
		a.[procedure]		,
		a.obj_name		,
		a.lockMode		,
		a.code			,
		a.clientApp		,
		a.HostName		,
		a.LoginName		,
		a.TransactionTime	,
		a.InputBuffer		,
		l.LineList		,
		a.drmID                 ,
                a.sqlhandle
        FROM	#tmpProcess a
		inner join #tmpLine l on l.drmID = a.drmID
			and l.processID = a.processID
/*********************************************************************************************
**
*********************************************************************************************/
