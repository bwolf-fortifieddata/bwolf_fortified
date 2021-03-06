
/*************************************************************************************************
**view backup history
*************************************************************************************************/
if      object_id('tempdb..#tmp', 'U') is not null
        drop    table #tmp


create  table #tmp
	(
	location		varchar(256)	,
	[type]			char(1)		,
	backup_finish_date	datetime        ,
        dbname                  varchar(256)
	)
	
declare	@dbName	varchar(256)
set	@dbName = ''

set     @dbname = nullif(@dbname, '')

insert	into #tmp(location, [type], backup_finish_date, dbname)
select	Location, [type], backup_finish_date, dbname
from	(
	SELECT	physical_device_name AS [Location]							,
		[type]											,
		ROW_NUMBER() OVER(PARTITION BY database_name, [type] ORDER BY backup_finish_date DESC) AS 'RN'		,
		backup_finish_date                                                                      ,
                database_name as dbname
	FROM	master..sysdatabases DB
		inner JOIN msdb..backupset BS ON DB.name = BS.database_name
		inner JOIN msdb..backupmediaset MS ON BS.media_set_id = MS.media_set_id
		inner JOIN msdb..backupmediafamily MF ON BS.media_set_id = MF.media_set_id
	where	MF.device_type = 2
		and [TYPE] in ('D')
		and database_name = isnull(@dbName, database_name)
	) a
where	a.RN = 1


--insert	into #tmp(location, [type], backup_finish_date, dbname)
--select	Location, [type], backup_finish_date, dbname
--from	(
--	SELECT	physical_device_name AS [Location]							,
--		[type]											,
--		ROW_NUMBER() OVER(PARTITION BY database_name, [type] ORDER BY backup_finish_date DESC) AS 'RN'		,
--		backup_finish_date                                                                      ,
--                database_name as dbname
--	FROM	master..sysdatabases DB
--		inner JOIN msdb..backupset BS ON DB.name = BS.database_name
--		inner JOIN msdb..backupmediaset MS ON BS.media_set_id = MS.media_set_id
--		inner JOIN msdb..backupmediafamily MF ON BS.media_set_id = MF.media_set_id
--	where	MF.device_type = 2
--		and [TYPE] in ('I')
--		and database_name = @dbName
--	) a
--where	a.RN = 1


--insert	into #tmp(location, [type], backup_finish_date, dbname)
--SELECT	physical_device_name AS [Location]							,
--	[type]											,
--	backup_finish_date,
--        database_name
--FROM	master..sysdatabases DB
--	inner JOIN msdb..backupset BS ON DB.name = BS.database_name
--	inner JOIN msdb..backupmediaset MS ON BS.media_set_id = MS.media_set_id
--	inner JOIN msdb..backupmediafamily MF ON BS.media_set_id = MF.media_set_id
--where	MF.device_type = 2
--	and [TYPE] in ('L')
--	and backup_finish_date > (select MAX(backup_finish_date) from #tmp)
	
select	*
from	#tmp
where   type in ('D', 'I', 'L')
order	by 3 desc