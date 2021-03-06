declare @dbName varchar(256)

select  @dbname = 'UsageReporting'

select  @dbname = nullif(@dbname, '')

/*************************************************************************************************
**view recovery model
*************************************************************************************************/
	create	table #DBRecovery
		(
		db_name		nvarchar(128),
		recovery	nvarchar(30)
		)

	Insert	Into #DBRecovery
	Exec sp_MSforeachdb 'SELECT ''?'', CAST(DATABASEPROPERTYEX(''?'', ''Recovery'') as nvarchar(30))'

	select	*
	from	#DBRecovery
        where   [db_name] = isnull(@dbName, [db_name])

	drop	table #DBRecovery
	
/*************************************************************************************************
**view backup history
*************************************************************************************************/

	SELECT	database_name									AS [DBName]			,
		user_name									AS [UserName]			,
		cast(backup_size AS decimal(20,2)) / 1048576					AS [BackupSize_MB]		,
		cast(compressed_backup_size AS decimal(20,2)) / 1048576				AS [Compressed_BackupSize_MB]	,
		datediff(n,backup_start_date,backup_finish_date)				AS [Duration_Min]		,
		datediff(dd,backup_finish_date,Getdate())					AS [BackupAge_days]		,
		backup_finish_date								AS [FinishDate]			,
		physical_device_name								AS [Location]			,
		MF.device_type													,
		case	type
			when	'D'	then 'Database'
			when	'I'	then 'Differential Database'
			when	'L'	then 'Log'
			when	'F'	then 'File or Filegroup'
			when	'G'	then 'Differential File'
			when	'P'	then 'Partial'
			when	'Q'	then 'Differential Partial'
		end										as [Backup_Type]		,
		bs.software_vendor_id												,
		software_major_version												,
		software_minor_version												,
		software_build_version												,
		recovery_model
	FROM	master..sysdatabases DB
		inner JOIN msdb..backupset BS ON DB.name = BS.database_name
		inner JOIN msdb..backupmediaset MS ON BS.media_set_id = MS.media_set_id
		inner JOIN msdb..backupmediafamily MF ON BS.media_set_id = MF.media_set_id
	where	[type] = 'D'
                and database_name = isnull(@dbName, database_name)
	order	by backup_finish_date desc
