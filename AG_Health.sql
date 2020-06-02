select  cs.replica_server_name          ,
        cn.group_name                   ,
        s.secondary_lag_seconds         ,
        s.redo_rate                     ,
        s.redo_queue_size               ,
        s.log_send_rate                 ,
        s.log_send_queue_size           ,
        s.synchronization_state_desc    ,
        s.synchronization_health_desc
from    sys.dm_hadr_database_replica_states s
        inner join sys.dm_hadr_availability_replica_cluster_states cs on cs.replica_id = s.replica_id
        inner join sys.dm_hadr_availability_replica_cluster_nodes cn on cn.replica_server_name = cs.replica_server_name
        
        
SELECT  ar.replica_server_name          ,	adc.database_name               ,
	ag.name AS ag_name              ,	drs.is_local                    ,
	drs.is_primary_replica          ,	drs.synchronization_state_desc  ,
	drs.is_commit_participant       ,	drs.synchronization_health_desc ,
	drs.recovery_lsn                ,	drs.truncation_lsn              ,
	drs.last_sent_lsn               ,	drs.last_sent_time              ,
	drs.last_received_lsn           ,	drs.last_received_time          ,
	drs.last_hardened_lsn           ,	drs.last_hardened_time          ,
	drs.last_redone_lsn             ,	drs.last_redone_time            ,
	drs.log_send_queue_size         ,	drs.log_send_rate               ,
	drs.redo_queue_size             ,	drs.redo_rate                   ,
	drs.filestream_send_rate        ,	drs.end_of_log_lsn              ,
	drs.last_commit_lsn             ,	drs.last_commit_time
FROM    sys.dm_hadr_database_replica_states AS drs
        INNER JOIN sys.availability_databases_cluster AS adc ON drs.group_id = adc.group_id
                AND drs.group_database_id = adc.group_database_id
        INNER JOIN sys.availability_groups AS ag ON ag.group_id = drs.group_id
        INNER JOIN sys.availability_replicas AS ar ON drs.group_id = ar.group_id
                AND drs.replica_id = ar.replica_id
ORDER   BY
        drs.log_send_rate desc  ,
	ag.name                 ,
	ar.replica_server_name  ,
	adc.database_name       ;
        
        
        --Check log send queue
;WITH UpTime AS
			(
			SELECT DATEDIFF(SECOND,create_date,GETDATE()) [upTime_secs]
			FROM sys.databases
			WHERE name = 'tempdb'
			),
	AG_Stats AS 
			(
			SELECT AR.replica_server_name,
				   HARS.role_desc, 
				   Db_name(DRS.database_id) [DBName], 
				   CAST(DRS.log_send_queue_size AS DECIMAL(19,2)) log_send_queue_size_KB, 
				   (CAST(perf.cntr_value AS DECIMAL(19,2)) / CAST(UpTime.upTime_secs AS DECIMAL(19,2))) / CAST(1024 AS DECIMAL(19,2)) [log_KB_flushed_per_sec]
			FROM   sys.dm_hadr_database_replica_states DRS 
			INNER JOIN sys.availability_replicas AR ON DRS.replica_id = AR.replica_id 
			INNER JOIN sys.dm_hadr_availability_replica_states HARS ON AR.group_id = HARS.group_id 
				AND AR.replica_id = HARS.replica_id 
			--I am calculating this as an average over the entire time that the instance has been online.
			--To capture a smaller, more recent window, you will need to:
			--1. Store the counter value.
			--2. Wait N seconds.
			--3. Recheck counter value.
			--4. Divide the difference between the two checks by N.
			INNER JOIN sys.dm_os_performance_counters perf ON perf.instance_name = Db_name(DRS.database_id)
				AND perf.counter_name like 'Log Bytes Flushed/sec%'
			CROSS APPLY UpTime
			),
	Pri_CommitTime AS 
			(
			SELECT	replica_server_name
					, DBName
					, [log_KB_flushed_per_sec]
			FROM	AG_Stats
			WHERE	role_desc = 'PRIMARY'
			),
	Sec_CommitTime AS 
			(
			SELECT	replica_server_name
					, DBName
					--Send queue will be NULL if secondary is not online and synchronizing
					, log_send_queue_size_KB
			FROM	AG_Stats
			WHERE	role_desc = 'SECONDARY'
			)
SELECT p.replica_server_name [primary_replica]
	, p.[DBName] AS [DatabaseName]
	, s.replica_server_name [secondary_replica]
	, CAST(s.log_send_queue_size_KB / p.[log_KB_flushed_per_sec] AS BIGINT) [Sync_Lag_Secs]
FROM Pri_CommitTime p
LEFT JOIN Sec_CommitTime s ON [s].[DBName] = [p].[DBName]