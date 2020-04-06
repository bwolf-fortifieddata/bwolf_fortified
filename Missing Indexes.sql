--heaps
SELECT	SCHEMA_NAME(o.schema_id) AS [schema]
	,object_name(i.object_id ) AS [table]
	,p.rows
	,user_seeks
	,user_scans
	,user_lookups
	,user_updates
	,last_user_seek
	,last_user_scan
	,last_user_lookup
FROM	sys.indexes i 
	INNER JOIN sys.objects o ON i.object_id = o.object_id
	INNER JOIN sys.partitions p ON i.object_id = p.object_id
		AND i.index_id = p.index_id
	LEFT OUTER JOIN sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id
		AND i.index_id = ius.index_id
WHERE	i.type_desc = 'HEAP'
ORDER	BY rows desc

/************************************************************************************************************************
**--unused indexes
************************************************************************************************************************/

if      object_id('tempdb..#tmp_unused_indexes', 'U') is not null
        drop    table #tmp_unused_indexes
go
SELECT  o.name                  AS ObjectName,
        i.name                  AS IndexName,
        i.index_id              AS IndexID,
        dm_ius.user_seeks       AS UserSeek,
        dm_ius.user_scans       AS UserScans,
        dm_ius.user_lookups     AS UserLookups,
        dm_ius.user_updates     AS UserUpdates,
        p.TableRows,
        'alter INDEX ' + QUOTENAME(i.name) + ' ON ' + QUOTENAME(s.name) + '.' + QUOTENAME(OBJECT_NAME(dm_ius.OBJECT_ID)) + ' disable ' AS 'disable statement'
into    #tmp_unused_indexes
FROM    sys.dm_db_index_usage_stats dm_ius
        INNER JOIN sys.indexes i ON i.index_id = dm_ius.index_id AND dm_ius.OBJECT_ID = i.OBJECT_ID
        INNER JOIN sys.objects o ON dm_ius.OBJECT_ID = o.OBJECT_ID
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN      (
                        SELECT  SUM(p.rows) TableRows, p.index_id, p.OBJECT_ID
                        FROM    sys.partitions p
                        GROUP   BY p.index_id, p.OBJECT_ID
                        ) p ON p.index_id = dm_ius.index_id AND dm_ius.OBJECT_ID = p.OBJECT_ID
WHERE   OBJECTPROPERTY(dm_ius.OBJECT_ID,'IsUserTable') = 1
        AND dm_ius.database_id = DB_ID()
        AND i.type_desc = 'nonclustered'
        AND i.is_primary_key = 0
        AND i.is_unique_constraint = 0
ORDER   BY (dm_ius.user_seeks + dm_ius.user_scans + dm_ius.user_lookups) ASC
GO


select  *
from    #tmp_unused_indexes
where   (UserSeek + UserScans + UserLookups) = 0
order   by (UserSeek + UserScans + UserLookups) asc


-- --indexes not in use
-- SELECT 
-- o.name
-- , indexname=i.name
-- , i.index_id   
-- , reads=user_seeks + user_scans + user_lookups   
-- , writes =  user_updates   
-- , rows = (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.index_id = s.index_id AND s.object_id = p.object_id)
-- , CASE
 -- WHEN s.user_updates < 1 THEN 100
 -- ELSE 1.00 * (s.user_seeks + s.user_scans + s.user_lookups) / s.user_updates
  -- END AS reads_per_write
-- , 'DROP INDEX ' + QUOTENAME(i.name) 
-- + ' ON ' + QUOTENAME(c.name) + '.' + QUOTENAME(OBJECT_NAME(s.object_id)) as 'drop statement'
-- FROM sys.dm_db_index_usage_stats s  
-- INNER JOIN sys.indexes i ON i.index_id = s.index_id AND s.object_id = i.object_id   
-- INNER JOIN sys.objects o on s.object_id = o.object_id
-- INNER JOIN sys.schemas c on o.schema_id = c.schema_id
-- WHERE OBJECTPROPERTY(s.object_id,'IsUserTable') = 1
-- AND s.database_id = DB_ID()   
-- AND i.type_desc = 'nonclustered'
-- AND i.is_primary_key = 0
-- AND i.is_unique_constraint = 0
-- AND (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.index_id = s.index_id AND s.object_id = p.object_id) > 10000
-- ORDER BY reads
/************************************************************************************************************************
**--missing indexes
************************************************************************************************************************/
        drop    table if exists tempdb.dbo.FD_MissingIndexes
        go
/************************************************************************************************************************
** --missing indexes
************************************************************************************************************************/

        SELECT  top 30
                sys.objects.name                                                                                ,
                (avg_total_user_cost * avg_user_impact) * (user_seeks + user_scans)     AS Impact               ,
                ----------------------------------------------------------------------------------------------
                'CREATE NONCLUSTERED INDEX FD_ix_' + replace(sys.objects.name, ' ', '')
                + '_'
                + replace(replace(replace(IsNull(mid.equality_columns, ''), '[', ''), ']', ''), ', ', '_')
                + ' ON ' + sys.objects.name COLLATE DATABASE_DEFAULT
                + ' ( '
                + IsNull(mid.equality_columns, '')
                +
                CASE    
                        WHEN mid.inequality_columns IS NULL THEN ''  
                        ELSE
                                CASE
                                        WHEN mid.equality_columns IS NULL THEN ''  
                                        ELSE ','
                                END     + mid.inequality_columns
                        END
                + ' ) '
                +
                CASE
                        WHEN mid.included_columns IS NULL THEN ''  
                        ELSE 'INCLUDE (' + mid.included_columns + ')'
                END
                + ';'                                                                   AS CreateIndexStatement ,
                ----------------------------------------------------------------------------------------------
                mid.equality_columns                                                                            ,
                mid.inequality_columns                                                                          ,
                mid.included_columns
        into    tempdb.dbo.FD_MissingIndexes
        FROM    sys.dm_db_missing_index_group_stats AS migs 
                INNER JOIN sys.dm_db_missing_index_groups AS mig ON migs.group_handle = mig.index_group_handle 
                INNER JOIN sys.dm_db_missing_index_details AS mid ON mig.index_handle = mid.index_handle AND mid.database_id = DB_ID() 
                INNER JOIN sys.objects WITH (nolock) ON mid.OBJECT_ID = sys.objects.OBJECT_ID 
        WHERE     (
                migs.group_handle IN 
                        ( 
                        SELECT  TOP (500) group_handle 
                        FROM    sys.dm_db_missing_index_group_stats WITH (nolock) 
                        ORDER   BY (avg_total_user_cost * avg_user_impact) * (user_seeks + user_scans) DESC
                        )
                )
                AND OBJECTPROPERTY(sys.objects.OBJECT_ID, 'isusertable')=1 
                and not exists(select 1 from tempdb.dbo.table_indexes_reviewed tir where tir.tname = sys.objects.name)
        ORDER   BY 2 DESC , 3 DESC
/************************************************************************************************************************
**
************************************************************************************************************************/
    select      *
    from        tempdb.dbo.FD_MissingIndexes
    
 
 
 --Overlapping INDEXES
 ;WITH CTE_INDEX_DATA AS (
       SELECT
              SCHEMA_DATA.name AS schema_name,
              TABLE_DATA.name AS table_name,
              INDEX_DATA.name AS index_name,
              STUFF((SELECT  ', ' + COLUMN_DATA_KEY_COLS.name + ' ' + CASE WHEN INDEX_COLUMN_DATA_KEY_COLS.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END -- Include column order (ASC / DESC)

                                  FROM    sys.tables AS T
                                                INNER JOIN sys.indexes INDEX_DATA_KEY_COLS
                                                ON T.object_id = INDEX_DATA_KEY_COLS.object_id
                                                INNER JOIN sys.index_columns INDEX_COLUMN_DATA_KEY_COLS
                                                ON INDEX_DATA_KEY_COLS.object_id = INDEX_COLUMN_DATA_KEY_COLS.object_id
                                                AND INDEX_DATA_KEY_COLS.index_id = INDEX_COLUMN_DATA_KEY_COLS.index_id
                                                INNER JOIN sys.columns COLUMN_DATA_KEY_COLS
                                                ON T.object_id = COLUMN_DATA_KEY_COLS.object_id
                                                AND INDEX_COLUMN_DATA_KEY_COLS.column_id = COLUMN_DATA_KEY_COLS.column_id
                                  WHERE   INDEX_DATA.object_id = INDEX_DATA_KEY_COLS.object_id
                                                AND INDEX_DATA.index_id = INDEX_DATA_KEY_COLS.index_id
                                                AND INDEX_COLUMN_DATA_KEY_COLS.is_included_column = 0
                                  ORDER BY INDEX_COLUMN_DATA_KEY_COLS.key_ordinal
                                  FOR XML PATH('')), 1, 2, '') AS key_column_list ,
          STUFF(( SELECT  ', ' + COLUMN_DATA_INC_COLS.name
                                  FROM    sys.tables AS T
                                                INNER JOIN sys.indexes INDEX_DATA_INC_COLS
                                                ON T.object_id = INDEX_DATA_INC_COLS.object_id
                                                INNER JOIN sys.index_columns INDEX_COLUMN_DATA_INC_COLS
                                                ON INDEX_DATA_INC_COLS.object_id = INDEX_COLUMN_DATA_INC_COLS.object_id
                                                AND INDEX_DATA_INC_COLS.index_id = INDEX_COLUMN_DATA_INC_COLS.index_id
                                                INNER JOIN sys.columns COLUMN_DATA_INC_COLS
                                                ON T.object_id = COLUMN_DATA_INC_COLS.object_id
                                                AND INDEX_COLUMN_DATA_INC_COLS.column_id = COLUMN_DATA_INC_COLS.column_id
                                  WHERE   INDEX_DATA.object_id = INDEX_DATA_INC_COLS.object_id
                                                AND INDEX_DATA.index_id = INDEX_DATA_INC_COLS.index_id
                                                AND INDEX_COLUMN_DATA_INC_COLS.is_included_column = 1
                                  ORDER BY INDEX_COLUMN_DATA_INC_COLS.key_ordinal
                                  FOR XML PATH('')), 1, 2, '') AS include_column_list,
       INDEX_DATA.is_disabled -- Check if index is disabled before determining which dupe to drop (if applicable)
       FROM sys.indexes INDEX_DATA
       INNER JOIN sys.tables TABLE_DATA
       ON TABLE_DATA.object_id = INDEX_DATA.object_id
       INNER JOIN sys.schemas SCHEMA_DATA
       ON SCHEMA_DATA.schema_id = TABLE_DATA.schema_id
       WHERE TABLE_DATA.is_ms_shipped = 0
       AND INDEX_DATA.type_desc IN ('NONCLUSTERED', 'CLUSTERED')
)
SELECT
       *
FROM CTE_INDEX_DATA DUPE1
WHERE EXISTS
(SELECT * FROM CTE_INDEX_DATA DUPE2
 WHERE DUPE1.schema_name = DUPE2.schema_name
 AND DUPE1.table_name = DUPE2.table_name
 AND (DUPE1.key_column_list LIKE LEFT(DUPE2.key_column_list, LEN(DUPE1.key_column_list)) OR DUPE2.key_column_list LIKE LEFT(DUPE1.key_column_list, LEN(DUPE2.key_column_list)))
 AND DUPE1.index_name <> DUPE2.index_name)