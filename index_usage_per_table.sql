drop    table #tmp

SELECT
o.name AS ObjectName
, i.name AS IndexName
, i.index_id AS IndexID
, isnull(dm_ius.user_seeks, 0) AS UserSeek
, isnull(dm_ius.user_scans, 0) AS UserScans
, isnull(dm_ius.user_lookups, 0) AS UserLookups
, isnull(dm_ius.user_updates, 0) AS UserUpdates
, p.TableRows
into    #tmp
FROM    sys.dm_db_index_usage_stats dm_ius
        right JOIN sys.indexes i ON i.index_id = dm_ius.index_id AND dm_ius.OBJECT_ID = i.OBJECT_ID
        INNER JOIN sys.objects o ON i.OBJECT_ID = o.OBJECT_ID
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN (SELECT SUM(p.rows) TableRows, p.index_id, p.OBJECT_ID
        FROM sys.partitions p GROUP BY p.index_id, p.OBJECT_ID) p ON p.index_id = i.index_id AND o.OBJECT_ID = p.OBJECT_ID
WHERE   OBJECTPROPERTY(o.OBJECT_ID,'IsUserTable') = 1
order   by 1

select  objectName,
        sum(userSeek) as userSeeks,
        sum(userScans) as userScans,
        sum(userLookups) as userLookups,
        sum(userUpdates) as userUpdates,
        max(tableRows) as tableRows

from    #tmp
group   by objectName
order   by 1

