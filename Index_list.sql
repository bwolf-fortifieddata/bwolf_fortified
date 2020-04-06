declare @tableName varchar(256)

set     @tableName = 'ProCommission'

SELECT  '['+Sch.name+'].['+ Tab.[name]+']' AS TableName,
        Ind.[name] AS IndexName,
        SUBSTRING(
                        (
                        SELECT ', ' + AC.name
                        FROM    sys.[tables] AS T
                                INNER JOIN sys.[indexes] I ON T.[object_id] = I.[object_id]
                                INNER JOIN sys.[index_columns] IC ON I.[object_id] = IC.[object_id]
                                        AND I.[index_id] = IC.[index_id]
                                INNER JOIN sys.[all_columns] AC ON T.[object_id] = AC.[object_id]
                                        AND IC.[column_id] = AC.[column_id]
                        WHERE   Ind.[object_id] = I.[object_id]
                                AND Ind.index_id = I.index_id
                                AND IC.is_included_column = 0
                        ORDER   BY IC.key_ordinal
                        FOR     XML PATH('')
                        ), 2, 8000) AS KeyCols,

        SUBSTRING(
                        (
                        SELECT ', ' + AC.name
                        FROM    sys.[tables] AS T
                                INNER JOIN sys.[indexes] I ON T.[object_id] = I.[object_id]
                                INNER JOIN sys.[index_columns] IC ON I.[object_id] = IC.[object_id]
                                        AND I.[index_id] = IC.[index_id]
                                INNER JOIN sys.[all_columns] AC ON T.[object_id] = AC.[object_id]
                                        AND IC.[column_id] = AC.[column_id]
                        WHERE   Ind.[object_id] = I.[object_id]
                                AND Ind.index_id = I.index_id
                                AND IC.is_included_column = 1
                        ORDER   BY IC.key_ordinal
                        FOR
                        XML PATH('')
                        ), 2, 8000) AS IncludeCols,
        Ind.type_desc,
        (select  (SUM(ds.[used_page_count]) * 8)/1024.00 from sys.dm_db_partition_stats ds where ds.object_id = ind.object_id and ds.index_id = ind.index_id) AS IndexSizeMB
FROM    sys.[indexes] Ind
        INNER JOIN sys.[tables] AS Tab ON Tab.[object_id] = Ind.[object_id]
        INNER JOIN sys.[schemas] AS Sch ON Sch.[schema_id] = Tab.[schema_id]

where   tab.name = @tableName
ORDER   BY TableName, keycols

exec    sp_columns @tableName

exec    sp_spaceused @tableName


--dbcc    show_statistics(ProCommission, ProCommissionI2)

--select  PROCHECKBATCHGUID, count(1)
--from    ProCommission (nolock)
--group   by PROCHECKBATCHGUID



--CREATE NONCLUSTERED INDEX FD_ix_ProCommission_PROCHECKBATCHGUID ON ProCommission ( [PROCHECKBATCHGUID] ) where PROCHECKBATCHGUID is not null;
--CREATE NONCLUSTERED INDEX FD_ix_ProCommission_PROBATCHGUID ON ProCommission ( [PROBATCHGUID],[CashBonus] ) INCLUDE ([BEEBUSINESSGUID]);
--CREATE NONCLUSTERED INDEX FD_ix_ProCommission_PROBATCHGUID ON ProCommission ( [PROBATCHGUID],[MercedesBonus] ) INCLUDE ([BEEBUSINESSGUID]);
--CREATE NONCLUSTERED INDEX FD_ix_ProCommission_PROBATCHGUID ON ProCommission ( [PROBATCHGUID],[PromotionBoosterAmount] ) INCLUDE ([BEEBUSINESSGUID]);