if      object_id('tempdb..#tmp', 'U') is not null
        drop    table #tmp

select	object_schema_name(object_id) + '.' + object_name(object_id)	as table_name		,
	sum(case when index_id < 2 then row_count else 0 end)		as row_count		,
	8*sum(reserved_page_count)					as reserved_kb		,
	8*sum
		(
		case 
			when index_id<2 then in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count 
			else lob_used_page_count + row_overflow_used_page_count 
		end
		)							as data_kb		,
	8*(sum(used_page_count) - sum
		(
		case 
			when index_id<2 then in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count 
			else lob_used_page_count + row_overflow_used_page_count 
		end
		)
	)								as index_kb		,
	8*sum(reserved_page_count-used_page_count)			as unused_kb
into    #tmp
from	sys.dm_db_partition_stats
where	object_id > 1024
group	by object_id
order	by 5 desc


select  table_name                                      ,
        row_count                                       ,
        reserved_kb/1024.00     as 'reserved_mb'        ,
        data_kb/1024.00         as 'data_mb'            ,
        unused_kb/1024.00       as 'unused_mb'          ,
        index_kb/1024.00        as 'index_mb'
from    #tmp
order   by 3 desc