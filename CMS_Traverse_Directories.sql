use     msdb
go
if      object_id('tempdb..#tmp', 'U') is not null
        drop    table #tmp
go
if      object_id('tempdb..#tmp_results', 'U') is not null
        drop    table #tmp_results
go
select  g.server_group_id                               ,
        g.parent_id                                     ,
        cast(g.name as varchar(512))    as 'gname'      ,
        cast(g.name as varchar(512))    as 'root_name'
into    #tmp
FROM    [msdb].[dbo].[sysmanagement_shared_server_groups] g

;with   #rcte
        (
        server_group_id         ,
        parent_id               ,
        gname                   ,
        root_name
        )
        as
        (
        select  g.server_group_id                                                                       ,
                g.parent_id                                                                             ,
                g.gname                                                                                 ,
                g.root_name
        FROM    #tmp g
        where   g.parent_id is null
        union   all
        select  g.server_group_id                                                                       ,
                g.parent_id                                                                             ,
                g.gname                                                                                 ,
                cast((r.root_name + ' > ' + g.root_name) as varchar(512))       as 'parent_name'
        FROM    #tmp g
                inner join #rcte r on r.server_group_id = g.parent_id
        )
select  *
into    #tmp_results
from    #rcte


select  @@SERVERNAME as management_server, r.root_name, s.name as server_name
from    #tmp_results r
        inner join [sysmanagement_shared_registered_servers] s on s.server_group_id = r.server_group_id
order   by r.root_name
