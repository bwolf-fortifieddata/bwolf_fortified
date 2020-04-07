USE master
GO
declare @servername varchar(256)

select  @servername = left(@@SERVERNAME, charindex('\', @@SERVERNAME)-1)


if      object_id('tempdb..#tmp_error_log_directory', 'U') is not null
        drop    table #tmp_error_log_directory

create  table #tmp_error_log_directory
        (
        logDate         datetime        ,
        ProcessInfo     varchar(256)    ,
        logTxt          varchar(max)
        )

insert  into #tmp_error_log_directory(logDate, ProcessInfo, logTxt)
exec    xp_readerrorlog 0, 1, N'Logging SQL Server messages in file'

update  d
set     d.logTxt = replace(d.logtxt, 'Logging SQL Server messages in file ' + char(39), '')
from    #tmp_error_log_directory d

update  d
set     d.logTxt = replace(d.logtxt, char(39) + '.', '')
from    #tmp_error_log_directory d

select  replace('\\' + @servername + '\' + stuff(logTxt, 2, 1, '$'), '\ERRORLOG', '') as error_log_directory
from    #tmp_error_log_directory d