
declare @crdate             DATETIME
declare @min				VARCHAR(5)
declare @iday				int
declare @ihr				int
declare @imin				bigint
declare @msg				varchar(100)
declare @agentstatus		varchar(20)
declare @maxmemory			varchar(100)

declare @versionstring		varchar(128)
declare @version			int

declare @sql				nvarchar(300)
declare @paramDefinition	nvarchar(500)

select @versionstring = cast(serverproperty('ProductVersion') as varchar(128))
select @version = cast(SUBSTRING(@versionstring,1, CHARINDEX('.',@versionstring,1) -1)	 as int)
set @paramDefinition = N'@crdate_out datetime OUTPUT'

If @version > 9
	set @sql = N'SELECT @crdate_out = sqlserver_start_time FROM sys.dm_os_sys_info'
Else
	set @sql = N'SELECT @crdate_out = crdate FROM master..sysdatabases WHERE name = ''tempdb'''

execute sp_executesql @sql,  @paramDefinition,  @crdate_out =@crdate output;
 

set @imin = DATEDIFF(MINUTE ,@crdate, getdate())
set @iday = 0
if @imin > 60 AND @imin < 1440
begin
		set @ihr = @imin / 60
		set @min = cast(@imin - (@ihr * 60) as varchar)
end
Else
begin
		if @imin > 1440
				set @iday = @imin /1440
		set @ihr = (@imin - (1440 * @iday) )/60
		set @min = cast(@imin - (1440 * @iday) - (60 * @ihr) as varchar)
end

IF NOT EXISTS (SELECT 1 FROM master.dbo.sysprocesses WHERE program_name = N'SQLAgent - Generic Refresher')
       set @agentstatus =  'NOT Running'
Else
       set @agentstatus = 'Running' 

select @maxmemory=  cast(value_in_use as varchar(100)) from sys.configurations where name = N'max server memory (MB)'
       
select 
	@@servername as 'Server Name', 
	SERVERPROPERTY('ComputerNamePhysicalNetBioS') as [Phyiscal Server Name],
	@crdate  as 'sqlserver_start_time', 
	GETDATE() as 'CurrentDateTime', 
	Cast(@iday as varchar) + ' days ' +
       cast(@ihr as varchar) + ' hour(s) and ' +  @min + ' minute(s).' as 'SQL Up Time', 
	@agentstatus 'SQL Agent Status'
	,cpu_count / hyperthread_ratio AS PhysicalCPUs
	,cpu_count AS logicalCPUs
	,@maxmemory as 'max server memory (MB)'
FROM
  sys.dm_os_sys_info