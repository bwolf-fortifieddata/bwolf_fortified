use	Marketing
GO
if	object_id('tempdb..#tmp_Results', 'u') is not null
	drop	table #tmp_Results


create	table #tmp_Results
	(
	[userEmail]		varchar(100)	,
	[Region]		varchar(30)	,
	[District Manager]	varchar(30)	,
	[Agent Visits]		int		,
	[Prospect Visits]	int		,
	[Total No. Of Visits]	int		,
	[Visit Date]		date		,
	[District Name]		varchar(30)
	)
GO

if	object_id('RDX_RepSF_VisitsByDistrictManager_sendMail', 'P') is not null
	drop	procedure RDX_RepSF_VisitsByDistrictManager_sendMail
GO
CREATE	procedure RDX_RepSF_VisitsByDistrictManager_sendMail
as



declare	@emailSubject		varchar(100)	,
	@textTitle		varchar(max)	,
	@tableHTML		nvarchar(max)	,
	@body			varchar(max)	,
	@userEmail		varchar(2000)	,
	@Region			varchar(30)	,
	@avgCallsPerSalesRep	decimal(5, 2)	,
	@RegionTotal		smallint


declare	c cursor for
select	distinct UserEmail, Region
from	#tmp_Results
--where	Region = 'Northwest'
--where	DistrictName = '34 MN-IA-ND-SD-NE'
--
open	c
--
fetch	next from c into @userEmail, @region
	while	@@FETCH_STATUS != -1
		BEGIN

			select	@RegionTotal		= sum([Total No. Of Visits])	,
				@avgCallsPerSalesRep	= avg(convert(decimal(9, 2), [Total No. Of Visits]))
			from	#tmp_Results
			where	Region = @region


			select	@emailSubject	= 'Visits By District Manager ' + @region,
				@textTitle	= @emailSubject---'Visits By District Manager ' + @region

			set @tableHTML = '<html><head><style>' +
			   'td {border: solid black 1px;padding-left:5px;padding-right:5px;padding-top:1px;padding-bottom:1px;font-size:11pt;} ' +
			   '</style></head><body>' +
			   --'<div style="margin-top:20px; margin-left:5px; margin-bottom:15px; font-weight:bold; font-size:1.3em; font-family:calibri;">' +
			   --@textTitle + '</div>' +
			   '<div style="margin-left:1px; font-family:Calibri;"><table cellpadding=0 cellspacing=0 border=0>' +
				'<tr bgcolor=#00C301>' +
				'<td align=center width = 500><font face="calibri" color=White><b>Region</b></font></td>'+
				'<td align=center width = 500><font face="calibri" color=White><b>District Manager</b></font></td>'+
				'<td align=center width = 500><font face="calibri" color=White><b>Agent Visits</b></font></td>'+
				'<td align=center width = 500><font face="calibri" color=White><b>Prospect Visits</b></font></td>'+
				'<td align=center width = 500><font face="calibri" color=White><b>Total No. Of Visits</b></font></td>'+
				'<td align=center width = 500><font face="calibri" color=White><b>Visit Date</b></font></td>'+
				'<td align=center width = 500><font face="calibri" color=White><b>District Name</b></font></td></tr>'


		; with cCTE as
				(
				SELECT	[Region]		,
					[District Manager]	,
					[Agent Visits]		,
					[Prospect Visits]	,
					[Total No. Of Visits]	,
					[Visit Date]		,
					[District Name]		,
					1 as myOrder
				FROM	#tmp_Results
				where	UserEmail = @UserEmail
				union	all
				select	'Average visits per Sales Rep: '	+ cast(@avgCallsPerSalesRep as varchar) ,
					'Region’s Total Visits: '		+ cast(@RegionTotal as varchar)		,
					NULL										,
					NULL										,
					NULL										,
					NULL										,
					NULL										,
					2

				)
			select @body =
				(
   				SELECT	ROW_NUMBER() over(order by [district Manager]) % 2 as TRRow,
					td = [Region]			,
					td = [District Manager]		,
					td = [Agent Visits]		,
					td = [Prospect Visits]		,
					td = [Total No. Of Visits]	,
					td = [Visit Date]		,
					td = [District Name]
				FROM	cCTE
				order	by myOrder, [district Manager]
				for XML raw('tr'), elements
				);


			
			set @body = REPLACE(@body, '<td>', '<td align=center><font face="calibri">')
			set @body = REPLACE(@body, '</td>', '</font></td>')
			set @body = REPLACE(@body, '_x0020_', space(1))
			set @body = Replace(@body, '_x003D_', '=')
			set @body = Replace(@body, '<tr><TRRow>0</TRRow>', '<tr bgcolor=#FFFFFF>')
			set @body = Replace(@body, '<tr><TRRow>1</TRRow>', '<tr bgcolor=#EEEEF4>')
			set @body = Replace(@body, '<TRRow>0</TRRow>', '')

			--set @tableHTML = @tableHTML
			--	+ '</table></div><div><table><tr><td align=center width = 500>Average visits per Sales Rep: ' + cast(@avgCallsPerSalesRep as varchar) + '</td>'
			--	+ '<td align=center width = 500>Region’s Total Visits: ' + cast(@RegionTotal as varchar) + '</td></tr>'


			set @tableHTML = @tableHTML + @body + '</table></div></body></html>'

			set @tableHTML = '<div style="color:Black; font-size:11pt; font-family:Calibri; width:100px;">' + @tableHTML + '</div>';


			set	@userEmail = @userEmail + '; notifytest@intermexusa.com; bwolf@rdx.com; adossantos@intermexusa.com;rlisy@intermexusa.com;RNilsen@intermexusa.com'


 

			exec	msdb.dbo.sp_send_dbmail
				@profile_name		= 'Alerts'			,
				@recipients = @userEmail,
				--@recipients = 'bwolf@rdx.com; notifytest@intermexusa.com; WVelez@intermexusa.com',
				--@recipients		= 'bwolf@rdx.com;WVelez@intermexusa.com',
				@from_address		= 'NoReply@intermexusa.com'	,
				@body			= @tableHTML			,
				@subject		= @emailSubject			,
				@body_format		= 'HTML'

			--select	@tableHTML, @body, @emailSubject, @textTitle

			fetch	next from c into @userEmail, @region
		END
deallocate	c