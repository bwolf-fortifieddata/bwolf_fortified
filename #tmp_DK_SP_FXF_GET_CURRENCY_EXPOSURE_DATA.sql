use     dk_db_apps
go
--exec DK_SP_FXF_GET_CURRENCY_EXPOSURE_DATA_SM_TEST '2020-05-06',NULL
/*
set     statistics time, io on


exec    DK_SP_FXF_GET_CURRENCY_EXPOSURE_DATA
        @FromDate       = '4/28/2018 12:00:00 AM'       ,
        @ToDate         = '4/28/2020 12:00:00 AM'       ,
        @SearchCriteria = 'T'                           ,
        @Deal           = 'NGF / DANISH BIOGAS'

exec    #tmp_DK_SP_FXF_GET_CURRENCY_EXPOSURE_DATA
        @FromDate       = '4/28/2018 12:00:00 AM'       ,
        @ToDate         = '4/28/2020 12:00:00 AM'       ,
        @SearchCriteria = 'T'                           ,
        @Deal           = 'NGF / DANISH BIOGAS'
*/


create  or alter procedure #tmp_DK_SP_FXF_GET_CURRENCY_EXPOSURE_DATA
--CREATE  or alter PROCEDURE [dbo].[DK_SP_FXF_GET_CURRENCY_EXPOSURE_DATA]
	@FromDate       DATETIME                ,
	@ToDate         DATETIME                ,
	@SearchCriteria varchar(1)      = 'T'   ,
	@Deal           VARCHAR(MAX)    = NULL        
AS          

BEGIN
SET NOCOUNT ON;

/****************************************************************************
**
****************************************************************************/
        declare @sqlStatement   nvarchar(max)   ,
                @parameters     nvarchar(512)

        create  table #tmp_results
                (
	        [Fx Type]       varchar(10)     ,
	        IsFwdFromSpot   int             ,
	        [Status]        varchar(8)      ,
	        [Forward Sent]  int             ,
	        Symbol          varchar(255)    ,
	        [Description]   varchar(255)    ,
	        Deal            varchar(max)    ,
	        Portfolio       varchar(100)    ,
	        UltimateFund    varchar(255)    ,
	        [Value LC]      money           ,
	        [Cust Acct]     varchar(255)    ,
	        [Profit Cntr]   varchar(max)    ,
	        [Forward Date]  datetime        ,
	        SellCurrency    varchar(100)    ,
	        BuyCurrency     varchar(100)    ,
	        ManagerCode     varchar(255)    ,
	        SecId           bigint          ,
	        SecName         varchar(255)    ,
	        [Tran Type]     varchar(100)    ,
	        Comments        varchar(1000)   ,
	        [ID]            bigint          ,
	        TradeID         varchar(30)     ,
	        FxRate          numeric(25, 10) ,
                deal2           varchar(max)
                )

        create  clustered index #ix#tmp_results on #tmp_results([fx type], [tran type])

        -------------------------------------------------------------------------------------------

        select  @parameters = '@deal varchar(max), @fromdate datetime, @todate datetime'


/****************************************************************************
** FX Forward
****************************************************************************/

        select  @sqlStatement = '
        SELECT  ''Fx Forward''  [Fx Type]
	        ,0 [IsFwdFromSpot]
	        ,CASE WHEN DK_TFCED_IS_SENT IS NULL  OR DK_TFCED_IS_SENT = 0 THEN ''Pending''  ELSE ''Approved'' END [Status]
	        ,CASE WHEN DK_TFCED_IS_SENT IS NULL THEN 0 ELSE DK_TFCED_IS_SENT END [Forward Sent]
	        ,DK_TFCED_SYMBOL [Symbol]
	        ,DK_TFCED_SYMBOL [Description]
	        ,DK_TFCED_DEALCODE [Deal]
	        ,DK_TFCED_ACTUALFUND [Portfolio]
	        ,DK_TFCED_ULTFUND [UltimateFund]
	        ,DK_TFCED_MV_LOCAL [Value LC]
	        ,DK_TFCED_CUSTODIAN[Cust Acct]
	        ,DK_TFCED_DEALCODE [Profit Cntr]
	        ,DK_TFCED_SETTLE_DATE [Forward Date]
	        ,DK_TFCED_SELLCURRENCY [SellCurrency]	
	        ,DK_TFCED_BUYCURRENCY [BuyCurrency]	
	        ,DK_TFCED_MANAGER [ManagerCode]	
	        ,DK_TFCED_SECID	[SecId]
	        ,DK_TFCED_SECNAME [SecName]
	        ,DK_TFCED_TRAN_TYPE [Tran Type]
	        ,DK_TFCED_COMMENTS [Comments]
	        ,DK_TFCED_ID [ID]
	        ,'''' [TradeID]
	        ,DK_TFCED_FXRATE [FxRate]
                ,DK_TFCED_DEALCODE [Deal2]
        FROM    DK_TBL_FXF_CURRENCY_EXPOSURE_DATA m  
        WHERE   '

        -------------------------------------------------------------------------------------------
        if      @SEARCHCRITERIA = 'T'
                select  @sqlStatement += '(CONVERT(DATE,DK_TFCED_TRADE_DATE) >= CONVERT(DATE,@FROMDATE)
                        AND CONVERT(DATE,DK_TFCED_TRADE_DATE) <= CONVERT(DATE,@TODATE)
                        )'

        else    if @SearchCriteria = 'C'
                select  @sqlStatement += '(
                                        (
                                        DK_TFCED_CREATE_DATE IS NULL
                                        AND CONVERT(DATE,DK_TFCED_TRADE_DATE) >= CONVERT(DATE,@FROMDATE)
                                        AND CONVERT(DATE,DK_TFCED_TRADE_DATE) <=CONVERT(DATE,@TODATE)
                                        )
	                                OR
                                        (
                                        CONVERT(DATE,DK_TFCED_CREATE_DATE) >= CONVERT(DATE,@FROMDATE)
                                        AND CONVERT(DATE,DK_TFCED_CREATE_DATE) <=CONVERT(DATE,@TODATE)
                                        )
                                )'

        -------------------------------------------------------------------------------------------

        select  @sqlStatement += ' AND ROUND(DK_TFCED_MV_LOCAL,0) <> 0'

        -------------------------------------------------------------------------------------------


        if      @Deal is not null
                select  @sqlStatement += ' and exists(select 1 from DK_FN_GET_CSV_TO_TABLE(@Deal) f where f.string = m.DK_TFCED_DEALCODE)'

        -------------------------------------------------------------------------------------------

        insert  into #tmp_results
                (
	        [Fx Type]       ,	IsFwdFromSpot   ,	[Status]        ,
	        [Forward Sent]  ,	Symbol          ,	[Description]   ,
	        Deal            ,	Portfolio       ,	UltimateFund    ,
	        [Value LC]      ,	[Cust Acct]     ,	[Profit Cntr]   ,
	        [Forward Date]  ,	SellCurrency    ,	BuyCurrency     ,
	        ManagerCode     ,	SecId           ,	SecName         ,
	        [Tran Type]     ,	Comments        ,	[ID]            ,
	        TradeID         ,	FxRate          ,       deal2
                )
        exec    sp_executesql
                @stmt           = @sqlStatement,
                @params         = @parameters,
                @deal           = @deal,
                @fromDate       = @fromDate,
                @toDate         = @todate


/****************************************************************************
** fx spot
****************************************************************************/

        select  DK_TFA_ACCOUNT_NAME,
                DK_TFA_ACCOUNT_NO,
                DK_TFA_BROKER,
                DK_TFA_SWAP_CASH_ACCOUNT
        into    #tmp_tfa
        FROM    dbo.DK_TBL_FXSA_ACCOUNTS(NOLOCK)  
        where   ISNULL(DK_TFA_SWAP_CASH_ACCOUNT, '') = ''


        select  @sqlStatement = '
        ;with   mcte as
        (
        SELECT  ''Fx Spot'' [Fx Type]
	        , NULL [IsFwdFromSpot]
	        ,CASE WHEN tft.DK_TFT_STATUS = ''P'' THEN ''Pending'' 
	        WHEN tft.DK_TFT_STATUS =''N''    or DK_TFT_STATUS = ''E'' THEN  ''Approved''
	        ELSE tft.DK_TFT_STATUS END [Status]
	        , CASE WHEN tft.DK_TFT_STATUS = ''P'' THEN 0 
	        WHEN tft.DK_TFT_STATUS =''N''  or tft.DK_TFT_STATUS = ''E'' THEN  1 ELSE 0 END [Forward Sent]
	        ,tft.DK_TFT_LOCAL_CCY + ''-SPOT'' [Symbol]
	        ,tft.DK_TFT_LOCAL_CCY + ''-SPOT'' [Description]
	        ,SUBSTRING(tft.DK_TFT_MANAGER_CODE,CHARINDEX(''-'',tft.DK_TFT_MANAGER_CODE,0)+1,LEN(tft.DK_TFT_MANAGER_CODE)) [Deal]
                ,isnull(tfa_portfolio.dk_tfa_account_name, '''') as [Portfolio]
                ,isnull(tfa_uf.dk_tfa_account_name, '''') as [UltimateFund]
	        --,ISNULL((SELECT DK_TFA_ACCOUNT_NAME  FROM dbo.DK_TBL_FXSA_ACCOUNTS(NOLOCK)  
		       -- where DK_TFA_ACCOUNT_NO = DK_TFT_ACCOUNT_NO and DK_TFA_BROKER =  DK_TFT_BROKER AND ISNULL(DK_TFA_SWAP_CASH_ACCOUNT, '''') = ''''
         --    ),'''') [Portfolio]
	        -- ,ISNULL((SELECT DK_TFA_ACCOUNT_NAME  FROM dbo.DK_TBL_FXSA_ACCOUNTS(NOLOCK)  
		       -- where DK_TFA_ACCOUNT_NO = DK_TFT_ULT_ACCOUNT_NO and DK_TFA_BROKER =  DK_TFT_BROKER AND ISNULL(DK_TFA_SWAP_CASH_ACCOUNT, '''') = ''''
         --    ), '''' ) [UltimateFund]
	         ,tft.DK_TFT_DENOM_AMT [Value LC]
	         ,tft.DK_TFT_BROKER [Cust Acct]
	         ,SUBSTRING(tft.DK_TFT_MANAGER_CODE,CHARINDEX(''-'',tft.DK_TFT_MANAGER_CODE,0)+1,LEN(tft.DK_TFT_MANAGER_CODE)) [Profit Cntr]
	         ,DK_TFT_SETTLE_DATE [Forward Date]
	         ,DK_TFT_SETTLE_CCY  [SellCurrency]
	         ,DK_TFT_LOCAL_CCY  [BuyCurrency]	
	         ,LEFT(tft.DK_TFT_MANAGER_CODE, CHARINDEX(''-'',tft.DK_TFT_MANAGER_CODE)-1) [ManagerCode]
	         ,'''' [SecId]
	         ,'''' [SecName]
	         ,tft.DK_TFT_TRANS_TYPE [Tran Type]
	         ,tft.DK_TFT_NOTES [Comments]
	         ,tft.DK_TFT_BATCH_ID [ID]
	         ,tft.DK_TFT_TRADE_NO [TradeID]
	         ,tft.DK_TFT_FX_RATE [FxRate]
	
        FROM    DK_TBL_FXSA_TRADES tft
                left join #tmp_tfa tfa_portfolio on tfa_portfolio.DK_TFA_ACCOUNT_NO = tft.DK_TFT_ACCOUNT_NO
                        and tfa_portfolio.DK_TFA_BROKER =  tft.DK_TFT_BROKER
                left join #tmp_tfa tfa_uf on tfa_uf.DK_TFA_ACCOUNT_NO = tft.DK_TFT_ULT_ACCOUNT_NO
                        and tfa_uf.DK_TFA_BROKER =  tft.DK_TFT_BROKER
        WHERE   '


        -------------------------------------------------------------------------------------------

        if      @SearchCriteria = 'T'
                select  @sqlStatement   += ' convert(date,DK_TFT_TRADE_DATE) >= convert(date,@fromdate)
                        and convert(date,DK_TFT_TRADE_DATE) <=convert(date,@todate)'

        -------------------------------------------------------------------------------------------

        else    if @SearchCriteria = 'C'
                select  @sqlStatement   += ' (convert(date,DK_TFT_CREATE_DATETIME) >= convert(date,@fromdate)
                        and  convert(date,DK_TFT_CREATE_DATETIME) <=convert(date,@todate))'

        -------------------------------------------------------------------------------------------
        select  @sqlStatement   += 'AND DK_TFT_DEPARTMENT IS NULL
        and DK_TFT_TRADE_NO like ''S%'')
        select  *, deal as dead2
        from    mcte m '

        if      @Deal is not null
                select  @sqlStatement += 'where exists(select 1 from DK_FN_GET_CSV_TO_TABLE(@Deal) f where f.string = m.deal)'

        -------------------------------------------------------------------------------------------

        insert  into #tmp_results
                (
	        [Fx Type]       ,	IsFwdFromSpot   ,	[Status]        ,
	        [Forward Sent]  ,	Symbol          ,	[Description]   ,
	        Deal            ,	Portfolio       ,	UltimateFund    ,
	        [Value LC]      ,	[Cust Acct]     ,	[Profit Cntr]   ,
	        [Forward Date]  ,	SellCurrency    ,	BuyCurrency     ,
	        ManagerCode     ,	SecId           ,	SecName         ,
	        [Tran Type]     ,	Comments        ,	[ID]            ,
	        TradeID         ,	FxRate          ,       deal2
                )
        exec    sp_executesql
                @stmt           = @sqlStatement,
                @params         = @parameters,
                @deal           = @deal,
                @fromDate       = @fromDate,
                @toDate         = @todate

/****************************************************************************
** fx forward 002
****************************************************************************/

        select  @sqlStatement = '
        --;with   mcte as
        --(
        SELECT  ''Fx Forward''  [Fx Type]
	        ,1 [IsFwdFromSpot]
	        ,CASE WHEN DK_TCFO_STATUS IS NULL  OR DK_TCFO_STATUS = ''PEND'' THEN ''Pending'' 
		          WHEN DK_TCFO_STATUS=''ACCT''  THEN  ''Approved'' END [Status]
	        ,CASE WHEN DK_TCFO_STATUS IS NULL OR DK_TCFO_STATUS = ''PEND''  THEN 0 
		          WHEN DK_TCFO_STATUS=''ACCT'' THEN 1	ELSE 0 END [Forward Sent]
	        ,DK_TCFO_SEC_NAME [Symbol]
	        ,DK_TCFO_SEC_NAME [Description]
	        ,DK_TCOA_DEAL [Deal]
	        ,ISNULL (DK_TCOA_FUND,'''')  [Portfolio]
	        ,ISNULL(DK_TCOA_ULTIMATE_FUND,'''') [UltimateFund]
	        ,DK_TCOA_QUANTITY [Value LC]
	        ,DK_TCOA_CUSTODIAN [Cust Acct]
	        ,DK_TCOA_DEAL [Profit Cntr]
	        ,DK_TCFO_SETTLEMENT_DATE [Forward Date]
	        ,DK_TCFO_SELL_CURENCY [SellCurrency]	
	        ,DK_TCFO_BUY_CURRENCY [BuyCurrency]	
	        ,DK_TCOA_MANGER [ManagerCode]	
	        ,DK_TCFO_SEC_ID	[SecId]
	        ,DK_TCFO_SEC_NAME [SecName]
	        ,DK_TCFO_TRAN_TYPE [Tran Type]
	        ,DK_TCFO_COMMENTS [Comments]
	        ,DK_TCFO_ID [ID]
	        ,cast(DK_TCFO_ID as varchar) [TradeID]
	        ,DK_TCFO_FX_RATE [FxRate]
                ,SUBSTRING(DK_TFT_MANAGER_CODE,CHARINDEX(''-'',DK_TFT_MANAGER_CODE,0)+1,LEN(DK_TFT_MANAGER_CODE)) as deal2
        from    [dbo].[dk_tbl_cfwd_order_allocation] tcoa 
	        inner join [dbo].[dk_tbl_cfwd_order] tcfo on  dk_tcoa_order_id = dk_tcfo_id 
	        inner join [dbo].[dk_tbl_fxsa_trades] fxsa on dk_tfco_external_spot_id = dk_tft_batch_id 
        where   '
        -------------------------------------------------------------------------------------------
        if      @SearchCriteria = 'T'
                select  @sqlStatement += ' convert(date,tcfo.dk_tcfo_trade_date) >= convert(date,@fromdate)
                        and convert(date,tcfo.dk_tcfo_trade_date) <=convert(date,@todate)'

        else    if @SearchCriteria = 'C'
                select  @sqlStatement += ' (convert(date,tcfo.dk_tcfo_create_date) >= convert(date,@fromdate)
                        and  convert(date,tcfo.dk_tcfo_create_date) <=convert(date,@todate))'

        select  @sqlStatement += 'and dk_tft_department is null'
        --)
        --select  [Fx Type]       ,	IsFwdFromSpot   ,	[Status]        ,
	       -- [Forward Sent]  ,	Symbol          ,	[Description]   ,
	       -- Deal            ,	Portfolio       ,	UltimateFund    ,
	       -- [Value LC]      ,	[Cust Acct]     ,	[Profit Cntr]   ,
	       -- [Forward Date]  ,	SellCurrency    ,	BuyCurrency     ,
	       -- ManagerCode     ,	SecId           ,	SecName         ,
	       -- [Tran Type]     ,	Comments        ,	[ID]            ,
	       -- TradeID         ,	FxRate
        --from    mcte m '

        --if      @Deal is not null
        --        select  @sqlStatement += 'where exists(select 1 from DK_FN_GET_CSV_TO_TABLE(@Deal) f where f.string = m.deal2)'
        -------------------------------------------------------------------------------------------
        insert  into #tmp_results
                (
	        [Fx Type]       ,	IsFwdFromSpot   ,	[Status]        ,
	        [Forward Sent]  ,	Symbol          ,	[Description]   ,
	        Deal            ,	Portfolio       ,	UltimateFund    ,
	        [Value LC]      ,	[Cust Acct]     ,	[Profit Cntr]   ,
	        [Forward Date]  ,	SellCurrency    ,	BuyCurrency     ,
	        ManagerCode     ,	SecId           ,	SecName         ,
	        [Tran Type]     ,	Comments        ,	[ID]            ,
	        TradeID         ,	FxRate          ,       deal2
                )
        exec    sp_executesql
                @stmt           = @sqlStatement,
                @params         = @parameters,
                @deal           = @deal,
                @fromDate       = @fromDate,
                @toDate         = @todate


/****************************************************************************
** final results
****************************************************************************/

select  distinct
        [Fx Type]       ,	IsFwdFromSpot   ,	[Status]        ,
	[Forward Sent]  ,	Symbol          ,	[Description]   ,
	Deal            ,	Portfolio       ,	UltimateFund    ,
	[Value LC]      ,	[Cust Acct]     ,	[Profit Cntr]   ,
	[Forward Date]  ,	SellCurrency    ,	BuyCurrency     ,
	ManagerCode     ,	SecId           ,	SecName         ,
	[Tran Type]     ,	Comments        ,	[ID]            ,
	TradeID         ,	FxRate
from    #tmp_results
where   isnull(@deal, deal2) = deal2
order   by [fx type], [tran type],managercode,deal


END

