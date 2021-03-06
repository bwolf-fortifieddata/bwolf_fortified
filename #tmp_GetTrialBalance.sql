USE [ODS]
GO
/****** Object:  StoredProcedure [FundAccounting].[GetTrialBalance]    Script Date: 4/21/2020 1:01:53 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 --EXEC FundAccounting.GetTrialBalance 'May 2019', 'DKIL', 1, 1
 /*
 set    statistics time, io on

exec    [FundAccounting].[GetTrialBalance]
        @period         = 'March 2020'  ,
        @fund           = 'DKIL'        ,
        @countOnly      = 0             ,
        @pageNumber     = 1             ,
        @PageSize       = 10000

exec    [FundAccounting].[GetTrialBalance_FD_Wolf]
        @period         = 'March 2020'  ,
        @fund           = 'DKIL'        ,
        @countOnly      = 0             ,
        @pageNumber     = 1             ,
        @PageSize       = 10000

exec    #tmp_GetTrialBalance
        @period         = 'March 2020'  ,
        @fund           = 'DKIL'        ,
        @countOnly      = 0             ,
        @pageNumber     = 1             ,
        @PageSize       = 10000
--create  or alter procedure #tmp_GetTrialBalance
--create or alter   PROCEDURE [FundAccounting].[GetTrialBalance]

 */
create  or alter procedure FundAccounting.GetTrialBalance_FD_Wolf
        (
        @Period                 date                    ,
        @Fund                   varchar(max)    = null  ,
        @CountOnly              bit             = null  ,
        @PageNumber             int             = null  ,
        @PageSize               int             = null  ,
        @MonthEndScheduleBucket varchar(max)    = null
        ,@IsFlashExtract        bit             = 0
        )
AS
	SET NOCOUNT ON

 --       declare
 --               @Period DATE
	--,@Fund VARCHAR(MAX) = NULL
	--,@CountOnly BIT = NULL
	--,@PageNumber INT = NULL
	--,@PageSize INT = NULL
	--,@MonthEndScheduleBucket VARCHAR(MAX) = NULL
	--,@IsFlashExtract bit=0


        declare @offset         int     ,
                @rowcount       int     ,
                @PeriodDate     date    ,
                @PeriodEndDate  datetime = dateadd(s, -1, dateadd(d, 1, cast(eomonth(@period) as datetime)))

        --select  @period         = 'March 2020'  ,
        --        @fund           = 'DKIL'        ,
        --        @countOnly      = 0             ,
        --        @pageNumber     = 0             ,
        --        @PageSize       = 10000
/*********************************************************************************************************************
**
*********************************************************************************************************************/
	select  @PageSize       = ISNULL(@PageSize, 10000)      ,
                @PeriodDate     = convert(date,@PeriodDate)


        if      isnull(@pageNumber, 0) < 1
                Set     @PageNumber = 1

        if      @PageNumber = 1
                set     @offset = 0
        else
                set     @offset = (@PageNumber - 1) * @PageNumber
                

/*********************************************************************************************************************
**
*********************************************************************************************************************/
        drop    table if exists #tmp_PricingApproval
        drop    table if exists #tmp_Funds
        drop    table if exists #tmp_TrialBalance

	create  table #tmp_PricingApproval
                (
                AltId2          int             ,
                SignOffStatus   varchar(200)    ,
                PriceDate       datetime        ,
                PostedDateTime  datetime        ,
                Fund            varchar(200)    ,
                DealCode        varchar(1000)   ,
                MgrCode         varchar(200)
                )

        create  table #tmp_Funds
                (
                Code            varchar(20)     ,
                GenevaPortfolio varchar(20)
                )
/*********************************************************************************************************************
**
*********************************************************************************************************************/	
	insert  into #tmp_PricingApproval
                (
                AltId2          ,
                SignOffStatus   ,
                PriceDate       ,
                PostedDateTime  ,
                Fund            ,
                DealCode        ,
                MgrCode
                )
	exec    [Pricing].[GetPricingWorkflow] @PeriodEndDate,6

	insert  into #tmp_Funds (Code)
	select  item
        from    ToolBox.ParseDelimitedStringIntoTable(@Fund,',')

        select  @rowcount = @@ROWCOUNT
/*********************************************************************************************************************
**
*********************************************************************************************************************/
	--if      not exists(select 1 from #tmp_Funds)
        if      @rowcount = 0
		insert into #tmp_Funds(Code)
		select  EntityCode
                from    [Reference].[GetGenevaEntities](DATEADD(S, -1, DATEADD(MM, DATEDIFF(m, 0, @Period) + 1, 0)))

        update  f
        set     f.GenevaPortfolio = isnull(E.GenevaPortfolio, E.EntityCode)
        from    #tmp_Funds F
                inner join Reference.Entity E on F.Code = E.EntityCode

/*********************************************************************************************************************
**
*********************************************************************************************************************/
        if      @CountOnly = 1
                begin
                        select  ceiling(count(t.id) / (@pagesize * 1.0)) [count]
                        FROM    FundAccounting.RorView (NOLOCK) T -- WITH (NOEXPAND)
                                LEFT JOIN Reference.Entity E ON T.LegalEntityCode = E.EntityCode
                                inner JOIN #tmp_Funds F ON T.PortfolioCode = F.GenevaPortfolio
	                                AND ISNULL(E.EntityCode, T.PortfolioCode) = F.Code
                        WHERE   T.PeriodEndDate = @PeriodEndDate
	                        AND
	                        (
		                        @MonthEndScheduleBucket IS NULL
		                        OR T.MonthEndBucket IN (SELECT item FROM ToolBox.ParseDelimitedStringIntoTable(@MonthEndScheduleBucket,','))
	                        )
                        return
                end
        else
                begin
                        select  t.id
                        into    #tmp_TrialBalance
                        FROM    FundAccounting.RorView (NOLOCK) T -- WITH (NOEXPAND)
                                LEFT JOIN Reference.Entity E ON T.LegalEntityCode = E.EntityCode
                                inner JOIN #tmp_Funds F ON T.PortfolioCode = F.GenevaPortfolio
	                                AND ISNULL(E.EntityCode, T.PortfolioCode) = F.Code
                        WHERE   T.PeriodEndDate = @PeriodEndDate
	                        AND
	                        (
		                        @MonthEndScheduleBucket IS NULL
		                        OR T.MonthEndBucket IN (SELECT item FROM ToolBox.ParseDelimitedStringIntoTable(@MonthEndScheduleBucket,','))
	                        )
                        order   by t.id
                        offset  @offset rows fetch next @pagesize rows only
                        option  (recompile)
                end
        
/*********************************************************************************************************************
**
*********************************************************************************************************************/
        select  T.PeriodStartDate                                                                               ,
	        T.PeriodEndDate                                                                                 ,
	        T.CustodianAccountName                                                                          ,
	        T.CustodianDesc                                                                                 ,
	        T.InventoryState                                                                                ,
	        T.FinAcct                                                                                       ,
	        T.FinAcctDesc                                                                                   ,
	        T.Sect                                                                                          ,
	        T.Cat                                                                                           ,
	        T.SubCategory                                                                                   ,
	        T.Description                                                                                   ,
	        T.OpeningBalance                                                                                ,
	        T.Debits                                                                                        ,
	        T.Credits                                                                                       ,
	        T.ClosingBalance                                                                                ,
	        T.PtdClosingBalance                                                                             ,
	        T.ClosingBalanceJeLineLocalCcy                                                                  ,
	        T.EstimatedClosingBalTxnInvLocalCcy                                                             ,
	        T.TDCashFlag                                                                                    ,
	        T.PortfolioCode                                                                                 ,
	        E2.FundFamily                                                                                   ,
	        T.LegalEntityCode                                       as 'FundLegalEntity'                    ,
	        T.UltimateFund                                                                                  ,
	        T.ManagerCode                                                                                   ,
	        T.DealCode                                                                                      ,
	        T.StrategyCode                                                                                  ,
	        T.CustodianAccount                                                                              ,
	        T.Custodian                                                                                     ,
	        T.LongShort                                                                                     ,
	        T.AssetType                                                                                     ,
	        T.PvInvAltId2                                                                                   ,
	        T.PvInvCode                                                                                     ,
	        T.PvInvDesc                                                                                     ,
	        T.PvInvType                                                                                     ,
	        T.PvInvAssetType                                                                                ,
	        T.PvInvLocalCcy                                                                                 ,
	        T.UnrealFxGlArtFlag                                                                             ,
	        A.Code                                                  as 'Account'                            ,
	        B.Bucket                                                as 'AccruedInterestAssetTypeBucket'     ,
	        T.AccountBucket                                         as 'ReconciledAccountBucket'            ,
	        C.AccountType                                           as 'ReconciliationAccountType'          ,
	        M.Name                                                  as 'AccountBucket'                      ,
	        T.NavBucket                                             as 'ReconciledNavBucket'                ,
	        N.Name                                                  as 'NavBucket'                          ,
	        T.FinancialStatementBucket                                                                      ,
	        T.MonthEndBucket                                                                                ,
	        AC.PeriodCloseDate                                                                              ,
	        t.PricingRelationship                                                                           ,
	        t.EntityInvestmentCode                                                                          ,
	        T.InvesteePortfolioCode                                                                         ,
	        case
                        when    ISNULL(t.IsFlashExtract, 0) = 1 then 'Flash'
		        else    'Final'
                end                                                     as 'ExtractTimingType'                  ,
	        CoverpageExcludedIncluded                               as 'CoverpageExcludedIncluded'          ,
	        case
                        when    ET.SecurityType is not null then 'Excluded SignOff'
		        else    pa.SignOffStatus
                end                                                     as 'SignOffStatus'                      ,
	        PA.PostedDateTime                                       as 'SignOffDate'                        ,
	        t.MTDCapital                                                                                    ,
	        CONVERT(numeric(16, 10), CAST(t.MTDRoR as float))       as 'MTDRoR'                             ,
	        ISNULL(t.CoverpageFund, '')                             as 'CoverpageFund'                      ,
	        ISNULL(t.CoverpageFundFamily, '')                       as 'CoverpageFundFamily'                ,
	        T.PurchasedAI                                                                                   ,
	        T.SoldAI                                                                                        ,
	        T.AccruedDividendCost                                                                           ,
	        T.AccruedDividendWithholding                                                                    ,
	        T.AccruedDividend                                                                               ,
	        T.AccruedInterestCost                                                                           ,
	        T.AccruedInterestWithholding                                                                    ,
	        T.AccruedInterest                                                                               ,
	        T.UnrealizedFXAccruedDividend                                                                   ,
	        T.UnrealizedFXAccruedDividendWHTax                                                              ,
	        T.UnrealizedFXAccruedInterest                                                                   ,
	        T.UnrealizedFXAccruedInterestWithholdingTax                                                     ,
	        T.UnrealizedFXPurchasedAiSoldAi                                                                 ,
	        T.UnrealizedFXOtherAccrual                                                                      ,
	        T.AccruedShortRebate                                                                            ,
	        T.OtherAccrual                                                                                  ,
	        T.PvGroup1                                                                                      ,
	        T.AccrualDesc                                                                                   ,
	        T.AltId2
        from    #tmp_TrialBalance T1
                inner join FundAccounting.RorView T on T1.ID = T.ID
                inner join Reference.Entity E2 on T.PortfolioCode = E2.EntityCode
                left join FundAccounting.AccountingCalendar AC on E2.EntityCode = AC.Fund
	                and AC.PeriodStartDate = T.PeriodStartDate
                left join FundAccounting.Account A on case 
		                when T.Cat in (
				                'Cash Long',
				                'Cash Short'
				                )
			                then ISNULL(T.Sect, '') + ISNULL(T.Cat, '') + ISNULL(T.Description, '')
		                else ISNULL(T.Sect, '') + ISNULL(T.Cat, '') + ISNULL(T.SubCategory, '') + ISNULL(T.Description, '')
		                end = A.Code
                left join Reconciliation.AccountNavMap M on A.AccountNavMapId = M.ID
                left join Reconciliation.NavBucket N on M.NavBucketId = N.ID
                left join Reconciliation.AssetTypeBucket B on T.PvInvAssetType = B.AssetType
                left join Reconciliation.CustodianAccountType C on T.CustodianAccount = C.CustodianAccount
                left join #tmp_PricingApproval PA on PA.AltId2 = T.AltId2
	                and PA.Fund = T.PortfolioCode
	                and PA.DealCode = T.DealCode
	                and PA.MgrCode = T.ManagerCode
	                and t.Sect in (
		                'Assets',
		                'Liabilities'
		                )
	                and t.Cat in (
		                'Investments Long',
		                'Investments Short'
		                )
	                and t.[Description] != 'Accrued Interest'
                left outer join Pricing.PricingWorkflowExcludedSecurityType ET on ET.SecurityType = PvInvType
        where   isnull(t.IsFlashExtract, 0) = @IsFlashExtract
