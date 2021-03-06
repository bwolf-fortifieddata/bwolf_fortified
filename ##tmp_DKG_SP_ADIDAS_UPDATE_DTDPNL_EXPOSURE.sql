USE [DK_DB_ADIDAS]
GO
/****** Object:  StoredProcedure [dbo].[DKG_SP_ADIDAS_UPDATE_DTDPNL_EXPOSURE]    Script Date: 4/2/2020 2:32:47 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Jianqin Lou
-- Create date: 03/18/2016
-- Description:	Insert all position level invested capital and pnl to InputExposuresDetails (intermediate table for debug/audit)
--				Aggregate to deal level and insert to InputExposures (for PCALC)
-- =============================================
-- [DKG_SP_ADIDAS_UPDATE_DTDPNL_EXPOSURE] '2015-01-01', '2015-12-31', 'SYSTEM'
/*
set     statistics time, io on
exec    ##tmp_DKG_SP_ADIDAS_UPDATE_DTDPNL_EXPOSURE '2020-02-16', '2020-03-30', 'SYSTEM'
*/
create  or alter PROCEDURE ##tmp_DKG_SP_ADIDAS_UPDATE_DTDPNL_EXPOSURE
	@StartDate DATE = NULL,
	@EndDate DATE = NULL,
	@User VARCHAR(128) = NULL
AS
--BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
        --set     transaction isolation level read uncommitted
/**********************************************************************************
**
**********************************************************************************/
        --declare @StartDate      datetime        = '2020-02-16',--NULL,
	       -- @EndDate        datetime        = '2020-03-30',--NULL,
	       -- @User           varchar(128)    = 'SYSTEM'--NULL


        --drop    table if exists #SecurityFilter
        --drop    table if exists #FundFilter
        --drop    table if exists #SecurityTypesZeroExposure
        --drop    table if exists #SecurityTypesEquityOptions
        --drop    table if exists #SecurityTypesCDS
        --drop    table if exists #CreditContract
        --drop    table if exists #tmp_dkg_tbl_adidas_dtd_pnl_final
        --drop    table if exists #tmp_dk_vw_geneva_flatview
        --drop    table if exists #tmp_PositionLevelDetail
        --drop    table if exists #tmp_what_would_be_updated
        --drop    table if exists #tmp_dk_vw_opt_delta
        --drop    table if exists #tmp_dk_vw_opt_undl_price
        



        create  table #securitytypeszeroexposure
                (
                securitytype    varchar(50)
                )
	
        create  table #securitytypesequityoptions
                (
                securitytype varchar(50)
                )

        create  table #securitytypescds
                (
                securitytype    varchar(50)
                )

	-- variables declaration
        declare @datestart      datetime        ,
	        @dateend        datetime

	-- startdate = 2010-01-01 in case the parameter is not passed
	if      (@startdate is null)
		select @startdate = '2010-01-01'
	--else 
	--	select @datestart = @startdate

	-- enddate = max(risk dtd pnl date) in case the parameter is not passed
	if(@enddate is null)
		-- todo, replace with business date table
		select @enddate = convert(date, max(dkg_tdp_date_to)) from [dk_db_adidas].[dbo].[dkg_tbl_adidas_dtd_pnl_master] with(nolock)
	--else 
	--	select @dateend = @enddate

        	
/**********************************************************************************
** -- Security Types Temporary Tables
**********************************************************************************/
        insert  into #securitytypeszeroexposure(securitytype)
        values  ('Cash and Equivalents')        ,
	        ('Currency forward')            ,
	        ('Currency futures option')

	insert  into #securitytypesequityoptions(securitytype)
        values  ('Exchange-traded equity option')       ,
	        ('OTC equity option')

	insert  into #securitytypescds(securitytype)
        values  ('Credit Default Swap')         ,
	        ('Credit Default Swap Index')   ,
	        ('CMBX')

/**********************************************************************************
** #creditcontract
**********************************************************************************/

        select  b.dk_tccd_period_end_date                               ,
                b.dk_tccd_portfolio                                     ,
                b.dk_tccd_investment_id                                 ,
                b.[total funded]                as total_funded         ,
                b.[total unfunded]              as total_unfunded       ,
                b.[total commitment]            as total_commitment
        into    #creditcontract
        from    (
                select  c.dk_tccd_period_end_date       ,
                        c.dk_tccd_portfolio             ,
                        c.dk_tccd_investment_id         ,
                        c.dk_tccd_invest                ,
                        c.dk_tccd_globalamt
                from    dkg_tbl_credit_contract_details c
                where   c.dk_tccd_period_end_date between @startdate and @enddate
                        and c.dk_tccd_invest in ('total funded', 'total unfunded', 'total commitment')
	        ) a
	        pivot
                (
                sum     (dk_tccd_globalamt)
                for     dk_tccd_invest in
                        (
                        [total funded]    ,
                        [total unfunded]  ,
                        [total commitment]
                        )
                ) b


/**********************************************************************************
**
**********************************************************************************/
	-- All the SPV security ID that should be filtered
	
	select  altid 
        into    #securityfilter
	from    dk_db_pnl_monitor.dbo.dk_vw_select_formula_spv_investments with(nolock)
	where   cast(altid as varchar) not in 
                        (
                        select  dk_ied_value
                        from    dk_db_reference..dkg_vw_inclusion_exclusion_details
	                where   dk_ied_type                     = 'investment'
                                and dk_ied_application_code     = 'dk_adidas'
	                        and dk_ied_module_name          = 'adidas'
                                and dk_ied_included             = 1
                        )
	union
	select  dk_ied_value
        from    dk_db_reference..dkg_vw_inclusion_exclusion_details
	where   dk_ied_type                     = 'investment'
                and dk_ied_application_code     = 'dk_adidas'
	        and dk_ied_module_name          = 'adidas'
                and dk_ied_included             = 0
/**********************************************************************************
**
**********************************************************************************/
	-- All the Funds that should not be filtered
	select  distinct dk_tfm_fund_name  as fund
        into    #fundfilter
        from    dk_db_reference.dbo.dk_tbl_fund_master with(nolock)
	        inner join dk_db_reference.dbo.dk_tbl_app_fund_mapping with(nolock) 
	                on dk_tfm_id = dk_fk_tfm_tafm_fund_id
        where   dk_fk_tfm_tafm_application_code = 'dk_covp'  


/**********************************************************************************
**
**********************************************************************************/    

        select  f.dkg_tdp_id                    ,
                f.dkg_tdp_date_from             ,
                f.dkg_tdp_ultimate_manager      ,
                f.dkg_tdp_ultimate_deal_code    ,
                f.dkg_tdp_ultimate_fund         ,
                f.dkg_tdp_alt_id2               ,
                f.dkg_tdp_print_group           ,
                f.dkg_tdp_quantity_end          ,
                f.dkg_tdp_price_end             ,
                f.dkg_tdp_fxrate_end            ,
                f.dkg_tdp_total_income_base     ,
                f.dkg_tdp_total_income_local    ,
                f.dkg_tdp_fxrate_begin          ,
                f.dkg_tdp_coupon                ,
                f.dkg_tdp_dividend              ,
                f.dkg_tdp_market_value          ,
                f.dkg_tdp_exposure_base         ,
                f.dkg_tdp_exposure_local        ,
                f.dkg_tdp_vehicle_code          ,
                f.dkg_tdp_date_to               ,
                c.dk_tccd_investment_id         ,
                c.dk_tccd_portfolio             ,
                c.dk_tccd_period_end_date       ,
                c.total_commitment              ,
                c.total_funded                  ,
                c.total_unfunded
        into    #tmp_dkg_tbl_adidas_dtd_pnl_final
        from    dkg_tbl_adidas_dtd_pnl_final f
                left join #creditcontract c
                        on      f.dkg_tdp_date_to       = c.dk_tccd_period_end_date
		        and     f.dkg_tdp_vehicle_code  = c.dk_tccd_portfolio
		        and     f.dkg_tdp_alt_id2       = c.dk_tccd_investment_id
		        and     c.total_commitment      <> 0
        where   f.dkg_tdp_date_from between @startdate and @enddate
                and exists(select 1 from #FundFilter ff where f.dkg_tdp_vehicle_code = ff.fund)
        --option  (recompile)


/**********************************************************************************
**
**********************************************************************************/    

        select  dk_vgf_ts_id                    ,
	        dk_vgf_nb_geneva_pricing_factor ,
	        dk_vgf_nb_geneva_unit_factor
        into    #tmp_dk_vw_geneva_flatview
        from    dk_db_secmaster.dbo.dk_vw_geneva_flatview v
        where   exists(select 1 from #tmp_dkg_tbl_adidas_dtd_pnl_final f where f.dkg_tdp_alt_id2 = v.dk_vgf_ts_id)

    
	-- Main DTD PNL Query
	SELECT DKG_TDP_ID [ID],DKG_TDP_DATE_FROM AsOfDate,
		DKG_TDP_ULTIMATE_MANAGER Manager,
		DKG_TDP_ULTIMATE_DEAL_CODE Deal,
		DKG_TDP_ULTIMATE_FUND Fund,
		DKG_TDP_ALT_ID2 AltId2,
		DKG_TDP_PRINT_GROUP SecurityType,
		ISNULL(DK_VGF_NB_GENEVA_PRICING_FACTOR, 0.01) PricingFactor,
		ISNULL(DK_VGF_NB_GENEVA_UNIT_FACTOR, 1) UnitFactor,
		SUM(DKG_TDP_QUANTITY_END) AS Quantity,
		AVG(CASE WHEN DKG_TDP_PRICE_END = 0.0 THEN NULL ELSE DKG_TDP_PRICE_END END) AS Price,
		AVG(CASE WHEN DKG_TDP_FXRATE_END = 0.0 THEN NULL ELSE DKG_TDP_FXRATE_END END) AS FxRate,
		SUM(DKG_TDP_TOTAL_INCOME_BASE) AS PnLBase,
		SUM(DKG_TDP_TOTAL_INCOME_LOCAL * ISNULL(DKG_TDP_FXRATE_END, ISNULL(DKG_TDP_FXRATE_BEGIN, 1.0))) AS PnLLocal,
		CONVERT(MONEY, 0.0) AS FxFwdPnLBase,
		CONVERT(MONEY, 0.0) AS FxFwdPnLLocal,
		SUM(DKG_TDP_COUPON) AS InterestBase,
		SUM(DKG_TDP_COUPON * ISNULL(DKG_TDP_FXRATE_END, ISNULL(DKG_TDP_FXRATE_BEGIN, 1.0))) AS InterestLocal,
		SUM(DKG_TDP_DIVIDEND) AS DividendBase ,
		SUM(DKG_TDP_DIVIDEND * ISNULL(DKG_TDP_FXRATE_END, ISNULL(DKG_TDP_FXRATE_BEGIN, 1.0))) AS DividendLocal,
		SUM(DKG_TDP_MARKET_VALUE) As MarketValue,
		SUM(DKG_TDP_EXPOSURE_BASE) AS InvestedCapitalBase,
		SUM(DKG_TDP_EXPOSURE_LOCAL * ISNULL(DKG_TDP_FXRATE_END, ISNULL(DKG_TDP_FXRATE_BEGIN, 1.0))) AS InvestedCapitalLocal,
		SUM(ISNULL(DKG_TDP_EXPOSURE_BASE * (f.total_funded + f.total_unfunded * .1) / f.total_commitment, DKG_TDP_EXPOSURE_BASE)) AS AdjInvestedCapitalBase,
		SUM(ISNULL(DKG_TDP_EXPOSURE_BASE * (f.total_funded + f.total_unfunded * .1) / f.total_commitment, DKG_TDP_EXPOSURE_BASE)) AS AdjInvestedCapitalLocal -- eventually conver to local currency?
	INTO #tmp_PositionLevelDetail
	FROM    #tmp_dkg_tbl_adidas_dtd_pnl_final f--[DK_DB_ADIDAS].[dbo].[DKG_TBL_ADIDAS_DTD_PNL_FINAL] F WITH (NOLOCK,FORCESEEK)
	        --JOIN #FundFilter  ON DKG_TDP_VEHICLE_CODE = Fund
	        JOIN #tmp_dk_vw_geneva_flatview DK_VW_GENEVA_FLATVIEW --DK_DB_SECMASTER.dbo.DK_VW_GENEVA_FLATVIEW
		        ON DKG_TDP_ALT_ID2 = DK_VGF_TS_ID
                --left JOIN #CreditContract C ON
		--F.DKG_TDP_DATE_TO = C.DK_TCCD_PERIOD_END_DATE
		--AND F.DKG_TDP_VEHICLE_CODE = C.DK_TCCD_PORTFOLIO
		--AND F.DKG_TDP_ALT_ID2 = C.DK_TCCD_INVESTMENT_ID
		--AND C.total_commitment <> 0
	WHERE   DKG_TDP_DATE_FROM BETWEEN @startdate and @enddate
		AND DKG_TDP_ALT_ID2 NOT IN (SELECT AltId FROM #SecurityFilter)
	GROUP   BY
                DKG_TDP_DATE_FROM               ,
		DKG_TDP_ULTIMATE_MANAGER        ,
		DKG_TDP_ULTIMATE_DEAL_CODE      ,
		DKG_TDP_ULTIMATE_FUND           ,
		DKG_TDP_ALT_ID2                 ,
		DKG_TDP_PRINT_GROUP             ,
		DK_VGF_NB_GENEVA_PRICING_FACTOR ,
		DK_VGF_NB_GENEVA_UNIT_FACTOR    ,
		DKG_TDP_ID



/**********************************************************************************
**
**********************************************************************************/   
	-- Update the Invested Capital to 0.0 for the Cash/Currency/Currency Derivatives securities
	-- Update FxFwdPnL for the Cash/Currency/Currency Derivatives securities
	UPDATE  pd 
	SET     MarketValue = 0.0,
		AdjInvestedCapitalBase = 0.0,
		AdjInvestedCapitalLocal = 0.0,
		FxFwdPnLBase = PnLBase,
		FxFwdPnLLocal = PnLLocal
	FROM    #tmp_PositionLevelDetail pd 
	WHERE   SecurityType IN (SELECT SecurityType FROM #SecurityTypesZeroExposure)

	-- For CDS, calculate bond equiv. market value
	UPDATE  pd
	SET	AdjInvestedCapitalBase = ISNULL(-1.0 * Quantity * UnitFactor * (1 / PricingFactor - Price) * FxRate * PricingFactor, 0),
		AdjInvestedCapitalLocal = ISNULL(-1.0 * Quantity * UnitFactor * (1 / PricingFactor - Price) * FxRate * PricingFactor, 0)
	FROM    #tmp_PositionLevelDetail pd
	WHERE   SecurityType IN (SELECT SecurityType FROM #SecurityTypesCDS)


/**********************************************************************************
**
**********************************************************************************/   
        select  d.dk_vod_altid2,
                d.dk_vod_delta_mid_risk,
                d.dk_vod_date
        into    #tmp_dk_vw_opt_delta
        from    dk_db_history.dbo.dk_vw_opt_delta d
        where   exists(select 1 from #tmp_positionleveldetail pld where pld.altid2 = d.dk_vod_altid2 and  pld.AsOfDate = d.dk_vod_date)

        select  d.dk_voup_altid2,
                d.dk_voup_price,
                d.dk_voup_date
        into    #tmp_dk_vw_opt_undl_price
        from    dk_db_history.dbo.dk_vw_opt_undl_price d
        where   exists(select 1 from #tmp_positionleveldetail pld where pld.altid2 = d.dk_voup_altid2 and  pld.AsOfDate = d.dk_voup_date)


        update  pd
        set     pd.AdjInvestedCapitalBase = isnull(quantity * unitfactor * pricingfactor * fxrate * dk_vod_delta_mid_risk * dk_voup_price, 0),
                pd.adjinvestedcapitallocal = isnull(quantity * unitfactor * pricingfactor * fxrate * dk_vod_delta_mid_risk * dk_voup_price, 0)
        from    #tmp_positionleveldetail pd
                left join #tmp_dk_vw_opt_delta on altid2 = dk_vod_altid2
                        and asofdate = dk_vod_date
                left join #tmp_dk_vw_opt_undl_price on altid2 = dk_voup_altid2
                        and asofdate = dk_voup_date
        where   securitytype in (select securitytype from #securitytypesequityoptions)
                and     (
                        pd.AdjInvestedCapitalBase != isnull(quantity * unitfactor * pricingfactor * fxrate * dk_vod_delta_mid_risk * dk_voup_price, 0)
                        or
                        pd.adjinvestedcapitallocal != isnull(quantity * unitfactor * pricingfactor * fxrate * dk_vod_delta_mid_risk * dk_voup_price, 0)
                        )
/**********************************************************************************
** Final update
**********************************************************************************/   
	-- End of Options Section ---------------------------------------------------------------------------------------------
	
	-- Clean up & fill existing staging table
	----------DELETE FROM InputExposuresDetails WHERE AsOfDate BETWEEN @DateStart AND @DateEnd

		DECLARE @timeStamp DATETIME
		SELECT @timeStamp = GETDATE()

		select  dpnl.DKG_TDP_ADJINVESTEDCAPITALBASE     ,
                        pld.AdjInvestedCapitalBase              ,
		        dpnl.DKG_TDP_ADJINVESTEDCAPITALLOCAL    ,
                        pld.AdjInvestedCapitalLocal             ,
		        dpnl.DKG_TDP_FXFWDPNLBASE               ,
                        pld.FxFwdPnLBase                        ,
		        dpnl.DKG_TDP_FXFWDPNLLOCAL              ,
                        pld.FxFwdPnLLocal                       ,
		        dpnl.DKG_TDP_MARKET_VALUE               ,
                        pld.MarketValue                         ,
                        dpnl.dkg_tdp_id
                into    #tmp_what_would_be_updated
		FROM    [DK_DB_ADIDAS].[dbo].[DKG_TBL_ADIDAS_DTD_PNL_FINAL] dpnl
		        JOIN #tmp_PositionLevelDetail pld ON DKG_TDP_ID =[ID]
                where   dpnl.DKG_TDP_ADJINVESTEDCAPITALBASE     != pld.AdjInvestedCapitalBase
		        or dpnl.DKG_TDP_ADJINVESTEDCAPITALLOCAL != pld.AdjInvestedCapitalLocal
		        or dpnl.DKG_TDP_FXFWDPNLBASE            != pld.FxFwdPnLBase
		        or dpnl.DKG_TDP_FXFWDPNLLOCAL           != pld.FxFwdPnLLocal
		        or dpnl.DKG_TDP_MARKET_VALUE            != pld.MarketValue
                
                --UPDATE dpnl
		--SET dpnl.DKG_TDP_ADJINVESTEDCAPITALBASE =pld.AdjInvestedCapitalBase,
		--dpnl.DKG_TDP_ADJINVESTEDCAPITALLOCAL =pld.AdjInvestedCapitalLocal,
		--dpnl.DKG_TDP_FXFWDPNLBASE =pld.FxFwdPnLBase,
		--dpnl.DKG_TDP_FXFWDPNLLOCAL=pld.FxFwdPnLLocal,
		--dpnl.DKG_TDP_MARKET_VALUE =pld.MarketValue
		--FROM [DK_DB_ADIDAS].[dbo].[DKG_TBL_ADIDAS_DTD_PNL_FINAL] dpnl
		--JOIN #tmp_PositionLevelDetail pld ON DKG_TDP_ID =[ID]

	--DROP TABLE #SecurityFilter
	--DROP TABLE #FundFilter
	--DROP TABLE #SecurityTypesZeroExposure
	--DROP TABLE #SecurityTypesEquityOptions
	--DROP TABLE #SecurityTypesCDS
	--DROP TABLE #tmp_PositionLevelDetail
	--DROP TABLE #DealLevelDetail

