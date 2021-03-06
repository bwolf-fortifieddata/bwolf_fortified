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

exec    ##tmp_DKG_SP_ADIDAS_UPDATE_DTDPNL_EXPOSURE_Original '2020-02-16', '2020-03-30', 'SYSTEM'
*/
create  or alter PROCEDURE ##tmp_DKG_SP_ADIDAS_UPDATE_DTDPNL_EXPOSURE_Original
	@StartDate DATE = NULL,
	@EndDate DATE = NULL,
	@User VARCHAR(128) = NULL
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	-- Variables declaration
    DECLARE @DateStart DATETIME
	DECLARE @DateEnd DATETIME

	-- StartDate = 2010-01-01 in case the parameter is not passed
	IF(@StartDate IS NULL)
		SELECT @DateStart = '2010-01-01'
	ELSE 
		SELECT @DateStart = @StartDate

	-- EndDate = Max(RISK DTD PNL Date) in case the parameter is not passed
	IF(@EndDate IS NULL)
		-- TODO, replace with business date table
		SELECT @DateEnd = CONVERT(DATE, MAX(DKG_TDP_DATE_TO)) FROM [DK_DB_ADIDAS].[dbo].[DKG_TBL_ADIDAS_DTD_PNL_MASTER] WITH(NOLOCK)
	ELSE 
		SELECT @DateEnd = @EndDate

	-- All the SPV security ID that should be filtered
	
	SELECT AltID INTO #SecurityFilter FROM 	
	(SELECT AltID 
		FROM DK_DB_PNL_MONITOR.dbo.DK_VW_SELECT_FORMULA_SPV_INVESTMENTS WITH(NOLOCK)
		WHERE CAST(AltID AS VARCHAR) NOT IN (SELECT DK_IED_VALUE FROM DK_DB_REFERENCE..DKG_VW_INCLUSION_EXCLUSION_DETAILS
		WHERE DK_IED_TYPE='INVESTMENT' AND DK_IED_APPLICATION_CODE='DK_ADIDAS'
		AND DK_IED_MODULE_NAME='ADIDAS' AND DK_IED_INCLUDED=1)
		UNION
		SELECT DK_IED_VALUE FROM DK_DB_REFERENCE..DKG_VW_INCLUSION_EXCLUSION_DETAILS
		WHERE DK_IED_TYPE='INVESTMENT' AND DK_IED_APPLICATION_CODE='DK_ADIDAS'
		AND DK_IED_MODULE_NAME='ADIDAS' AND DK_IED_INCLUDED=0 
	) f


	-- All the Funds that should not be filtered
	SELECT DISTINCT DK_TFM_FUND_NAME  AS Fund INTO #FundFilter FROM DK_DB_REFERENCE.dbo.DK_TBL_FUND_MASTER WITH(NOLOCK)
	INNER JOIN DK_DB_REFERENCE.dbo.DK_TBL_APP_FUND_MAPPING WITH(NOLOCK) 
	ON DK_TFM_ID = DK_FK_TFM_TAFM_FUND_ID WHERE DK_FK_TFM_TAFM_APPLICATION_CODE = 'DK_COVP'   

	-- Security Types Temporary Tables 	
	CREATE TABLE #SecurityTypesZeroExposure (SecurityType varchar(50))
	CREATE TABLE #SecurityTypesEquityOptions (SecurityType varchar(50))
	CREATE TABLE #SecurityTypesCDS (SecurityType varchar(50))

	INSERT INTO #SecurityTypesZeroExposure VALUES ('Cash and Equivalents')
	INSERT INTO #SecurityTypesZeroExposure VALUES ('Currency')
	INSERT INTO #SecurityTypesZeroExposure VALUES ('Currency forward')
	INSERT INTO #SecurityTypesZeroExposure VALUES ('Currency futures option')

	INSERT INTO #SecurityTypesEquityOptions VALUES ('Exchange-traded equity option')
	INSERT INTO #SecurityTypesEquityOptions VALUES ('OTC equity option')

	INSERT INTO #SecurityTypesCDS VALUES ('Credit Default Swap')
	INSERT INTO #SecurityTypesCDS VALUES ('Credit Default Swap Index')
	INSERT INTO #SecurityTypesCDS VALUES ('CMBX')

	SELECT
		B.DK_TCCD_PERIOD_END_DATE
		,B.DK_TCCD_PORTFOLIO
		,B.DK_TCCD_INVESTMENT_ID
		,B.[Total Funded]
		,B.[Total UnFunded]
		,B.[Total Commitment]
	INTO #CreditContract
	FROM
	(
		SELECT
			C.DK_TCCD_PERIOD_END_DATE
			,C.DK_TCCD_PORTFOLIO
			,C.DK_TCCD_INVESTMENT_ID
			,C.DK_TCCD_INVEST
			,C.DK_TCCD_GLOBALAMT
		FROM DKG_TBL_CREDIT_CONTRACT_DETAILS C
		WHERE
			CAST(C.DK_TCCD_PERIOD_END_DATE AS DATE) BETWEEN @DateStart AND @DateEnd
			AND C.DK_TCCD_INVEST IN ('Total Funded', 'Total UnFunded', 'Total Commitment')
	) A
	PIVOT
	(
		SUM(DK_TCCD_GLOBALAMT)
		FOR DK_TCCD_INVEST IN ([Total Funded], [Total UnFunded], [Total Commitment])
	) B

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
		SUM(ISNULL(DKG_TDP_EXPOSURE_BASE * (C.[Total Funded] + C.[Total UnFunded] * .1) / C.[Total Commitment], DKG_TDP_EXPOSURE_BASE)) AS AdjInvestedCapitalBase,
		SUM(ISNULL(DKG_TDP_EXPOSURE_BASE * (C.[Total Funded] + C.[Total UnFunded] * .1) / C.[Total Commitment], DKG_TDP_EXPOSURE_BASE)) AS AdjInvestedCapitalLocal -- eventually conver to local currency?
	INTO #PositionLevelDetail
	FROM [DK_DB_ADIDAS].[dbo].[DKG_TBL_ADIDAS_DTD_PNL_FINAL] F WITH (NOLOCK,FORCESEEK)
	JOIN #FundFilter 
		ON DKG_TDP_VEHICLE_CODE = Fund
	JOIN DK_DB_SECMASTER.dbo.DK_VW_GENEVA_FLATVIEW
		ON DKG_TDP_ALT_ID2 = DK_VGF_TS_ID
	LEFT JOIN #CreditContract C ON
		F.DKG_TDP_DATE_TO = C.DK_TCCD_PERIOD_END_DATE
		AND F.DKG_TDP_VEHICLE_CODE = C.DK_TCCD_PORTFOLIO
		AND F.DKG_TDP_ALT_ID2 = C.DK_TCCD_INVESTMENT_ID
		AND C.[Total Commitment] <> 0
	WHERE DKG_TDP_DATE_FROM BETWEEN @DateStart AND @DateEnd
		AND DKG_TDP_ALT_ID2 NOT IN (SELECT AltId FROM #SecurityFilter)
	GROUP BY DKG_TDP_DATE_FROM,
		DKG_TDP_ULTIMATE_MANAGER,
		DKG_TDP_ULTIMATE_DEAL_CODE,
		DKG_TDP_ULTIMATE_FUND,
		DKG_TDP_ALT_ID2,
		DKG_TDP_PRINT_GROUP,
		DK_VGF_NB_GENEVA_PRICING_FACTOR,
		DK_VGF_NB_GENEVA_UNIT_FACTOR,
		DKG_TDP_ID


	-- Update the Invested Capital to 0.0 for the Cash/Currency/Currency Derivatives securities
	-- Update FxFwdPnL for the Cash/Currency/Currency Derivatives securities
	UPDATE pd 
	SET MarketValue = 0.0,
		AdjInvestedCapitalBase = 0.0,
		AdjInvestedCapitalLocal = 0.0,
		FxFwdPnLBase = PnLBase,
		FxFwdPnLLocal = PnLLocal
	FROM #PositionLevelDetail pd 
	WHERE SecurityType IN (SELECT SecurityType FROM #SecurityTypesZeroExposure)

	-- For CDS, calculate bond equiv. market value
	UPDATE pd
	SET	AdjInvestedCapitalBase = ISNULL(-1.0 * Quantity * UnitFactor * (1 / PricingFactor - Price) * FxRate * PricingFactor, 0),
		AdjInvestedCapitalLocal = ISNULL(-1.0 * Quantity * UnitFactor * (1 / PricingFactor - Price) * FxRate * PricingFactor, 0)
	FROM #PositionLevelDetail pd
	WHERE SecurityType IN (SELECT SecurityType FROM #SecurityTypesCDS)

	-- Options Section ---------------------------------------------------------------------------------------------
	-- For options, calculate the delta adjust market value
	-- Will implement logic to collect delta from all sources

	-- DK_DB_HISTORY.dbo.DK_VW_OPT_DELTA 
	-- Verified by Risk, backfill till 2013-01-02, no override for OTC prior 2014-01-01 

	-- DK_DB_APPS.dbo.DKG_TBL_LEVF_DATA_SOURCE 
	-- Not verified, date range 2011-11-30 till now 

	-- DK_DB_APPS.dbo.DKG_TBL_LEVD_DATA_SOURCE 
	-- Not verified, date range 2009-07-16 till 2015-01-29

	--UPDATE pd
	--SET	AdjInvestedCapitalBase = ISNULL(Quantity * UnitFactor * PricingFactor * FxRate * DK_VOD_DELTA_MID_RISK * DK_TP_PRICE, 0),
	--	AdjInvestedCapitalLocal = ISNULL(Quantity * UnitFactor * PricingFactor * FxRate * DK_VOD_DELTA_MID_RISK * DK_TP_PRICE, 0)
	--FROM #PositionLevelDetail pd
	--LEFT JOIN DK_DB_HISTORY.dbo.DK_VW_OPT_DELTA
	--	ON AltId2 = DK_VOD_ALTID2
	--	AND AsOfDate = DK_VOD_DATE
	--LEFT JOIN DK_DB_HISTORY.dbo.DK_TBL_BBG_UNDL_TICKER
	--	ON AltId2 = DK_TBUT_ALTID2
	--LEFT JOIN DK_DB_HISTORY.dbo.DK_TBL_PRICE
	--	ON AsOfDate = DK_TP_DATE
	--	AND DK_TBUT_UNDERLYING_TICKER = DK_TP_TICKER
	--WHERE SecurityType IN (SELECT SecurityType FROM #SecurityTypesEquityOptions)


		UPDATE pd
		SET    AdjInvestedCapitalBase = ISNULL(Quantity * UnitFactor * PricingFactor * FxRate * DK_VOD_DELTA_MID_RISK * DK_VOUP_PRICE, 0),
		AdjInvestedCapitalLocal = ISNULL(Quantity * UnitFactor * PricingFactor * FxRate * DK_VOD_DELTA_MID_RISK * DK_VOUP_PRICE, 0)
		FROM #PositionLevelDetail pd
		LEFT JOIN DK_DB_HISTORY.dbo.DK_VW_OPT_DELTA
		ON AltId2 = DK_VOD_ALTID2
		AND AsOfDate = DK_VOD_DATE
		LEFT JOIN DK_DB_HISTORY.dbo.DK_VW_OPT_UNDL_PRICE
		ON AltId2 = DK_VOUP_ALTID2
		AND AsOfDate = DK_VOUP_DATE
		WHERE SecurityType IN (SELECT SecurityType FROM #SecurityTypesEquityOptions)

	-- End of Options Section ---------------------------------------------------------------------------------------------
	
	-- Clean up & fill existing staging table
	----------DELETE FROM InputExposuresDetails WHERE AsOfDate BETWEEN @DateStart AND @DateEnd

		DECLARE @timeStamp DATETIME
		SELECT @timeStamp = GETDATE()


		select  dpnl.DKG_TDP_ADJINVESTEDCAPITALBASE ,pld.AdjInvestedCapitalBase,
		        dpnl.DKG_TDP_ADJINVESTEDCAPITALLOCAL ,pld.AdjInvestedCapitalLocal,
		        dpnl.DKG_TDP_FXFWDPNLBASE ,pld.FxFwdPnLBase,
		        dpnl.DKG_TDP_FXFWDPNLLOCAL,pld.FxFwdPnLLocal,
		        dpnl.DKG_TDP_MARKET_VALUE ,pld.MarketValue,
                        dpnl.dkg_tdp_id
                into    #tmp_what_would_be_updated
		FROM    [DK_DB_ADIDAS].[dbo].[DKG_TBL_ADIDAS_DTD_PNL_FINAL] dpnl
		        JOIN #PositionLevelDetail pld ON DKG_TDP_ID =[ID]
                where   dpnl.DKG_TDP_ADJINVESTEDCAPITALBASE != pld.AdjInvestedCapitalBase
		        or dpnl.DKG_TDP_ADJINVESTEDCAPITALLOCAL !=pld.AdjInvestedCapitalLocal
		        or dpnl.DKG_TDP_FXFWDPNLBASE !=pld.FxFwdPnLBase
		        or dpnl.DKG_TDP_FXFWDPNLLOCAL!=pld.FxFwdPnLLocal
		        or dpnl.DKG_TDP_MARKET_VALUE !=pld.MarketValue
                
		
		--UPDATE dpnl
		--SET dpnl.DKG_TDP_ADJINVESTEDCAPITALBASE =pld.AdjInvestedCapitalBase,
		--dpnl.DKG_TDP_ADJINVESTEDCAPITALLOCAL =pld.AdjInvestedCapitalLocal,
		--dpnl.DKG_TDP_FXFWDPNLBASE =pld.FxFwdPnLBase,
		--dpnl.DKG_TDP_FXFWDPNLLOCAL=pld.FxFwdPnLLocal,
		--dpnl.DKG_TDP_MARKET_VALUE =pld.MarketValue
		--FROM [DK_DB_ADIDAS].[dbo].[DKG_TBL_ADIDAS_DTD_PNL_FINAL] dpnl
		--JOIN #PositionLevelDetail pld ON DKG_TDP_ID =[ID]

	--DROP TABLE #SecurityFilter
	--DROP TABLE #FundFilter
	--DROP TABLE #SecurityTypesZeroExposure
	--DROP TABLE #SecurityTypesEquityOptions
	--DROP TABLE #SecurityTypesCDS
	--DROP TABLE #PositionLevelDetail
	--DROP TABLE #DealLevelDetail

END