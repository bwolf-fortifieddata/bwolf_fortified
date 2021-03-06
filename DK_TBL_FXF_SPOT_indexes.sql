
CREATE  INDEX [DK_TBL_FXF_SPOT_extract_id]
ON      [dbo].[DK_TBL_FXF_SPOT] ([DK_TFS_EXTRACT_ID],[DK_TFS_CUST_ACCT],[DK_TFS_STATUS],[DK_TFS_IS_SENT])
INCLUDE ([DK_TFS_TKT],[DK_TFS_PORTFOLIO],[DK_TFS_RECORD_ID],[DK_TFS_TRADE_NO],[DK_TFS_TRADE_TYPE],[DK_TFS_TRADE_TYPE2],[DK_TFS_T_DATE],[DK_TFS_TRADE_TIME],[DK_TFS_SETTLE_DATE],[DK_TFS_ACCOUNT_NO],[DK_TFS_CCY],[DK_TFS_SETTLE_CCY],[DK_TFS_DENOM_CCY],[DK_TFS_TXN_TYPE],[DK_TFS_DENOM_AMT],[DK_TFS_PRICE],[DK_TFS_TRADE_AMT],[DK_TFS_ALLOC_AMT],[DK_TFS_UNALLOC_AMT],[DK_TFS_NOTES],[DK_TFS_CREATE_USER],[DK_TFS_CREATE_DATETIME],[DK_TFS_UPDATE_USER],[DK_TFS_UPDATE_DATETIME],[DK_TFS_ULT_ACCOUNT_NO],[DK_TFS_IS_DELTA_TRADE],[DK_TFS_GENEVA_MANAGER_CODE],[DK_TFS_TRADE_ID],[DK_TFS_DEAL_DESC],[DK_TFS_NET_SETTLE_AMT],[DK_TFS_FX_RATE],[DK_TFS_TRANS_INSTR],[DK_TFS_COMMISSION],[DK_TFS_FEES],[DK_TFS_CPTY])
with    (data_compression = page)


CREATE  INDEX [DK_TBL_FXF_SPOT_DK_TFS_STATUS]
ON      [dbo].[DK_TBL_FXF_SPOT] ([DK_TFS_STATUS])
INCLUDE ([DK_TFS_ID],[DK_TFS_TKT],[DK_TFS_EXTRACT_ID])
with    (data_compression = page)