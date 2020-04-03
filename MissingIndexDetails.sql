use dk_db_adidas
go
/************************************************************************
** fd_ix_dkg_tbl_credit_contract_details_dk_tccd_invest
************************************************************************/
create  index fd_ix_dkg_tbl_credit_contract_details_dk_tccd_invest
on      dbo.dkg_tbl_credit_contract_details
        (
        dk_tccd_invest          ,
        dk_tccd_period_end_date
        )
include (
        dk_tccd_globalamt       ,
        dk_tccd_portfolio       ,
        dk_tccd_investment_id
        )
with    (
        data_compression        = page
        )
