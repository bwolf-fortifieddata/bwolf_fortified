-- Dropping the snapshot subscriptions
use [CRE]
exec sp_dropsubscription @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @subscriber = N'NJ1PRDDB11', @destination_db = N'CRE', @article = N'all'
GO

-- Dropping the snapshot articles
use [CRE]
exec sp_dropsubscription @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_business_plan_workout', @subscriber = N'all', @destination_db = N'all'
GO
use [CRE]
exec sp_droparticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_business_plan_workout', @force_invalidate_snapshot = 1
GO
use [CRE]
exec sp_dropsubscription @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_capital_stack', @subscriber = N'all', @destination_db = N'all'
GO
use [CRE]
exec sp_droparticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_capital_stack', @force_invalidate_snapshot = 1
GO
use [CRE]
exec sp_dropsubscription @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_creation_metric', @subscriber = N'all', @destination_db = N'all'
GO
use [CRE]
exec sp_droparticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_creation_metric', @force_invalidate_snapshot = 1
GO
use [CRE]
exec sp_dropsubscription @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_deal', @subscriber = N'all', @destination_db = N'all'
GO
use [CRE]
exec sp_droparticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_deal', @force_invalidate_snapshot = 1
GO
use [CRE]
exec sp_dropsubscription @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_deal_checklist', @subscriber = N'all', @destination_db = N'all'
GO
use [CRE]
exec sp_droparticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_deal_checklist', @force_invalidate_snapshot = 1
GO
use [CRE]
exec sp_dropsubscription @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_projection', @subscriber = N'all', @destination_db = N'all'
GO
use [CRE]
exec sp_droparticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_projection', @force_invalidate_snapshot = 1
GO

-- Dropping the snapshot publication
use [CRE]
exec sp_droppublication @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1'
GO



-- Enabling the replication database
use master
exec sp_replicationdboption @dbname = N'CRE', @optname = N'publish', @value = N'true'
GO

-- Adding the snapshot publication
use [CRE]
exec sp_addpublication @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @description = N'Snapshot publication of database ''CRE'' from Publisher ''NJ1PRDDB14''.', @sync_method = N'native', @retention = 0, @allow_push = N'true', @allow_pull = N'true', @allow_anonymous = N'true', @enabled_for_internet = N'false', @snapshot_in_defaultfolder = N'false', @alt_snapshot_folder = N'\\NJ1PRDDISTDB02\F$\Repldata', @compress_snapshot = N'false', @ftp_port = 21, @ftp_login = N'anonymous', @allow_subscription_copy = N'false', @add_to_active_directory = N'false', @repl_freq = N'snapshot', @status = N'active', @independent_agent = N'true', @immediate_sync = N'true', @allow_sync_tran = N'false', @autogen_sync_procs = N'false', @allow_queued_tran = N'false', @allow_dts = N'false', @replicate_ddl = 1
GO


exec sp_addpublication_snapshot @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @frequency_type = 4, @frequency_interval = 1, @frequency_relative_interval = 0, @frequency_recurrence_factor = 0, @frequency_subday = 1, @frequency_subday_interval = 0, @active_start_time_of_day = 200500, @active_end_time_of_day = 235959, @active_start_date = 0, @active_end_date = 0, @job_login = null, @job_password = null, @publisher_security_mode = 1
exec sp_grant_publication_access @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @login = N'sa'
GO
exec sp_grant_publication_access @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @login = N'dkp\Svc_SW_DPA'
GO
exec sp_grant_publication_access @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @login = N'DKP\DKDBACintra'
GO
exec sp_grant_publication_access @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @login = N'DKP\DB_PRD_Admins'
GO
exec sp_grant_publication_access @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @login = N'NT SERVICE\Winmgmt'
GO
exec sp_grant_publication_access @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @login = N'NT SERVICE\SQLWriter'
GO
exec sp_grant_publication_access @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @login = N'NT SERVICE\SQLSERVERAGENT'
GO
exec sp_grant_publication_access @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @login = N'NT SERVICE\MSSQLSERVER'
GO

-- Adding the snapshot articles
use [CRE]
exec sp_addarticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_business_plan_workout', @source_owner = N'pm', @source_object = N'v_business_plan_workout', @type = N'indexed view logbased', @description = N'', @creation_script = N'', @pre_creation_cmd = N'drop', @schema_option = 0x0000000008000001, @destination_table = N'v_business_plan_workout', @destination_owner = N'pm', @status = 16
GO
use [CRE]
exec sp_addarticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_capital_stack', @source_owner = N'pm', @source_object = N'v_capital_stack', @type = N'indexed view logbased', @description = N'', @creation_script = N'', @pre_creation_cmd = N'drop', @schema_option = 0x0000000008000001, @destination_table = N'v_capital_stack', @destination_owner = N'pm', @status = 16
GO
use [CRE]
exec sp_addarticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_creation_metric', @source_owner = N'pm', @source_object = N'v_creation_metric', @type = N'indexed view logbased', @description = N'', @creation_script = N'', @pre_creation_cmd = N'drop', @schema_option = 0x0000000008000001, @destination_table = N'v_creation_metric', @destination_owner = N'pm', @status = 16
GO
use [CRE]
exec sp_addarticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_deal', @source_owner = N'pm', @source_object = N'v_deal', @type = N'indexed view logbased', @description = N'', @creation_script = N'', @pre_creation_cmd = N'drop', @schema_option = 0x0000000008000001, @destination_table = N'v_deal', @destination_owner = N'pm', @status = 16
GO
use [CRE]
exec sp_addarticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_deal_checklist', @source_owner = N'pm', @source_object = N'v_deal_checklist', @type = N'indexed view logbased', @description = N'', @creation_script = N'', @pre_creation_cmd = N'drop', @schema_option = 0x0000000008000001, @destination_table = N'v_deal_checklist', @destination_owner = N'pm', @status = 16
GO
use [CRE]
exec sp_addarticle @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @article = N'v_projection', @source_owner = N'pm', @source_object = N'v_projection', @type = N'indexed view logbased', @description = N'', @creation_script = N'', @pre_creation_cmd = N'drop', @schema_option = 0x0000000008000001, @destination_table = N'v_projection', @destination_owner = N'pm', @status = 16
GO

-- Adding the snapshot subscriptions
use [CRE]
exec sp_addsubscription @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @subscriber = N'NJ1PRDDB11', @destination_db = N'CRE', @subscription_type = N'Push', @sync_type = N'automatic', @article = N'all', @update_mode = N'read only', @subscriber_type = 0
exec sp_addpushsubscription_agent @publication = N'RiskMgmt__CRE_PMSCHEMA_CODE1', @subscriber = N'NJ1PRDDB11', @subscriber_db = N'CRE', @job_login = null, @job_password = null, @subscriber_security_mode = 1, @frequency_type = 64, @frequency_interval = 1, @frequency_relative_interval = 1, @frequency_recurrence_factor = 0, @frequency_subday = 4, @frequency_subday_interval = 5, @active_start_time_of_day = 0, @active_end_time_of_day = 235959, @active_start_date = 0, @active_end_date = 0, @dts_package_location = N'Distributor'
GO

