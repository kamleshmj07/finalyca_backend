ALTER TABLE [PMS_Controller].[Transactions].[APILog] ADD entity_type NVARCHAR(10) NULL;

EXEC sp_rename 'Transactions.APILog.api_id', 'entity_id';