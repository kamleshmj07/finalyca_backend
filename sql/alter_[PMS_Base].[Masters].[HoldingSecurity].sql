USE [PMS_Base]
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Currency] varchar(4)
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD  DEFAULT ('INR') FOR [Currency]
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Maturity_Date] date
GO

-- ALTER TABLE [Masters].[HoldingSecurity]
-- ADD [Credit_Ratings] varchar(20)
-- GO

-- ALTER TABLE [Masters].[HoldingSecurity]
-- ADD [Credit_Ratings_Agency] varchar(200)
-- GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Interest_Rate] numeric(16,6)
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Is_Listed] bit
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Face_Value] numeric(16,6)
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Paid_Up_Value] numeric(16,6)
GO

/*
sp_help 'Masters.HoldingSecurity'
*/

DROP INDEX [IDX_HoldingSecurity_Co_Code] ON [Masters].[HoldingSecurity]
GO

DROP INDEX [idx_HoldingSecurity_isdeleted_co_code] ON [Masters].[HoldingSecurity]
GO

DROP INDEX [idx_holdingsecurity_isin_code_is_deleted] ON [Masters].[HoldingSecurity]
GO

DROP FUNCTION [dbo].[fn_getSecurity_NAV]
GO

DROP VIEW [Logics].[Index_Constituents]
GO

DROP VIEW [Logics].[UnderlyingHoldings]
GO

EXEC sp_rename 'Masters.HoldingSecurity.Co_Code', 'Vendor_Code', 'COLUMN';

ALTER TABLE [Masters].[HoldingSecurity]
ALTER COLUMN Vendor_Code varchar(50);
GO


CREATE NONCLUSTERED INDEX [idx_HoldingSecurity_Vendor_Code] ON [Masters].[HoldingSecurity]
(
	[Vendor_Code] ASC
)
INCLUDE([Is_Deleted],[BSE_Code],[NSE_Symbol]) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [idx_HoldingSecurity_isdeleted_Vendor_Code] ON [Masters].[HoldingSecurity]
(
	[Is_Deleted] ASC,
	[Vendor_Code] ASC
)
INCLUDE([BSE_Code],[NSE_Symbol]) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [idx_holdingsecurity_isin_code_is_deleted] ON [Masters].[HoldingSecurity]
(
	[ISIN_Code] ASC,
	[Is_Deleted] ASC
)
INCLUDE([Vendor_Code]) ON [PRIMARY]
GO





