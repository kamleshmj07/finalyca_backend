
/**********
STEP 1

**********/

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

--EXEC sp_rename 'Masters.HoldingSecurity.Co_Code', 'Vendor_Code', 'COLUMN';

ALTER TABLE [Masters].[HoldingSecurity]
ALTER COLUMN Co_Code varchar(50);
GO


CREATE NONCLUSTERED INDEX [IDX_HoldingSecurity_Co_Code] ON [Masters].[HoldingSecurity]
(
	[Co_Code] ASC
)
INCLUDE([Is_Deleted],[BSE_Code],[NSE_Symbol]) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [idx_HoldingSecurity_isdeleted_co_code] ON [Masters].[HoldingSecurity]
(
	[Is_Deleted] ASC,
	[Co_Code] ASC
)
INCLUDE([BSE_Code],[NSE_Symbol]) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [idx_holdingsecurity_isin_code_is_deleted] ON [Masters].[HoldingSecurity]
(
	[ISIN_Code] ASC,
	[Is_Deleted] ASC
)
INCLUDE([Co_Code]) ON [PRIMARY]
GO



/**********
STEP 2

**********/
USE [PMS_Base]
GO

/****** Object:  Table [Masters].[DebtSecurity] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Masters].[DebtSecurity]') AND type in (N'U'))
DROP TABLE [Masters].[DebtSecurity]
GO

CREATE TABLE [Masters].[DebtSecurity](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	DebtSecurity_Id	int NOT NULL UNIQUE,
	Security_Name varchar(100) NOT NULL,
	ISIN varchar(12) NOT NULL,
	Exchange_1 varchar(6) NULL,
	Exchange_1_Local_Code varchar(50) NULL,
	Exchange_2 varchar(6),
	Exchange_2_Local_Code varchar(50),
	Exchange_3 varchar(6),
	Exchange_3_Local_Code varchar(50),
	Security_Type varchar(100) NOT NULL,
	Bond_Type_Code varchar(20) NOT NULL,
	Bond_Type	varchar(100) NOT NULL,
	Country varchar(2) NOT NULL,
	Bilav_Internal_Issuer_Id int NOT NULL,
	Bilav_Code varchar(50) NOT NULL,
	LEI	varchar(25),
	CIN	varchar(25),
	Issuer varchar(70) NOT NULL,
	Issue_Price	numeric(16,6),
	Issue_Date date,
	Maturity_Price numeric(16,6) NOT NULL,
	Maturity_Based_On varchar(10),
	Maturity_Benchmark_Index varchar(100),
	Maturity_Price_As_Perc numeric(16,6) NOT NULL,
	Is_Perpetual varchar(5),
	On_Tap_Indicator bit,
	Deemed_Allotment_Date date,
	Coupon_Type_Code varchar(10) NOT NULL,
	Coupon_Type varchar(70) NOT NULL,
	Interest_Payment_Frequency_Code varchar(10),
	Interest_Payment_Frequency varchar(70),
	Interest_Payout_1 date,
	Is_Cumulative bit,
	Compounding_Frequency_Code varchar(10),
	Compounding_Frequency varchar(70),
	Interest_Accrual_Convention_Code varchar(10),
	Interest_Accrual_Convention varchar(70),
	Min_Investment_Amount numeric(16,6),
	FRN_Index_Benchmark	varchar(10),
	FRN_Index_Benchmark_Desc varchar(100),
	Interest_Pay_Date_1	varchar(4),
	Interest_Pay_Date_2	varchar(4),
	Interest_Pay_Date_3	varchar(4),
	Interest_Pay_Date_4	varchar(4),
	Interest_Pay_Date_5	varchar(4),
	Interest_Pay_Date_6	varchar(4),
	Interest_Pay_Date_7	varchar(4),
	Interest_Pay_Date_8	varchar(4),
	Interest_Pay_Date_9	varchar(4),
	Interest_Pay_Date_10 varchar(4),
	Interest_Pay_Date_11 varchar(4),
	Interest_Pay_Date_12 varchar(4),
	Issuer_Type_Code varchar(10) NOT NULL,
	Issuer_Type	varchar(50) NOT NULL,
	Issue_Size numeric(36,6),
	Outstanding_Amount numeric(16,6),
	Outstanding_Amount_Date	date,
	Yield_At_Issue numeric(16,6),
	Maturity_Structure_Code	varchar(20) NOT NULL,
	Maturity_Structure varchar(50) NOT NULL,
	Convention_Method_Code varchar(10),
	Convention_Method varchar(10),
	Interest_BDC_Code varchar(10),
	Interest_BDC varchar(100),
	Is_Variable_Interest_Payment_Date bit NOT NULL,
	Interest_Commencement_Date date,
	Coupon_Cut_Off_Days	int,
	Coupon_Cut_Off_Day_Convention varchar(10),
	FRN_Type varchar(10),
	FRN_Interest_Adjustment_Frequency varchar(10),
	Markup numeric(16,6),
	Minimum_Interest_Rate numeric(16,6),
	Maximum_Interest_Rate numeric(16,6),
	Is_Guaranteed bit,
	Is_Secured bit,
	Security_Charge	varchar(10),
	Security_Collateral	bit,
	Tier int,
	Is_Upper bit,
	Is_Sub_Ordinate	bit,
	Is_Senior varchar(50),
	Is_Callable	bit NOT NULL,
	Is_Puttable	bit NOT NULL,
	Strip varchar(50),
	Is_Taxable bit,
	Latest_Applied_INTPY_Annual_Coupon_Rate	numeric(16,6),
	Latest_Applied_INTPY_Annual_Coupon_Rate_Date date,
	Bond_Notes nvarchar(max),
	End_Use	nvarchar(max),
	Initial_Fixing_Date	date,
	Initial_Fixing_Level varchar(100),
	Final_Fixing_Date date,
	Final_Fixing_Level varchar(100),
	PayOff_Condition varchar(100),
	Majority_Anchor_Investor varchar(100),
	Security_Cover_Ratio numeric(16,6),
	Margin_TopUp_Trigger varchar(100),
	Current_Yield numeric(16,6),
	Security_Presentation_Link varchar(500),
	Coupon_Reset_Event varchar(100),
	[MES_Code] [varchar](100),
	[Macro_Economic_Sector] [varchar](200),
	[Sect_Code] [varchar](100),
	[Sector] [varchar](200),
	[Ind_Code] [varchar](100),
	[Industry] [varchar](200),
	[Basic_Ind_Code] [varchar](100),
	[Basic_Industry] [varchar](200),
	[Is_Deleted] [bit] NOT NULL,
	[Created_By] [bigint] NOT NULL,
	[Created_Date] [datetime] NOT NULL,
	[Updated_By] [bigint],
	[Updated_Date] [datetime]
	PRIMARY KEY CLUSTERED 
	(
		[Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO


USE [PMS_Base]
GO

/****** Object:  Table [Masters].[DebtCreditRating] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Masters].[DebtCreditRating]') AND type in (N'U'))
DROP TABLE [Masters].[DebtCreditRating]
GO

CREATE TABLE [Masters].[DebtCreditRating](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[DebtCreditRating_Id] [bigint] NOT NULL,
	[DebtSecurity_Id] [int] NOT NULL,
	ISIN varchar(12) NOT NULL,
	Rating_Agency varchar(100) NOT NULL,
	Rating_Date	date NOT NULL,
	Rating_Symbol varchar(10) NOT NULL,
	Rating_Direction_Code varchar(10),
	Rating_Direction varchar(70),
	Watch_Flag_Code	varchar(10),
	Watch_Flag varchar(70),
	Watch_Flag_Reason_Code varchar(10),
	Watch_Flag_Reason varchar(70),
	Rating_Prefix varchar(10),
	Prefix_Description varchar(70),
	Rating_Suffix varchar(10),
	Suffix_Description varchar(70),
	Rating_Outlook_Description varchar(70),
	Expected_Loss varchar(10),
	[AsofDate] date NOT NULL,
	[Is_Deleted] [bit] NOT NULL,
	[Created_By] [bigint] NOT NULL,
	[Created_Date] [datetime] NOT NULL,
	[Updated_By] [bigint] NULL,
	[Updated_Date] [datetime] NULL
	PRIMARY KEY CLUSTERED 
	(
		[Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [Masters].[DebtCreditRating] WITH CHECK ADD FOREIGN KEY([DebtSecurity_Id])
REFERENCES [Masters].[DebtSecurity] ([DebtSecurity_Id])
GO


USE [PMS_Base]
GO

/****** Object:  Table [Masters].[DebtCallOption] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Masters].[DebtCallOption]') AND type in (N'U'))
DROP TABLE [Masters].[DebtCallOption]
GO

CREATE TABLE [Masters].[DebtCallOption](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[DebtCallOption_Id] [bigint] NOT NULL UNIQUE,
	[DebtSecurity_Id] [int] NULL,
	ISIN varchar(12) NOT NULL,
	Call_Type_Code varchar(10),
	Call_Type varchar(70),
	[From_Date] date,
	[To_Date] date,
	Notice_From_Date date,
	Notice_To_Date date,
	Min_Notice_Days int,
	Max_Notice_Days	int,
	Currency varchar(3),
	Call_Price numeric(16,6),
	Call_Price_As_Perc numeric(12,6),
	Is_Formulae_Based bit,
	Is_Mandatory_Call bit,
	Is_Part_Call bit,
	[Is_Deleted] [bit] NOT NULL,
	[Created_By] [bigint] NOT NULL,
	[Created_Date] [datetime] NOT NULL,
	[Updated_By] [bigint] NULL,
	[Updated_Date] [datetime] NULL
	PRIMARY KEY CLUSTERED
	(
		[Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [Masters].[DebtCallOption] WITH CHECK ADD FOREIGN KEY([DebtSecurity_Id])
REFERENCES [Masters].[DebtSecurity] ([DebtSecurity_Id])
GO


USE [PMS_Base]
GO

/****** Object:  Table [Masters].[DebtPutOption] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Masters].[DebtPutOption]') AND type in (N'U'))
DROP TABLE [Masters].[DebtPutOption]
GO

CREATE TABLE [Masters].[DebtPutOption](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[DebtPutOption_Id] [bigint] NOT NULL UNIQUE,
	[DebtSecurity_Id] [int] NULL,
	ISIN varchar(12) NOT NULL,
	Put_Type_Code varchar(10),
	Put_Type varchar(70),
	[From_Date] date,
	[To_Date] date,
	Notice_From_Date date,
	Notice_To_Date date,
	Min_Notice_Days int,
	Max_Notice_Days	int,
	Currency varchar(3),
	Put_Price numeric(16,6),
	Put_Price_As_Perc numeric(12,6),
	Is_Formulae_Based bit,
	Is_Mandatory_Put bit,
	Is_Part_Put bit,
	[Is_Deleted] [bit] NOT NULL,
	[Created_By] [bigint] NOT NULL,
	[Created_Date] [datetime] NOT NULL,
	[Updated_By] [bigint] NULL,
	[Updated_Date] [datetime] NULL
	PRIMARY KEY CLUSTERED
	(
		[Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [Masters].[DebtPutOption] WITH CHECK ADD FOREIGN KEY([DebtSecurity_Id])
REFERENCES [Masters].[DebtSecurity] ([DebtSecurity_Id])
GO


USE [PMS_Base]
GO

/****** Object:  Table [Masters].[DebtRedemption] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Masters].[DebtRedemption]') AND type in (N'U'))
DROP TABLE [Masters].[DebtRedemption]
GO


CREATE TABLE [Masters].[DebtRedemption](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[DebtRedemption_Id] [bigint] NOT NULL UNIQUE,
	[DebtSecurity_Id] int FOREIGN KEY REFERENCES [Masters].[DebtSecurity]([DebtSecurity_Id]),
	[ISIN]	varchar(12) NOT NULL,
	Redemption_Date	date NOT NULL,
	Redemption_Type_Code varchar(20) NOT NULL,
	Redemption_Type varchar(70) NOT NULL,
	Redemption_Currency varchar(3) NOT NULL,
	Redemption_Price numeric(16,6) NOT NULL,
	Redemption_Amount numeric(16,6),
	Redemption_Price_As_Perc numeric(12,8) NOT NULL,
	Redemption_Percentage numeric(12,9),
	Redemption_Premium	numeric(16,6),
	Redemption_Premium_As_Perc numeric(12,8),
	Is_Mandatory_Redemption	bit NOT NULL,
	Is_Part_Redemption bit NOT NULL,
	[Is_Deleted] [bit] NOT NULL,
	[Created_By] [bigint] NOT NULL,
	[Created_Date] [datetime] NOT NULL,
	[Updated_By] [bigint] NULL,
	[Updated_Date] [datetime] NULL
	PRIMARY KEY CLUSTERED 
	(
		[Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO


USE [PMS_Base]
GO

/****** Object:  Table [Transactions].[DebtPrice] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Transactions].[DebtPrice]') AND type in (N'U'))
DROP TABLE [Transactions].[DebtPrice]
GO

CREATE TABLE [Transactions].[DebtPrice](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[DebtPrice_Id] [bigint] NOT NULL,
	[DebtSecurity_Id] [int] NOT NULL,
	ISIN varchar(12) NOT NULL,
	[Trading_Date] date NOT NULL,
	[Exchange_Code] varchar(50) NOT NULL,
	Exchange varchar(50),
	Segment_Code varchar(50),
	Segment varchar(50),
	Local_Code varchar(50),
	No_Of_Trades bigint,
	Traded_Qty bigint,
	Traded_Value numeric(36,6),
	[Open] numeric(16,6),
	[High] numeric(16,6),
	[Low] numeric(16,6),
	[Close] numeric(16,6),
	Weighted_Avg_Price numeric(16,6),
	FaceValuePrice numeric(16,6) NOT NULL,
	Currency varchar(4),
	WYTM numeric(16,6),
	TT_Status varchar(100),
	Trade_Type varchar(100),
	Settlement_Type	varchar(100),
	Residual_Maturity_Date date,
	Residual_Maturity_Derived_From varchar(100),
	Clean_Dirty_Indicator varchar(4),
	Dirty_Price	numeric(16,6),
	[AsofDate] date NOT NULL,
	[Is_Deleted] [bit] NOT NULL,
	[Created_By] [bigint] NOT NULL,
	[Created_Date] [datetime] NOT NULL,
	[Updated_By] [bigint],
	[Updated_Date] [datetime]
	PRIMARY KEY CLUSTERED
	(
		[Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [Transactions].[DebtPrice] WITH CHECK ADD FOREIGN KEY([DebtSecurity_Id])
REFERENCES [Masters].[DebtSecurity] ([DebtSecurity_Id])
GO

ALTER TABLE [Masters].[RecentlyViewed]
ADD table_obj nvarchar(max)
GO

/**********
STEP 3

**********/


USE [ServiceManager]
GO

/*
select *
from [Upload].[UploadTemplates]
*/

INSERT INTO [Upload].[UploadTemplates] ([UploadTemplates_Name],[Template_Description],[Parameters],[Status],[Is_Deleted],[Enabled_Python])
	VALUES ('Bilav - Debt Master Upload'
           ,'Security Master data with credit, call, put, redemption data'
           ,'<Parameters></Parameters>'
           ,0
           ,0
           ,1),
		   ('Bilav - Debt Price Upload'
           ,'Pricing debt instruments data'
           ,'<Parameters></Parameters>'
           ,0
           ,0
           ,1);
GO


INSERT
INTO [Reporting].[ReportJobs] ([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],
							   [Status],[Status_Message],[Last_Run],[Schedule_Id],[Channel_Id],[Enabled_Python])
VALUES ('Bilav - Debt Secmaster',
		'Debt master and credit rating, redemption, call put',
		3,
		'',
		'<Parameters><ImportType>DirectUpload</ImportType><UploadPath>E:\Finalyca\ServiceManager\Imports\</UploadPath><FileName>FINA_202311291509.txt</FileName><DaysCount>-1</DaysCount></Parameters>',
		0,
		2,
		'Success',
		getdate()-3,
		4,
		2,
		1),
		('Bilav - Debt Price',
		'Debt pricing data',
		3,
		'',
		'<Parameters><ImportType>DirectUpload</ImportType><UploadPath>E:\Finalyca\ServiceManager\Imports\</UploadPath><FileName>FIPR231121.csv</FileName><DaysCount>-1</DaysCount></Parameters>',
		0,
		3,
		'Success_',
		getdate()-3,
		4,
		2,
		1)
GO

