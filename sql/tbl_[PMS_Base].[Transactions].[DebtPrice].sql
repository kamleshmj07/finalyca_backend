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

