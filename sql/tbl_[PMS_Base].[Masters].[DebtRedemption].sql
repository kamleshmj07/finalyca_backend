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

