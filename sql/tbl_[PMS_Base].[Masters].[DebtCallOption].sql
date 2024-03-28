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

