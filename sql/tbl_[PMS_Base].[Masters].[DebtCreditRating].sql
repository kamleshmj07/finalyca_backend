USE [PMS_Base]
GO

/****** Object:  Table [Masters].[DebtCreditRating] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Masters].[DebtCreditRating]') AND type in (N'U'))
DROP TABLE [Masters].[DebtCreditRating]
GO

CREATE TABLE [Masters].[DebtCreditRating](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[DebtCreditRating_Id] [bigint] NOT NULL UNIQUE,
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

