USE [PMS_Base]
GO

/****** Object:  Table [Transactions].[NAV]    Script Date: 13-01-2023 11:28:25 ******/

CREATE TABLE [Transactions].[NAV](
	[NAV_Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Plan_Id] [bigint] NULL,
	[NAV_Date] [date] NULL,
	[NAV] [float] NULL,
	[NAV_PortfolioReturn] [numeric](18, 2) NULL,
	[Is_Deleted] [bit] NULL,
	[NAV_Type] [nvarchar](1) NULL,
	[Created_By] [bigint] NULL,
	[Created_Date] [datetime] NULL,
	[Updated_By] [bigint] NULL,
	[Updated_Date] [datetime] NULL,
	[is_locked] [bit] NULL,
	PRIMARY KEY CLUSTERED 
	(
		[NAV_Id] ASC
	)
) ON [PRIMARY]
GO

ALTER TABLE [Transactions].[NAV] ADD  DEFAULT ((0)) FOR [is_locked]
GO

ALTER TABLE [Transactions].[NAV] ADD [RAW_NAV] decimal(24,12) NULL
GO




