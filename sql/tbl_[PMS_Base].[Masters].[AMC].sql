USE [PMS_Base]
GO

/****** Object:  Table [Masters].[AMC] ******/


CREATE TABLE [Masters].[AMC] (
	[AMC_Id] [bigint] IDENTITY(1,1) NOT NULL,
	[AMC_Name] [nvarchar](100) NULL,
	[AMC_Code] [nvarchar](100) NULL,
	[Is_Deleted] [bit] NULL,
	[Created_By] [bigint] NULL,
	[Created_Date] [datetime] NULL,
	[Updated_By] [bigint] NULL,
	[Updated_Date] [datetime] NULL,
	[AMC_Description] [nvarchar](max) NULL,
	[AMC_Logo] [nvarchar](500) NULL,
	[Product_Id] [bigint] NULL,
	[Address1] [nvarchar](500) NULL,
	[Address2] [nvarchar](500) NULL,
	[Website_link] [nvarchar](250) NULL,
	[Contact_Numbers] [nvarchar](100) NULL,
	[AMC_background] [nvarchar](500) NULL,
	[Corporate_Identification_Number] [nvarchar](100) NULL,
	[SEBI_Registration_Number] [nvarchar](100) NULL,
	[Contact_Person] [nvarchar](100) NULL,
	[Email_Id] [varchar](256) NULL,
	[hide_fields] [varchar](500) NULL,
	PRIMARY KEY CLUSTERED 
	(
		[AMC_Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [Masters].[AMC] ADD  CONSTRAINT [df_is_deleted]  DEFAULT ((0)) FOR [Is_Deleted]
GO

ALTER TABLE [Masters].[AMC] ADD hide_fields varchar(500)
GO

