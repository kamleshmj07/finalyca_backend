USE [PMS_Controller]
GO

/****** Object:  Table [Masters].[Newsletter_Subscribers] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Masters].[Newsletter_Subscribers]') AND type in (N'U'))
DROP TABLE [Masters].[Newsletter_Subscribers]
GO


CREATE TABLE [Masters].[Newsletter_Subscribers](
	[Subscriber_Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](50) NULL,
	[Organization_Name] [varchar](100) NULL,
	[Email_Address] [varchar](100) NULL,
	[Contact_Number] [varchar](20) NULL,
	[Is_Active] [bit] NOT NULL,
	[Created_By] [bigint] NOT NULL,
	[Created_Date] [datetime] NOT NULL,
	PRIMARY KEY CLUSTERED 
	(
		[Subscriber_Id] ASC
	)
) ON [PRIMARY]
GO


