USE [PMS_Controller]
GO

/****** Object:  Table [Masters].[User] ******/


CREATE TABLE [Masters].[User](
	[User_Id] [bigint] IDENTITY(1,1) NOT NULL,
	[User_Name] [nvarchar](50) NOT NULL,
	[Display_Name] [nvarchar](50) NULL,
	[Salutation] [nvarchar](5) NOT NULL,
	[First_Name] [nvarchar](20) NOT NULL,
	[Middle_Name] [nvarchar](20) NOT NULL,
	[Last_Name] [nvarchar](20) NOT NULL,
	[Gender] [tinyint] NOT NULL,
	[Marital_Status] [tinyint] NOT NULL,
	[Birth_Date] [date] NOT NULL,
	[Email_Address] [varchar](100) NULL,
	[Contact_Number] [varchar](20) NULL,
	[Login_Failed_Attempts] [tinyint] NOT NULL,
	[Is_Account_Locked] [bit] NOT NULL,
	[Account_Locked_Till_Date] [date] NULL,
	[Secret_Question_Id] [bigint] NULL,
	[Hint_Word] [nvarchar](50) NULL,
	[Secret_Answer] [nvarchar](100) NULL,
	[Referred_By_Id] [bigint] NULL,
	[Reference_Code] [nvarchar](50) NULL,
	[Last_Login_Date_Time] [datetime] NOT NULL,
	[Is_Active] [bit] NOT NULL,
	[Created_By_User_Id] [bigint] NOT NULL,
	[Created_Date_Time] [datetime] NOT NULL,
	[Modified_By_User_Id] [bigint] NULL,
	[Modified_Date_Time] [datetime] NULL,
	[Role_Id] [bigint] NULL,
	[Organization_Id] [bigint] NULL,
	[Activation_Code] [varchar](50) NULL,
	[OTP] [int] NULL,
	[Login_Count] [bigint] NULL,
	[Designation] [varchar](50) NULL,
	[City] [varchar](50) NULL,
	[State] [varchar](50) NULL,
	[Pin_Code] [varchar](10) NULL,
	[Session_Id] [varchar](50) NULL,
	[Access_Level] [nvarchar](50) NULL,
	[downloadnav_enabled] [bit] NULL,
	PRIMARY KEY CLUSTERED 
	(
		[User_Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [Masters].[User] ADD  CONSTRAINT [df_Access_Level]  DEFAULT ('pro') FOR [Access_Level]
GO

ALTER TABLE [Masters].[User] ADD  DEFAULT ((0)) FOR [downloadnav_enabled]
GO

ALTER TABLE [Masters].[User] ADD [Is_SSO_Login] bit NULL
GO

-- adding profile picture column to user table
ALTER TABLE [PMS_Controller].[Masters].[User]
ADD Profile_Picture nvarchar(100)
