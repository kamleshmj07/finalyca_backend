USE [PMS_Controller]
GO

/****** Object:  Table [Masters].[Organization] ******/


CREATE TABLE [Masters].[Organization](
	[Organization_Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Organization_Name] [varchar](100) NOT NULL,
	[No_Of_Licenses] [int] NOT NULL,
	[License_Expiry_Date] [date] NOT NULL,
	[Adminuser_Email] [varchar](50) NOT NULL,
	[Adminuser_Mobile] [varchar](20) NOT NULL,
	[Adminuser_Fullname] [varchar](100) NOT NULL,
	[Is_Active] [bit] NOT NULL,
	[Is_DatacontrolEnable] [bit] NULL,
	[AMC_Id] [bigint] NULL,
	[Is_Enterprise_Value] [bit] NULL,
	[Is_WhiteLabel_Value] [bit] NULL,
	[Application_Title] [nvarchar](100) NULL,
	[Logo_Img] [nvarchar](max) NULL,
	[Disclaimer_Img] [nvarchar](max) NULL,
	[Disclaimer_Img2] [nvarchar](max) NULL,
	[is_api_enabled] [bit] NULL,
	[api_access_level] [int] NULL,
	[api_available_hits] [bigint] NULL,
	[api_remote_addr] [nvarchar](1000) NULL,
	[is_excel_export_enabled] [bit] NULL,
	[excel_export_count] [bigint] NULL,
	[is_buy_enable] [bit] NULL,
	[No_Of_Lite_Licenses] [int] NOT NULL,
	[No_Of_Pro_Licenses] [int] NOT NULL,
	[is_self_subscribed] [tinyint] NULL,
	[is_payment_pending] [tinyint] NULL,
	[disclaimer] [nvarchar](max) NULL,
	[usertype_id] [int] NULL,
	PRIMARY KEY CLUSTERED 
	(
		[Organization_Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [Masters].[Organization] ADD  DEFAULT ((0)) FOR [No_Of_Lite_Licenses]
GO

ALTER TABLE [Masters].[Organization] ADD  DEFAULT ((0)) FOR [No_Of_Pro_Licenses]
GO

ALTER TABLE [Masters].[Organization] ADD  DEFAULT ((0)) FOR [is_self_subscribed]
GO

ALTER TABLE [Masters].[Organization] ADD  DEFAULT ((0)) FOR [is_payment_pending]
GO

ALTER TABLE [Masters].[Organization] ADD [Is_Mobile_Mandatory] bit NULL
GO

ALTER TABLE [Masters].[Organization] ADD [No_Of_Silver_Licenses] int NULL
GO

ALTER table masters.organization add otp_Allowed_Over_Mail bit default 0
GO
Update masters.organization set otp_Allowed_Over_Mail = 0
-- Update masters.organization set otp_Allowed_Over_Mail = 1 where Organization_Id in (20227,10027,3,10068)

alter table PMS_Controller.Masters.Organization
add gst_number varchar(100)


