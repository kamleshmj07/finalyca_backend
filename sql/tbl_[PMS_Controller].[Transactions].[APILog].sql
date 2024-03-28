USE [PMS_Controller]
GO

ALTER TABLE [Transactions].[APILog] DROP CONSTRAINT [df_resp_error]
GO

ALTER TABLE [Transactions].[APILog] DROP CONSTRAINT [df_resp_payload]
GO

ALTER TABLE [Transactions].[APILog] DROP CONSTRAINT [df_req_payload]
GO

/****** Object:  Table [Transactions].[APILog]  ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Transactions].[APILog]') AND type in (N'U'))
DROP TABLE [Transactions].[APILog]
GO


CREATE TABLE [Transactions].[APILog](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[entity_id] [bigint] NULL,
	[remote_addr] [nvarchar](100) NULL,
	[http_method] [nvarchar](10) NULL,
	[url_path] [nvarchar](1000) NULL,
	[query_str] [text] NULL,
	[req_ts] [datetime] NULL,
	[req_payload] [nvarchar](max) NULL,
	[req_has_files] [bit] NULL,
	[resp_status_code] [int] NULL,
	[resp_payload] [nvarchar](max) NULL,
	[resp_error] [nvarchar](max) NULL,
	[resp_time_ms] [int] NULL,
	[resp_size_bytes] [int] NULL,
	[entity_type] [nvarchar](10) NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [Transactions].[APILog] ADD  CONSTRAINT [df_req_payload]  DEFAULT ('{}') FOR [req_payload]
GO

ALTER TABLE [Transactions].[APILog] ADD  CONSTRAINT [df_resp_payload]  DEFAULT ('{}') FOR [resp_payload]
GO

ALTER TABLE [Transactions].[APILog] ADD  CONSTRAINT [df_resp_error]  DEFAULT ('{}') FOR [resp_error]
GO

ALTER TABLE [Transactions].[APILog] ALTER COLUMN [query_str] TEXT
GO

ALTER TABLE [Transactions].[APILog] ADD [fqdn] varchar(250)
GO