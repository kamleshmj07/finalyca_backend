USE [PMS_Controller]
GO


CREATE TABLE [Masters].[ReportIssue]
(  
    Id int IDENTITY(1,1) PRIMARY KEY,
    Created_By int NOT NULL,
    Created_Date datetime NOT NULL,
    Updated_By int NOT NULL,
    Updated_Date datetime NOT NULL,
	Report_Title nvarchar(100) NOT NULL,
    Report_Description nvarchar(500) NOT NULL,
    Report_Attachment nvarchar(100),
    Issue_Type nvarchar(50),
    Is_Active bit
);

GO
