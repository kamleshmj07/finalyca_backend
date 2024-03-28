USE [PMS_Controller]
GO


CREATE TABLE [Masters].[UserInterestedAMC]
(  
    Id int IDENTITY(1,1) PRIMARY KEY,
    Created_By int NOT NULL,
    Created_Date datetime NOT NULL,
	AMC_Id int NOT NULL,
    AMC_Name varchar(30) NOT NULL,
    Plan_Id int NOT NULL,
    Plan_Name nvarchar(30) NOT NULL
);

GO
