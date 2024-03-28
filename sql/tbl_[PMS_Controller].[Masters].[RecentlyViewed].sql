USE [PMS_Controller]
GO


CREATE TABLE [Masters].[RecentlyViewed]
(  
    Id int IDENTITY(1,1) PRIMARY KEY,
    Created_By int NOT NULL,
    Created_Date datetime NOT NULL,
	Table_Name nvarchar(30) NOT NULL,
    Table_Row_Id int NOT NULL,
	Table_Row_Code nvarchar(30),
	Display_Name nvarchar(100) NOT NULL
);

GO

ALTER TABLE [Masters].[RecentlyViewed]
ADD table_obj nvarchar(max)
GO
