USE [PMS_Controller]
GO


CREATE TABLE [Masters].[Favorites]
(  
    id int IDENTITY(1,1) PRIMARY KEY,
    created_by int NOT NULL,
    created_date datetime NOT NULL,
	table_name nvarchar(30) NOT NULL,
    table_row_id int NOT NULL,
	table_row_code nvarchar(30),
	display_name nvarchar(100) NOT NULL,
    table_obj nvarchar(max),
);

GO

ALTER TABLE [Masters].[Favorites]
ADD table_obj nvarchar(max)