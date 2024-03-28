Use PMS_Base

DROP TABLE [Transactions].[FundStocks]

CREATE TABLE [Transactions].[FundStocks] (
        [FundStocks_Id] BIGINT NOT NULL IDENTITY(1,1), 
        [AMC_Id] BIGINT NOT NULL,
        [AMC_Name] NVARCHAR(300) NOT NULL,
        [AMC_Logo] NVARCHAR(300) NOT NULL,
        [Product_Id] BIGINT NOT NULL,
        [Product_Name] NVARCHAR(300) NOT NULL,
        [Product_Code] NVARCHAR(50) NOT NULL,
        [Fund_Id] BIGINT NOT NULL,
        [Fund_Name] NVARCHAR(300) NOT NULL,
        [Classification_Id] INTEGER NOT NULL,
        [Classification_Name] NVARCHAR(200) NULL,
        [Plan_Id] BIGINT NOT NULL,
        [Plan_Name] NVARCHAR(300) NOT NULL,
        [HoldingSecurity_Id] BIGINT NOT NULL,
        [Portfolio_Date] DATETIME NOT NULL,
        [P_Portfolio_Date] DATETIME NULL,
        [Percentage_to_AUM] NUMERIC(30, 10) NULL,
        [P_Percentage_to_AUM] NUMERIC(30, 10) NULL,
        [Diff_Percentage_to_AUM] NUMERIC(30, 10) NULL,
        [Value_In_Inr] NUMERIC(30, 10) NULL,
        [P_Value_In_Inr] NUMERIC(30, 10) NULL,
        [IncreaseExposure] BIT NULL,
        [DecreaseExposure] BIT NULL,
        [NewStockForFund] BIT NULL,
        [ExitStockForFund] BIT NULL,
        [HoldingSecurity_Name] NVARCHAR(300) NULL,
        [InstrumentType] NVARCHAR(100) NULL,
        [Equity_Style] NVARCHAR(50) NULL,
        [ISIN_Code] NVARCHAR(100) NULL,
        [Issuer_Id] BIGINT NULL,
        [IssuerName] NVARCHAR(100) NULL,
        [Sector_Id] BIGINT NULL,
        [Sector_Code] NVARCHAR(100) NULL,
        [Sector_Names] NVARCHAR(100) NULL,
        [Asset_Class] NVARCHAR(100) NULL,
        [Risk_Category] NVARCHAR(100) NULL,
        [MarketCap] NVARCHAR(100) NULL,
        [Purchase_Date] DATETIME NULL DEFAULT (NULL),
        PRIMARY KEY ([FundStocks_Id])
)