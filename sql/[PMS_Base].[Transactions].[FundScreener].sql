CREATE TABLE [PMS_Base].[Transactions].[FundScreener] (
        id BIGINT NOT NULL IDENTITY(1,1),
        transaction_date DATETIME NULL,
        -- portfolio_date DATETIME NULL,
        product NVARCHAR(200) NULL,
        -- plan_name NVARCHAR(200) NULL,
        fund NVARCHAR(200) NULL,
        amc NVARCHAR(200) NULL,
        classification_name NVARCHAR(200) NULL,
        asset_class NVARCHAR(200) NULL,
        expense_ratio NUMERIC(18, 4) NULL,
        total_stocks INTEGER NULL,
        aum NUMERIC(18, 4) NULL,
        equity NUMERIC(18, 4) NULL,
        debt NUMERIC(18, 4) NULL,
        cash NUMERIC(18, 4) NULL,
        avg_market_cap_in_cr NUMERIC(18, 4) NULL,
        pb_ratio NUMERIC(18, 4) NULL,
        pe_ratio NUMERIC(18, 4) NULL,
        avg_maturity_years NUMERIC(18, 4) NULL,
        modified_duration_years NUMERIC(18, 4) NULL,
        portfolio_dividend_yield NUMERIC(18, 4) NULL,
        churning_ratio NUMERIC(18, 4) NULL,
        returns_1_month NUMERIC(18, 4) NULL,
        returns_3_months NUMERIC(18, 4) NULL,
        returns_6_months NUMERIC(18, 4) NULL,
        returns_1_yr NUMERIC(18, 4) NULL,
        returns_2_yr NUMERIC(18, 4) NULL,
        returns_3_yr NUMERIC(18, 4) NULL,
        returns_5_yr NUMERIC(18, 4) NULL,
        returns_10_yr NUMERIC(18, 4) NULL,
        returns_since_inception NUMERIC(18, 4) NULL,
        std_1_yr NUMERIC(18, 4) NULL,
        sharpe_ratio_1_yr NUMERIC(18, 4) NULL,
        beta_1_yr NUMERIC(18, 4) NULL,
        r_squared_1_yr NUMERIC(18, 4) NULL,
        alpha_1_yr NUMERIC(18, 4) NULL,
        mean_1_yr NUMERIC(18, 4) NULL,
        sortino_1_yr NUMERIC(18, 4) NULL,
        std_3_yr NUMERIC(18, 4) NULL,
        sharpe_ratio_3_yr NUMERIC(18, 4) NULL,
        beta_3_yr NUMERIC(18, 4) NULL,
        r_squared_3_yr NUMERIC(18, 4) NULL,
        alpha_3_yr NUMERIC(18, 4) NULL,
        mean_3_yr NUMERIC(18, 4) NULL,
        sortino_3_yr NUMERIC(18, 4) NULL,
        PRIMARY KEY (id)
)

ALTER table Transactions.FundScreener
ADD benchmark_name varchar(200)

ALTER table Transactions.FundScreener
ADD large_cap numeric(18,4)

ALTER table Transactions.FundScreener
ADD mid_cap numeric(18,4)

ALTER table Transactions.FundScreener
ADD small_cap numeric(18,4)