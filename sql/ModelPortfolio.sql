CREATE TABLE [Transactions].[ModelPortfolio] (
        id BIGINT NOT NULL IDENTITY(1,1),
        is_deleted TINYINT NULL DEFAULT 0,
        created_by BIGINT NULL,
        created_date DATETIME NULL,
        updated_by BIGINT NULL,
        updated_date DATETIME NULL,
        name NVARCHAR(100) NULL,
        description NVARCHAR(1000) NULL,
        PRIMARY KEY (id),
        UNIQUE (name)
)

CREATE TABLE [Transactions].[ModelPortfolioHoldings] (
        id BIGINT NOT NULL IDENTITY(1,1),
        is_deleted TINYINT NULL DEFAULT 0,
        created_by BIGINT NULL,
        created_date DATETIME NULL,
        updated_by BIGINT NULL,
        updated_date DATETIME NULL,
        model_portfolio_id BIGINT NULL,
        as_of_date DATETIME NULL,
        isin NVARCHAR(20) NULL,
        name NVARCHAR(200) NULL,
        weight FLOAT NULL,
        PRIMARY KEY (id)
)

CREATE TABLE [Transactions].[ModelPortfolioReturns] (
        id BIGINT NOT NULL IDENTITY(1,1),
        is_deleted TINYINT NULL DEFAULT 0,
        created_by BIGINT NULL,
        created_date DATETIME NULL,
        updated_by BIGINT NULL,
        updated_date DATETIME NULL,
        model_portfolio_id BIGINT NULL,
        as_of_date DATETIME NULL,
        return_1_month FLOAT NULL,
        PRIMARY KEY (id)
)