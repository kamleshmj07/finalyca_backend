use PMS_Controller
CREATE TABLE [Transactions].[Investor] (
        id BIGINT NOT NULL IDENTITY(1,1),
        is_deleted TINYINT NOT NULL DEFAULT 0,
        created_by BIGINT NULL,
        created_date DATETIME NULL,
        updated_by BIGINT NULL,
        updated_date DATETIME NULL,
        label NVARCHAR(100) NULL,
        name NVARCHAR(100) NULL,
        pan_no NVARCHAR(10) NULL,
        PRIMARY KEY (id)
)

CREATE TABLE [Transactions].[InvestorAccount] (
        id BIGINT NOT NULL IDENTITY(1,1),
        is_deleted TINYINT NOT NULL DEFAULT 0,
        created_by BIGINT NULL,
        created_date DATETIME NULL,
        updated_by BIGINT NULL,
        updated_date DATETIME NULL,
        investor_id BIGINT NULL,
        owners NVARCHAR(max) NULL,
        account_type NVARCHAR(10) NULL,
        depository NVARCHAR(100) NULL,
        dp_name NVARCHAR(100) NULL,
        account_no NVARCHAR(100) NULL,
        label NVARCHAR(100) NULL,
        mapped_fund_code NVARCHAR(100) NULL,
        is_dummy SMALLINT NULL,
        PRIMARY KEY (id)
)

CREATE TABLE [Transactions].[InvestorHoldings] (
        id BIGINT NOT NULL IDENTITY(1,1),
        is_deleted TINYINT NULL DEFAULT 0,
        created_by BIGINT NULL,
        created_date DATETIME NULL,
        updated_by BIGINT NULL,
        updated_date DATETIME NULL,
        account_id BIGINT NULL,
        as_of_date DATETIME NULL,
        isin NVARCHAR(20) NULL,
        name NVARCHAR(200) NULL,
        type NVARCHAR(100) NULL,
        coupon_rate NVARCHAR(50) NULL,
        maturity_date NVARCHAR(50) NULL,
        units FLOAT NULL,
        unit_price FLOAT NULL,
        total_price FLOAT NULL,
        PRIMARY KEY (id)
)

CREATE TABLE [Transactions].[InvestorRecommendation] (        
        id BIGINT NOT NULL IDENTITY(1,1),
        is_deleted TINYINT NULL DEFAULT 0,
        created_by BIGINT NULL,
        created_date DATETIME NULL,
        updated_by BIGINT NULL,
        updated_date DATETIME NULL,
        investor_id BIGINT NULL,
        suggestion_date DATETIME NULL,
        observation NVARCHAR(max) NULL,
        suggestion NVARCHAR(max) NULL,
        demat_id BIGINT NULL,
        isin NVARCHAR(20) NULL,
        action_type NVARCHAR(20) NULL,
        units FLOAT NULL,
        PRIMARY KEY (id)
)

CREATE TABLE [Transactions].[InvestorTransactions] (
        id BIGINT NOT NULL IDENTITY(1,1),
        account_id BIGINT NULL,
        isin NVARCHAR(20) NULL,
        name NVARCHAR(200) NULL,
        tran_type NVARCHAR(10) NULL,
        tran_date DATETIME NULL,
        type NVARCHAR(100) NULL,
        units FLOAT NULL,
        unit_price FLOAT NULL,
        total_price FLOAT NULL,
        stamp_duty FLOAT NULL,
        is_valid_tran bit NULL,
        status NVARCHAR(200) NULL,
        is_deleted TINYINT NULL DEFAULT 0,
        created_by BIGINT NULL,
        created_date DATETIME NULL,
        updated_by BIGINT NULL,
        updated_date DATETIME NULL,
        PRIMARY KEY (id)
)

