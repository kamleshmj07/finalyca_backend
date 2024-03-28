use PMS_Controller;

CREATE TABLE [Transactions].[AccountAggregatorAPIStatus] (
        id BIGINT NOT NULL IDENTITY(1,1),
        user_id NVARCHAR(100) NULL,
        txn_id NVARCHAR(100) NULL,
        data_start_date DATETIME NULL,
        data_end_date DATETIME NULL,
        consent_init_req_time DATETIME NULL,
        consent_init_resp NVARCHAR(max) NULL,
        consent_status_time DATETIME NULL,
        consent_status NVARCHAR(10) NULL,
        consent_accounts NVARCHAR(max) NULL,
        fetch_init_req_time DATETIME NULL,
        fetch_init_resp NVARCHAR(max) NULL,
        PRIMARY KEY (id)
)
