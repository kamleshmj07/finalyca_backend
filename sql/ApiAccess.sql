CREATE TABLE [PMS_Controller].[Masters].[API] (
        id BIGINT NOT NULL IDENTITY(1,1), 
        name NVARCHAR(100) NOT NULL, 
        org_id BIGINT NOT NULL, 
        api_key NVARCHAR(100) NOT NULL, 
        requested_by BIGINT NOT NULL, 
        requested_at DATETIME NOT NULL, 
        edited_by BIGINT NULL, 
        edited_at DATETIME NULL, 
        is_active BIT NULL, 
        inactive_reason NVARCHAR(100) NULL, 
        PRIMARY KEY (id), 
        UNIQUE (name, org_id)
)

CREATE TABLE [PMS_Controller].[Transactions].[APILog] (
        id BIGINT NOT NULL IDENTITY(1,1),
        api_id BIGINT NULL,
        remote_addr NVARCHAR(100) NULL,
        http_method NVARCHAR(10) NULL,
        url_path NVARCHAR(1000) NULL,
        query_str NVARCHAR(1000) NULL,
        req_ts DATETIME NULL,
        req_payload NTEXT NULL,
        req_has_files BIT NULL,
        resp_status_code INTEGER NULL,
        resp_payload NTEXT NULL,
        resp_error NTEXT NULL,
        resp_time_ms INTEGER NULL,
        resp_size_bytes INTEGER NULL,
        PRIMARY KEY (id)
)

CREATE TABLE [PMS_Controller].[Masters].[DOFacilitator] (
        id BIGINT NOT NULL IDENTITY(1,1),
        created_by BIGINT NOT NULL,
        created_at DATETIME NOT NULL,
        edited_by BIGINT NULL,
        edited_at DATETIME NULL,
        is_active BIT NULL,
        name NVARCHAR(200) NOT NULL,
        facilitator_token NVARCHAR(1000) NOT NULL,
        facilitator_url NTEXT NOT NULL,
        facilitator_settings NTEXT NULL,
        PRIMARY KEY (id)
)

CREATE TABLE [PMS_Controller].[Transactions].[OrgFundSettings] (
        id BIGINT NOT NULL IDENTITY(1,1),
        org_id BIGINT NULL,
        amc_id BIGINT NOT NULL,
        fund_code NVARCHAR(100) NULL,
        can_show BIT NULL,
        can_export BIT NULL,
        can_buy BIT NULL,
        facilitator_id BIGINT NULL,
        facilitator_settings NTEXT NULL,
        created_by BIGINT NOT NULL,
        created_at DATETIME NOT NULL,
        modified_by BIGINT NULL,
        modified_at DATETIME NULL,
        PRIMARY KEY (id)
)

ALTER TABLE [PMS_Controller].[Masters].[Organization]
ADD 
is_api_enabled BIT NULL,
api_access_level INTEGER NULL,
api_available_hits BIGINT NULL,
api_remote_addr NVARCHAR(1000) NULL,
is_excel_export_enabled BIT NULL,
excel_export_count BIGINT NULL,
is_buy_enable BIT NULL
;
