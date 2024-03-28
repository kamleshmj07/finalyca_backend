ALTER TABLE [PMS_Controller].[Masters].[DOFacilitator] 
ALTER COLUMN facilitator_url NVARCHAR(1000) NOT NULL;

ALTER TABLE [PMS_Controller].[Masters].[DOFacilitator] 
ALTER COLUMN facilitator_settings NVARCHAR(max) NULL;

ALTER TABLE [PMS_Controller].[Masters].[DOFacilitator] 
ADD CONSTRAINT df_fac_settings
DEFAULT '{}' FOR facilitator_settings;

ALTER TABLE [PMS_Controller].[Transactions].[CustomScreens]
ALTER COLUMN query_json NVARCHAR(max) NULL;

ALTER TABLE [PMS_Controller].[Transactions].[CustomScreens] 
ADD CONSTRAINT df_query_json
DEFAULT '{}' FOR query_json;

ALTER TABLE [PMS_Controller].[Transactions].[APILog]
ALTER COLUMN req_payload NVARCHAR(max) NULL;

ALTER TABLE [PMS_Controller].[Transactions].[APILog] 
ADD CONSTRAINT df_req_payload
DEFAULT '{}' FOR req_payload;

ALTER TABLE [PMS_Controller].[Transactions].[APILog]
ALTER COLUMN resp_payload NVARCHAR(max) NULL;

ALTER TABLE [PMS_Controller].[Transactions].[APILog] 
ADD CONSTRAINT df_resp_payload
DEFAULT '{}' FOR resp_payload;

ALTER TABLE [PMS_Controller].[Transactions].[APILog]
ALTER COLUMN resp_error NVARCHAR(max) NULL;

ALTER TABLE [PMS_Controller].[Transactions].[APILog] 
ADD CONSTRAINT df_resp_error
DEFAULT '{}' FOR resp_error;

DROP TABLE [PMS_Controller].[Transactions].[OrgFundSettings]
CREATE TABLE [PMS_Controller].[Transactions].[OrgFundSettings] (
        id BIGINT NOT NULL IDENTITY(1,1),
        org_id BIGINT NULL,
        amc_id BIGINT NOT NULL,
        fund_code NVARCHAR(100) NULL,
        can_show BIT NULL DEFAULT '1',
        can_export BIT NULL DEFAULT '0',
        can_buy BIT NULL DEFAULT '0',
        facilitator_id BIGINT NULL,
        distributor_pan_no NVARCHAR(100) NULL,
        created_by BIGINT NOT NULL,
        created_at DATETIME NOT NULL,
        modified_by BIGINT NULL,
        modified_at DATETIME NULL,
        PRIMARY KEY (id),
        UNIQUE (org_id, amc_id, fund_code)
)