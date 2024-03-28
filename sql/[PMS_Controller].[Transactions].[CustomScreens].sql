CREATE TABLE [Transactions].[CustomScreens] (
        id BIGINT NOT NULL IDENTITY(1,1),
        uuid NVARCHAR(100) NOT NULL,
        name NVARCHAR(100) NOT NULL,
        description NVARCHAR(1000) NULL,
        query_json NTEXT NOT NULL,
        access NVARCHAR(100) NOT NULL DEFAULT 'public',
        is_active BIT NOT NULL DEFAULT '1',
        created_by BIGINT NOT NULL,
        created_at DATETIME NOT NULL,
        org_id BIGINT NOT NULL,
        modified_by BIGINT NULL,
        modified_at DATETIME NULL,
        PRIMARY KEY (id),
        UNIQUE (name)
)
