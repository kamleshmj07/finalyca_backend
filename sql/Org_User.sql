ALTER TABLE [PMS_Controller].[Masters].[Organization]
ADD  
[No_Of_L1_Licenses] INTEGER NOT NULL DEFAULT 0, 
[No_Of_L2_Licenses] INTEGER NOT NULL DEFAULT 0, 
[No_Of_L3_Licenses] INTEGER NOT NULL DEFAULT 0
;
Update  [PMS_Controller].[Masters].[Organization] SET No_Of_L3_Licenses = No_Of_Licenses

ALTER TABLE [PMS_Controller].[Masters].[User] ADD [Access_Level] INTEGER NOT NULL DEFAULT 3;

ALTER TABLE [PMS_Controller].[Transactions].[OrgFundSettings] DROP COLUMN [distributor_pan_no];
ALTER TABLE [PMS_Controller].[Transactions].[OrgFundSettings] ADD [distributor_org_id] NVARCHAR(500);
ALTER TABLE [PMS_Controller].[Transactions].[OrgFundSettings] ADD [distributor_token] NVARCHAR(500);

ALTER TABLE  [PMS_Controller].[Masters].[DOFacilitator] DROP COLUMN [facilitator_token];
INSERT INTO [Masters].[DOFacilitator] ( [created_by], [created_at], [is_active], [name], [facilitator_url], [facilitator_settings] ) VALUES (10215, '2022-06-30', 1, '1 Silverbullet', 'https://InvestmentGateway.1silverbullet.tech/sso', '{}');


#added downloadnav_enabled
ALTER TABLE [PMS_Controller].[Masters].[User] ADD [downloadnav_enabled] bit null DEFAULT 0;
update [PMS_Controller].[Masters].[User] set [downloadnav_enabled] = 0

update usr set [downloadnav_enabled] = 1 from Masters.Organization org
join Masters.[user] usr on usr.Organization_Id = org.Organization_Id
where org.Is_Enterprise_Value = 1