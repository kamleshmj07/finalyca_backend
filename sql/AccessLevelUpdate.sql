ALTER TABLE [PMS_Controller].[Masters].[Organization]
DROP CONSTRAINT [DF__Organizat__No_Of__2739D489];

ALTER TABLE [PMS_Controller].[Masters].[Organization]
DROP COLUMN [No_Of_L1_Licenses];

EXEC sp_rename 'Masters.Organization.No_Of_L2_Licenses', 'No_Of_Lite_Licenses', 'COLUMN';
EXEC sp_rename 'Masters.Organization.No_Of_L3_Licenses', 'No_Of_Pro_Licenses', 'COLUMN';

---
ALTER TABLE [PMS_Controller].[Masters].[User]
DROP CONSTRAINT [DF__User__Access_Lev__2A164134];

ALTER TABLE [PMS_Controller].[Masters].[User]
ALTER COLUMN Access_Level NVARCHAR(50) NULL;

ALTER TABLE [PMS_Controller].[Masters].[User]
ADD CONSTRAINT df_Access_Level
DEFAULT 'pro' FOR Access_Level;

UPDATE [PMS_Controller].[Masters].[User] SET Access_Level = 'pro' where Access_Level = '3';
UPDATE [PMS_Controller].[Masters].[User] SET Access_Level = 'lite' where Access_Level = '2';

