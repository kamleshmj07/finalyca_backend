ALTER TABLE [PMS_Controller].[Masters].[Organization] ADD [is_self_subscribed] TINYINT NULL DEFAULT 0;
ALTER TABLE [PMS_Controller].[Masters].[Organization] ADD [is_payment_pending] TINYINT NULL DEFAULT 0;
ALTER TABLE [PMS_Controller].[Masters].[Organization] ADD [disclaimer] NVARCHAR(max) NULL DEFAULT 0;



-- #Addition of User Type field in organization
use PMS_Controller

create table PMS_Controller.Masters.[user_type]
(
id int primary key identity,
usertype_name nvarchar(200),
is_deleted bit
)


ALTER TABLE PMS_Controller.MASTERS.ORGANIZATION
ADD usertype_id int 

--insert into Masters.[user_type](usertype_name, is_deleted)
--values('AMC',0)

--insert into Masters.[user_type](usertype_name, is_deleted)
--values('User',0)

--insert into Masters.[user_type](usertype_name, is_deleted)
--values('API',0)

--insert into Masters.[user_type](usertype_name, is_deleted)
--values('Trial',0)

--insert into Masters.[user_type](usertype_name, is_deleted)
--values('Finalyca',0)

--insert into Masters.[user_type](usertype_name, is_deleted)
--values('HSL',0)

--insert into Masters.[user_type](usertype_name, is_deleted)
--values('PMS Bazaar',0)


-- select * from PMS_Controller.Masters.[user_type]


-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10124;
-- update a set usertype_id=1 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20195;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20187;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20173;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10100;
-- update a set usertype_id=1 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10103;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10054;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10102;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 6;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20262;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20226;
-- update a set usertype_id=1 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10096;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20254;
-- update a set usertype_id=3 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20168;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10035;
-- update a set usertype_id=1 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10069;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20201;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10066;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20253;
-- update a set usertype_id=4 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20261;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20251;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10151;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10126;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10129;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10020;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20178;
-- update a set usertype_id=5 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10027;
-- update a set usertype_id=4 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10121;
-- update a set usertype_id=5 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20200;
-- update a set usertype_id=5 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 3;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20228;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10162;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20227;
-- update a set usertype_id=6 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10068;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10152;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20179;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10057;
-- update a set usertype_id=1 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 11;
-- update a set usertype_id=1 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10164;
-- update a set usertype_id=4 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20258;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10111;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10155;
-- update a set usertype_id=7 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20249;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10142;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20244;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20246;
-- update a set usertype_id=7 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20255;
-- update a set usertype_id=7 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20165;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20177;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20252;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10144;
-- update a set usertype_id=5 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10087;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10110;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10074;
-- update a set usertype_id=5 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20188;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10071;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 5;
-- update a set usertype_id=4 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20260;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20257;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10046;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20259;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10065;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 20196;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10163;
-- update a set usertype_id=2 from PMS_Controller.MASTERS.ORGANIZATION a where organization_id = 10112;

