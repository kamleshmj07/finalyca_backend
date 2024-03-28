USE [PMS_Base]
GO

/****** Object: Table [Masters].[HoldingSecurity] ******/


/****** Drop Table/Constraints Script

EXEC sys.sp_dropextendedproperty @name=N'MS_Description' , @level0type=N'SCHEMA',@level0name=N'Masters', @level1type=N'TABLE',@level1name=N'HoldingSecurity', @level2type=N'COLUMN',@level2name=N'Sector_Id'
GO

ALTER TABLE [Masters].[HoldingSecurity] DROP CONSTRAINT [DF__HoldingSe__activ__2C0A41F5]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Masters].[HoldingSecurity]') AND type in (N'U'))
DROP TABLE [Masters].[HoldingSecurity]
GO

*/


CREATE TABLE [Masters].[HoldingSecurity](
	[HoldingSecurity_Id] [bigint] IDENTITY(1,1) NOT NULL,
	[HoldingSecurity_Name] [nvarchar](500) NULL,
	[ISIN_Code] [nvarchar](50) NULL,
	[Sector_Id] [bigint] NULL,
	[Asset_Class] [nvarchar](200) NULL,
	[Instrument_Type] [nvarchar](100) NULL,
	[Issuer_Code] [nvarchar](100) NULL,
	[Issuer_Name] [nvarchar](500) NULL,
	[Is_Deleted] [bit] NULL,
	[Created_By] [bigint] NULL,
	[Created_Date] [datetime] NULL,
	[Updated_By] [bigint] NULL,
	[Updated_Date] [datetime] NULL,
	[Issuer_Id] [bigint] NULL,
	[MarketCap] [nvarchar](50) NULL,
	[Equity_Style] [nvarchar](20) NULL,
	[HoldingSecurity_Type] [nvarchar](50) NULL,
	[BSE_Code] [bigint] NULL,
	[NSE_Symbol] [nvarchar](20) NULL,
	[BSE_GroupName] [nvarchar](20) NULL,
	[Co_Code] [bigint] NULL,
	[Short_CompanyName] [nvarchar](200) NULL,
	[Sub_SectorName] [nvarchar](100) NULL,
	[active] [bit] NULL,
	PRIMARY KEY CLUSTERED 
	(
		[HoldingSecurity_Id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD  DEFAULT ((1)) FOR [active]
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Masters.Sector' , @level0type=N'SCHEMA',@level0name=N'Masters', @level1type=N'TABLE',@level1name=N'HoldingSecurity', @level2type=N'COLUMN',@level2name=N'Sector_Id'
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Currency] varchar(4)
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD  DEFAULT ('INR') FOR [Currency]
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Maturity_Date] date
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Credit_Ratings] varchar(20)
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Credit_Ratings_Agency] varchar(200)
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Interest_Rate] numeric(16,6)
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Is_Listed] bit
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Face_Value] numeric(16,6)
GO

ALTER TABLE [Masters].[HoldingSecurity]
ADD [Paid_Up_Value] numeric(16,6)
GO

EXEC sp_rename 'Masters.HoldingSecurity.Co_Code', 'Vendor_Code', 'COLUMN';

ALTER TABLE [Masters].[HoldingSecurity]
ALTER COLUMN Vendor_Code varchar(50);



/******************************************** Other older SQL statements ********************************************/

alter table masters.holdingsecurity
add active bit default 1


update masters.holdingsecurity set active = 1



update masters.holdingsecurity set active = 1 where holdingsecurity_id=1538;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=12885;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=967;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42920;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42412;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2232;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3326;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43494;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2404;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=57158;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3486;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48457;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42648;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=4347;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3994;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=23446;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=39717;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=57073;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43533;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3719;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5824;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54181;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42996;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1149;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3614;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=21836;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2228;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43496;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5037;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=28205;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5147;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42524;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35011;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68908;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=23805;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=4053;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54157;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53243;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3551;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35412;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52835;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47870;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35990;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1315;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=11714;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=18735;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=35751;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=1339;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56806;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2103;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=14985;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=2105;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46637;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49310;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34691;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2960;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=18564;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54553;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38027;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38056;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52800;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53682;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5067;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=2452;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47867;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38057;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56640;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54546;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=32989;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2412;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43743;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54547;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33417;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1125;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=11087;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33586;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=45281;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=20343;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3390;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=2248;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=44652;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47866;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38164;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52254;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35777;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=28020;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=49326;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68273;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42505;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1247;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34267;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56999;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69009;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35209;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38224;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47882;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52836;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33694;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=11769;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=53709;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53291;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33031;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1799;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=57159;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5199;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=28788;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34116;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47875;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=7535;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3183;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=4430;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52262;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=17687;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2684;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33617;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48983;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52260;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35667;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=1104;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=40618;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=45378;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48987;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1789;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=13652;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1105;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42685;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47856;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36528;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33911;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48978;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=1272;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=6390;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33378;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68526;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33072;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54859;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46354;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35208;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38365;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68992;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49315;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35621;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=32981;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54545;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=47872;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35861;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69137;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=4259;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54543;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52799;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3931;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=20009;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=67773;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2218;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42421;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5289;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=29517;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36197;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=57001;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33909;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52796;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54180;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34953;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=1102;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=58589;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69303;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36535;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35572;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46283;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52228;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=4357;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5319;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=29820;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3301;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53665;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54159;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=32957;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49015;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38253;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=31970;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5821;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34076;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46255;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53676;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34071;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=19995;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3310;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33371;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54160;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34946;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69076;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5835;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43550;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48986;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34411;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35798;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47881;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42708;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3305;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=842;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43530;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48985;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36778;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=2562;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=44764;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33263;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68561;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=1131;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=6892;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35796;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69108;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36193;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52249;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34524;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68939;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52838;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38084;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1400;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=57318;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1013;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42411;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=7666;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5743;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2496;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42543;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38331;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52229;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53655;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33514;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=32837;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68468;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69044;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35379;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54849;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37496;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38860;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54175;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56636;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35041;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34248;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53239;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47876;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33091;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34679;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=55227;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38907;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69437;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46300;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=34511;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=28201;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5033;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=47868;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38277;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68928;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48990;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35873;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=4017;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47862;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39083;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56644;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56367;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35957;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38601;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54554;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43528;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2208;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37247;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69444;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37179;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53666;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42487;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2165;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35185;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52801;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=28714;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5190;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37150;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53711;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=13281;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1634;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1533;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=12875;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=4126;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=24078;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33664;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49309;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53654;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35193;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37382;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69451;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36379;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46337;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54177;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5005;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5705;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=52657;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=18583;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2993;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3069;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=18869;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5305;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46645;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47507;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35310;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37658;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52251;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43580;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5706;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5095;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42981;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53240;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33027;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=4079;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42947;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37593;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49017;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43584;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5832;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=15976;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2331;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=774;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42986;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47883;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35578;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36364;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56637;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37738;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68502;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47504;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37169;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46331;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38042;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=16470;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=58586;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=30936;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53287;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49033;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=6503;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37066;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46293;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=29577;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5293;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37379;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53651;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48518;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39058;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52266;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37386;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37410;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46288;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=11186;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=1157;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36083;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46326;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38373;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48456;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49014;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38962;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53652;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3143;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3697;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=7083;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3040;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42504;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=36756;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=31197;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37778;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46344;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39075;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53712;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46263;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36428;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38578;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69168;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52256;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35439;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36628;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53650;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49016;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=32273;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35795;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46281;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48460;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=2296;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37287;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47908;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56642;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35173;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=24166;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=24167;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35118;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52259;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49018;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=35961;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36500;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69446;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5596;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48462;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43532;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=44468;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5952;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5570;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=57030;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5941;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=47817;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=43526;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3965;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=4880;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=27501;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=2287;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42566;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=52608;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5843;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33882;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68658;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36430;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48980;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48459;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37673;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3226;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=67666;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37647;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53653;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=52797;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5910;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5611;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=44157;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=7015;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=2737;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37007;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49314;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46347;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36986;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37582;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69465;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42366;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5889;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5934;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=47251;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=51873;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5921;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53237;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39097;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=51874;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5922;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37060;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48979;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53242;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=38952;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=30911;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5575;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=13282;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68426;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=3804;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53236;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=5103;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=28507;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36336;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49316;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53656;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36409;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5660;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=40343;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37012;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69468;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=46085;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52269;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47509;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37399;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39114;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48988;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49013;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39178;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37466;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69460;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=20770;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=3470;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36435;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54548;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48982;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37363;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36600;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49308;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46310;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36487;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37325;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69447;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5898;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42365;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54847;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36493;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37483;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69454;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42364;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5899;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36945;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47879;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56643;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37362;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37313;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48458;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48991;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37903;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52837;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37345;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39139;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69461;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46291;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37319;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=32439;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=51875;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=55229;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37643;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=45890;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54549;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54174;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37378;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36620;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69449;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69450;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36623;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37380;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48984;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=55228;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37425;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53244;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39082;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37431;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69452;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69455;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37403;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37421;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46274;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=9876;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=756;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=969;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=10515;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37435;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=46327;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54848;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=47877;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37424;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53715;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39090;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47855;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=46271;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47859;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36693;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5101;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52839;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37493;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69457;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47503;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36920;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=36926;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53245;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53684;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=46236;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53238;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37467;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=47878;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39152;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37477;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48463;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=49299;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5874;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37476;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=56645;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=764;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48976;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39150;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69464;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69462;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=45134;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37579;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69463;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69466;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39157;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=52193;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=5932;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=7213;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=42385;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39171;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=52252;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53290;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37627;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37672;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53683;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=33318;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=57016;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=54860;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37669;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39174;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=48989;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=52194;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=8182;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=37008;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=69467;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68140;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=44168;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=44976;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=68288;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=46572;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=9263;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=39202;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=41936;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=40228;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=57018;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=40042;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=44757;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=45915;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=49312;
update masters.holdingsecurity set active = 1 where holdingsecurity_id=53667;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=46018;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=45910;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=51877;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=47902;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=56646;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=52275;
update masters.holdingsecurity set active = 0 where holdingsecurity_id=56641;
