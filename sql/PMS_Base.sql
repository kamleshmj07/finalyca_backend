Use PMS_Base
ALTER table Masters.AMC Add hide_fields varchar(500)

--select * from Masters.Fund

#AIF debt changes
use PMS_Base

ALTER table Masters.Fund
add AIF_SPONSOR_COMMITMENT_IN_CR numeric(18,9) 

ALTER table Masters.Fund
add AIF_NRI_INVESTMENT_ALLOWED bit

ALTER table Masters.Fund
add AIF_TARGET_FUND_SIZE_IN_CR numeric(18,9) 

ALTER table Masters.Fund
add AIF_MIN_PURCHASE_AMOUNT numeric(18,9) 

ALTER table Transactions.factsheet
add AIF_DOLLAR_NAV numeric(18,9)

ALTER table Transactions.factsheet
add AIF_FUND_RATING varchar(100)

ALTER table Transactions.Underlying_Holdings
add Instrument varchar(100)

ALTER table Transactions.Underlying_Holdings
add Instrument_Rating varchar(100)

ALTER table Transactions.Underlying_Holdings
add secured_unsecured varchar(100)


--select * from Masters.Fund

--select top 100 * from Transactions.Underlying_Holdings order by 1 desc

-- AMC
ALTER table Masters.AMC
add facebook_url nvarchar(max)

ALTER table Masters.AMC
add linkedin_url nvarchar(max)

ALTER table Masters.AMC
add twitter_url nvarchar(max)

ALTER table Masters.AMC
add youtube_url nvarchar(max)

--Plans
ALTER table Masters.Plans
add ISIN2 nvarchar(200)

--Reports
create table Masters.Report_Plans_status
(
id bigint IDENTITY(1,1) PRIMARY KEY, 
plan_id bigint,
shared_in_last_month_report bit,
last_month_date datetime,
shared_in_current_month bit,
current_month_date datetime
)

create table Masters.Report_Data_Issues
(
id bigint IDENTITY(1,1) PRIMARY KEY, 
plan_id bigint,
plan_name varchar(400),
cur_nav_date datetime,
cur_nav numeric(18,9),
last_month_nav numeric(18,9),
nav_movement numeric(18,9),
fund_1month_performance numeric(18,9),
diff numeric(18,9),
issue_type varchar(200), -- deleted, data, 
is_fixed bit default 0
)

ALTER table Masters.MF_security
add risk_grade varchar(100)

ALTER table Masters.Fund
add fund_manager varchar(1000)


--Consolidated ratio
ALTER table Transactions.Fundamentals
Add PE_CONS float

ALTER table Transactions.Fundamentals
Add EPS_CONS float

ALTER table Transactions.Fundamentals
add PBV_CONS float



ALTER table transactions.factsheet 
add Treynor_Ratio_1Yr numeric(18,9)

ALTER table transactions.factsheet 
add Treynor_Ratio numeric(18,9)