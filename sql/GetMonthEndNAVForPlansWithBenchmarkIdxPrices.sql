Use PMS_Base
Go

declare @datestart date = '01/31/2012'

-- drop table #fund_navdata

select	Max(NAV_Date) NvDate,
		Max(NAV) Nav,
		Concat(Cast(Mn as varchar(20)),Cast(Yr as varchar(20))) as Criteria
into #fund_navdata
from (
	select	NAV_Date,
			NAV,
			Year(Nav_Date) Yr,
			Month(Nav_Date) Mn,
			Day(Nav_Date) Dy,
			Plan_Id
	from Transactions.NAV
	where NAV_Date > @datestart
	and Plan_Id = 1146
) nv
group by Concat(Cast(Mn as varchar(20)),Cast(Yr as varchar(20)))
order by 1


select *
from Masters.Plans
where plan_id = 1146 -- 21823

select *
from Masters.MF_Security
where MF_Security_Id = 21823 -- 173

select *
from Masters.BenchmarkIndices
where BenchmarkIndices_Id = 173 -- 71969


select *
from Transactions.TRI_Returns
where TRI_Co_Code = '71969'


select	tri.TRI_IndexDate as AsofDate,
		nv.Nav,
		tri.TRI_IndexName,
		tri.Return_1Week,
		tri.Return_1Month,
		tri.Return_3Month,
		tri.Return_6Month,
		tri.Return_1Year,
		tri.Return_3Year
from Transactions.TRI_Returns tri
join #fund_navdata nv
	on tri.TRI_IndexDate = nv.NvDate
where tri.TRI_Co_Code = '71969'
and tri.Is_Deleted <> 1
order by 1


