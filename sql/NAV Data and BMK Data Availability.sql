
select	Plan_Id,
		Plan_Name
from Masters.Plans
where Plan_Name like '%ASK%'

/* 6,7,44296,45002 */

select	min(fs.Portfolio_Date) PfDate,
		fs.Plan_Id,
		pl.Plan_Name,
		bmk.BenchmarkIndices_Name,
		bmk.BenchmarkIndices_Id,
		min(nv.NAV_Date) BmkPriceDate,
		NULL FundNavDate --min(nvf.NAV_Date) FundNavDate
from Transactions.FactSheet fs
join Masters.Plans pl
	on pl.Plan_Id = fs.Plan_Id
join Masters.MF_Security mf
	on mf.MF_Security_Id = pl.MF_Security_Id
join Masters.BenchmarkIndices bmk
	on bmk.BenchmarkIndices_Id = mf.BenchmarkIndices_Id
join Transactions.NAV nv
	on nv.Plan_Id = mf.BenchmarkIndices_Id
--join Transactions.NAV nvf
--	on nvf.Plan_Id = pl.Plan_Id
where pl.Plan_Id  in (6,7,44296,45002)
group by fs.Plan_Id, pl.Plan_Name, bmk.BenchmarkIndices_Name, bmk.BenchmarkIndices_Id

union all

select	NULL PfDate,
		Plan_Id,
		NULL Plan_Name,
		NULL BenchmarkIndices_Name,
		NULL BenchmarkIndices_Id,
		NULL BmkPriceDate,
		min(NAV_Date) FundNavDate
from Transactions.NAV
where Plan_Id  in (6,7,44296,45002)
group by Plan_Id

