USE [PMS_Base]
GO

/****** Object:  View [Logics].[Index_Constituents] ******/


CREATE view [Logics].[Index_Constituents]
as
select BI.BenchmarkIndices_Id
	,BI.BenchmarkIndices_Name
	,IW.WDATE as [Date]
	,HS.ISIN_Code as ISIN
	,IW.WEIGHT_INDEX as Weight_in_Percentage
	,IW.CO_CODE
	, IW.Index_CO_CODE
from Masters.BenchmarkIndices BI with(nolock)
	join Transactions.IndexWeightage IW with(nolock) on BI.Co_Code = IW.Index_CO_CODE 
	join Masters.HoldingSecurity HS with(nolock) on HS.Vendor_Code = IW.CO_CODE
	where isnull(IW.Is_Deleted,0) <> 1
															and HS.ISIN_Code is not null
															--and IW.WDATE = '2022-11-22 00:00:00.000'
															and HS.Is_Deleted <> 1 and HS.active <> 0
	--order by 1 desc, 2 desc,4 desc
	--select * from Transactions.IndexWeightage where ISIN_Code = 'INE935N01012'
	--select * from Transactions.IndexWeightage where ISIN_Code = 'INE935N01020'
	
	--select top 1000 * from Transactions.IndexWeightage order by 1 desc





GO


