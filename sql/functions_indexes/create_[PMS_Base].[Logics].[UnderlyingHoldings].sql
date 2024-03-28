USE [PMS_Base]
GO

/****** Object:  View [Logics].[UnderlyingHoldings] ******/


CREATE view [Logics].[UnderlyingHoldings]
WITH SCHEMABINDING
as
	select UH.Underlying_Holdings_Id
		,A.AMC_Id
		,A.AMC_Name
		,P.Plan_Id
		,P.Plan_Code
		,P.Plan_Name
		,BI.BenchmarkIndices_Id
		,BI.BenchmarkIndices_Code
		,BI.BenchmarkIndices_Name
		,AC.AssetClass_Id
		,AC.AssetClass_Name
		,MFS.MF_Security_OpenDate
		,PR.Product_Code
		,PR.Product_Id
		,PR.Product_Name
		,F.Fund_Id
		,F.Fund_Code
		,F.Fund_Name
		,UH.ISIN_Code
		,UH.Portfolio_Date
		,coalesce(UH.Asset_Class, case when HS.HoldingSecurity_Type = 'Equity' then HS.HoldingSecurity_Type else null end) as Asset_Class
		,cast(UH.Percentage_to_AUM as numeric(18,2)) as Percentage_to_AUM
		,UH.Value_in_INR
		,isnull(UH.Risk_Category,'') as Risk_Category
		--,case when HS.HoldingSecurity_Type = 'Equity' then HS.HoldingSecurity_Type else 'Others' end as InstrumentType--coalesce(UH.InstrumentType,'Equity') /*case when HS.HoldingSecurity_Id is null then isnull(UH.InstrumentType,'') else 'Equity' end*/  as InstrumentType
		,case when HS.Instrument_Type = 'Equity' then HS.Instrument_Type else 'Others' end as InstrumentType
		,UH.MarketCap
		,case when HS.HoldingSecurity_Type = 'Equity' then isnull(HS.MarketCap,'') else '' end as StocksRank
		,HS.HoldingSecurity_Id
		,coalesce(HS.HoldingSecurity_Name, UH.Company_Security_Name) as HoldingSecurity_Name
		,case when HS.HoldingSecurity_Type = 'Equity' then isnull(HS.Equity_Style,'') else '' end as Equity_Style
		,coalesce(S.Sector_Code, UH.Sector_Code) as Sector_Code
		,coalesce(S.Sector_Name, UH.Sector_Names,'') as Sector_Name
		,S.Sector_Id
		,coalesce(I.Issuer_Code, UH.IssuerCode) as Issuer_Code
		,coalesce(I.Issuer_Name, UH.IssuerName,'') as Issuer_Name
		,I.Issuer_Id
		,HS.HoldingSecurity_Type
		,HS.Vendor_Code
		,isnull(UH.LISTED_UNLISTED,'Listed') as LISTED_UNLISTED
	--select HS.* 
	from Masters.Plans P with(nolock)
		join Masters.MF_Security MFS with(nolock) on P.MF_Security_Id = MFS.MF_Security_Id and MFS.Status_Id = 1
		join Masters.AssetClass AC with(nolock) on MFS.AssetClass_Id = AC.AssetClass_Id
		join Transactions.PlanProductMapping PPM with(nolock) on P.Plan_Id = PPM.Plan_Id and ISNULL(PPM.Is_Deleted,0) <> 1
		join Masters.Product PR with(nolock) on PPM.Product_Id = PR.Product_Id
		join Masters.Fund F with(nolock) on MFS.Fund_Id = F.Fund_Id	
		join Masters.AMC A with(nolock) on MFS.AMC_Id = A.AMC_Id and isnull(A.Is_Deleted,0) <> 1
		join Transactions.Underlying_Holdings UH with(nolock) on F.Fund_Id = UH.Fund_Id and ISNULL(UH.Is_Deleted,0) <> 1
		left join Masters.HoldingSecurity HS with(nolock) on UH.HoldingSecurity_Id = HS.HoldingSecurity_Id
		left join Masters.Sector S with(nolock) on HS.Sector_Id = S.Sector_Id
		left join Masters.Issuer I with(nolock) on HS.Issuer_Id = I.Issuer_Id
		left join Masters.BenchmarkIndices BI with(nolock) on MFS.BenchmarkIndices_Id = BI.BenchmarkIndices_Id
	--where P.Plan_Id = 56
GO


