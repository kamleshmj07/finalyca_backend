-- Created by Vijay shah to fill FundScreener table.
If Object_Id('[Logics].[Generate_FundScreener]', 'P') Is Not Null
	DROP PROCEDURE [Logics].[Generate_FundScreener];
Go

CREATE procedure [Logics].[Generate_FundScreener]
as    
BEGIN

SET NOCOUNT ON

-- find last transaction date for all funds with growth direct plans
select FS.Plan_Id, F.Fund_Id, max(FS.TransactionDate) as TransactionDate into #AsOnDate1
from Transactions.FactSheet FS with (nolock)
Inner join [Masters].[Plans] P with(nolock) on FS.Plan_Id = P.Plan_Id and P.PlanType_Id = 1 and isnull(P.Is_Deleted,0) <> 1
Inner join [Masters].[MF_Security] MFS with(nolock) on MFS.MF_Security_Id = P.MF_Security_Id and MFS.Status_Id = 1 and isnull(MFS.Is_Deleted,0) <> 1
Inner Join [Masters].[Fund] F with (nolock) ON F.Fund_Id = MFS.Fund_Id AND F.AutoPopulate = 1 and isnull(F.Is_Deleted,0) <> 1
Inner Join [Masters].[Options] O with (nolock) on P.Option_Id = O.Option_Id and Option_Name COLLATE Latin1_General_CS_AS LIKE '%G%'
where isnull(FS.Is_Deleted,0) <> 1 
group by FS.Plan_Id, F.Fund_Id
order by TransactionDate Desc;

-- Find relevant information and put into temp table. (We may add more fields from other tables in future)
SELECT Top 1 WITH TIES
FS.TransactionDate as transaction_date, PR.Product_Name as product, F.Fund_Id as fund_id, Plan1.Plan_id as plan_id, Plan1.Plan_Name as plan_name, F.Fund_Name as fund, A.AMC_Name as amc, C.Classification_Name as classification_name, plan1.PlanType_Id as plan_type, plan1.Option_Id as option_id,  AC.AssetClass_Name as asset_class, isnull(FS.ExpenseRatio,0) as expense_ratio, isnull(FS.TotalStocks,0) as total_stocks, isnull(FS.NetAssets_Rs_Cr,0) as aum, isnull(FS.Equity,0) as equity, isnull(FS.Debt,0) as debt, isnull(FS.Cash,0) as cash, isnull(FS.AvgMktCap_Rs_Cr,0) as avg_market_cap_in_cr, isnull(FS.PortfolioP_BRatio,0) as pb_ratio, isnull(FS.PortfolioP_ERatio,0) as pe_ratio, isnull(FS.AvgMaturity_Yrs,0) as avg_maturity_years, isnull(FS.ModifiedDuration_yrs,0) as modified_duration_years, isnull(FS.Portfolio_Dividend_Yield,0) as portfolio_dividend_yield, isnull(FS.Churning_Ratio,0) as churning_ratio, isnull(FS.SCHEME_RETURNS_1MONTH,0) as returns_1_month, isnull(FS.SCHEME_RETURNS_3MONTH,0) as returns_3_months, isnull(FS.SCHEME_RETURNS_6MONTH,0) as returns_6_months, isnull(FS.SCHEME_RETURNS_1YEAR,0) as returns_1_yr, isnull(FS.SCHEME_RETURNS_2YEAR,0) as returns_2_yr, isnull(FS.SCHEME_RETURNS_3YEAR,0) as returns_3_yr, isnull(FS.SCHEME_RETURNS_5YEAR,0) as returns_5_yr, isnull(FS.SCHEME_RETURNS_10YEAR,0) as returns_10_yr, isnull(FS.SCHEME_RETURNS_since_inception,0) as returns_since_inception, isnull(FS.StandardDeviation_1Yr, 0) as std_1_yr, isnull(FS.SharpeRatio_1Yr, 0) as sharpe_ratio_1_yr, isnull(FS.Beta_1Yr, 0) as beta_1_yr, isnull(FS.R_Squared_1Yr, 0) as r_squared_1_yr, isnull(FS.Alpha_1Yr, 0) as alpha_1_yr, isnull(FS.Mean_1Yr, 0) as mean_1_yr, isnull(FS.Sortino_1Yr, 0) as sortino_1_yr, isnull(FS.StandardDeviation,0) as std_3_yr, isnull(FS.SharpeRatio,0) as sharpe_ratio_3_yr, isnull(FS.Beta,0) as beta_3_yr, isnull(FS.R_Squared,0) as r_squared_3_yr, isnull(FS.Alpha,0) as alpha_3_yr, isnull(FS.Mean,0) as mean_3_yr, isnull(FS.Sortino,0) as sortino_3_yr 
INTO #results
FROM Transactions.FactSheet FS
INNER JOIN #AsOnDate1 ON #AsOnDate1.Plan_Id=FS.Plan_Id and #AsOnDate1.TransactionDate=FS.TransactionDate
INNER JOIN [Masters].[Plans] Plan1 with (nolock) On #AsOnDate1.Plan_Id = Plan1.Plan_Id and Plan1.Is_Deleted <> 1
INNER JOIN Transactions.PlanProductMapping PPM with (nolock) on #AsOnDate1.Plan_Id = PPM.Plan_Id and isnull(PPM.Is_Deleted,0) <> 1  
INNER JOIN Masters.Product PR with (nolock) on PPM.Product_Id = PR.Product_Id and FS.SourceFlag = PR.Product_Code
INNER JOIN Masters.MF_Security MFS with (nolock) on MFS.MF_Security_Id = Plan1.MF_Security_Id and MFS.Status_Id = 1
INNER JOIN Masters.AssetClass AC with (nolock) on MFS.AssetClass_Id = AC.AssetClass_Id
INNER JOIN Masters.Classification C with (nolock) on MFS.Classification_Id = C.Classification_Id 
INNER JOIN Masters.Fund F with (nolock) on MFS.Fund_Id = F.Fund_Id
INNER JOIN Masters.AMC A with (nolock) on MFS.AMC_Id = A.AMC_Id and isnull(A.Is_Deleted,0) <> 1 
where FS.Is_Deleted <> 1
ORDER BY ROW_NUMBER() over (partition by F.Fund_Id Order by F.Fund_Id)

-- delete old results and insert latest results into fund screener table
TRUNCATE TABLE [Transactions].[FundScreener];
INSERT INTO [Transactions].[FundScreener] (transaction_date, product, fund, amc, classification_name, asset_class, expense_ratio, total_stocks, aum, equity, debt, cash, avg_market_cap_in_cr, pb_ratio, pe_ratio, avg_maturity_years, modified_duration_years, portfolio_dividend_yield, churning_ratio, returns_1_month, returns_3_months, returns_6_months, returns_1_yr, returns_2_yr, returns_3_yr, returns_5_yr, returns_10_yr, returns_since_inception, std_1_yr, sharpe_ratio_1_yr, beta_1_yr, r_squared_1_yr, alpha_1_yr, mean_1_yr, sortino_1_yr, std_3_yr, sharpe_ratio_3_yr, beta_3_yr, r_squared_3_yr, alpha_3_yr, mean_3_yr, sortino_3_yr) SELECT transaction_date, product, fund, amc, classification_name, asset_class, expense_ratio, total_stocks, aum, equity, debt, cash, avg_market_cap_in_cr, pb_ratio, pe_ratio, avg_maturity_years, modified_duration_years, portfolio_dividend_yield, churning_ratio, returns_1_month, returns_3_months, returns_6_months, returns_1_yr, returns_2_yr, returns_3_yr, returns_5_yr, returns_10_yr, returns_since_inception, std_1_yr, sharpe_ratio_1_yr, beta_1_yr, r_squared_1_yr, alpha_1_yr, mean_1_yr, sortino_1_yr, std_3_yr, sharpe_ratio_3_yr, beta_3_yr, r_squared_3_yr, alpha_3_yr, mean_3_yr, sortino_3_yr FROM #results;

DROP TABLE #results;
DROP TABLE #AsOnDate1;

-- Sql script to add inception date column in FundScreener table
ALTER table Transactions.FundScreener
ADD inception_date datetime

ALTER table transactions.fundscreener
Add fund_age_in_months int

END

