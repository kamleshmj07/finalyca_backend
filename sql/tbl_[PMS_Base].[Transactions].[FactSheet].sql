USE [PMS_Base]
GO

/****** Object:  Table [Transactions].[FactSheet] ******/


CREATE TABLE [Transactions].[FactSheet](
	[FactSheet_Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Plan_Id] [bigint] NULL,
	[TransactionDate] [datetime] NULL,
	[WeekHigh_52_Rs] [numeric](18, 9) NULL,
	[WeekLow_52_Rs] [numeric](18, 9) NULL,
	[TotalStocks] [numeric](18, 9) NULL,
	[PortfolioP_BRatio] [numeric](18, 9) NULL,
	[PortfolioP_ERatio] [numeric](18, 9) NULL,
	[EarningsGrowth_3Yrs_Percent] [numeric](18, 9) NULL,
	[AvgCreditRating] [numeric](18, 9) NULL,
	[ModifiedDuration_yrs] [numeric](18, 9) NULL,
	[StandardDeviation] [numeric](18, 9) NULL,
	[SharpeRatio] [numeric](18, 9) NULL,
	[Beta] [numeric](18, 9) NULL,
	[R_Squared] [numeric](18, 9) NULL,
	[Alpha] [numeric](18, 9) NULL,
	[Mean] [numeric](18, 9) NULL,
	[Sortino] [numeric](18, 9) NULL,
	[Equity] [numeric](18, 9) NULL,
	[Debt] [numeric](18, 9) NULL,
	[Cash] [numeric](18, 9) NULL,
	[RANKING_RANK_1MONTH] [bigint] NULL,
	[COUNT_1MONTH] [bigint] NULL,
	[RANKING_RANK_3MONTH] [bigint] NULL,
	[COUNT_3MONTH] [bigint] NULL,
	[RANKING_RANK_6MONTH] [bigint] NULL,
	[COUNT_6MONTH] [bigint] NULL,
	[RANKING_RANK_1YEAR] [bigint] NULL,
	[COUNT_1YEAR] [bigint] NULL,
	[RANKING_RANK_3YEAR] [bigint] NULL,
	[COUNT_3YEAR] [bigint] NULL,
	[RANKING_RANK_5YEAR] [bigint] NULL,
	[COUNT_5YEAR] [bigint] NULL,
	[SIP_RETURNS_1YEAR] [numeric](18, 9) NULL,
	[SIP_RETURNS_3YEAR] [numeric](18, 9) NULL,
	[SIP_RETURNS_5YEAR] [numeric](18, 9) NULL,
	[SIP_RANKINGS_1YEAR] [bigint] NULL,
	[SIP_RANKINGS_3YEAR] [bigint] NULL,
	[SIP_RANKINGS_5YEAR] [bigint] NULL,
	[SCHEME_RETURNS_1MONTH] [numeric](18, 9) NULL,
	[SCHEME_RETURNS_3MONTH] [numeric](18, 9) NULL,
	[SCHEME_RETURNS_6MONTH] [numeric](18, 9) NULL,
	[SCHEME_RETURNS_1YEAR] [numeric](18, 9) NULL,
	[SCHEME_RETURNS_3YEAR] [numeric](18, 9) NULL,
	[SCHEME_RETURNS_5YEAR] [numeric](18, 9) NULL,
	[SCHEME_RETURNS_since_inception] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_1MONTH] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_3MONTH] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_6MONTH] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_1YEAR] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_3YEAR] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_5YEAR] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_SI] [numeric](18, 9) NULL,
	[SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH] [numeric](18, 9) NULL,
	[SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH] [numeric](18, 9) NULL,
	[SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH] [numeric](18, 9) NULL,
	[SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR] [numeric](18, 9) NULL,
	[SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR] [numeric](18, 9) NULL,
	[SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR] [numeric](18, 9) NULL,
	[Risk_Grade] [nvarchar](100) NULL,
	[Return_Grade] [nvarchar](100) NULL,
	[Exit_Load] [nvarchar](500) NULL,
	[ExpenseRatio] [numeric](18, 9) NULL,
	[SOV] [numeric](18, 9) NULL,
	[AAA] [numeric](18, 9) NULL,
	[A1_Plus] [numeric](18, 9) NULL,
	[AA] [numeric](18, 9) NULL,
	[A_and_Below] [numeric](18, 9) NULL,
	[Bill_Rediscounting] [numeric](18, 9) NULL,
	[Cash_Equivalent] [numeric](18, 9) NULL,
	[Term_Deposit] [numeric](18, 9) NULL,
	[Unrated_Others] [numeric](18, 9) NULL,
	[Bonds_Debentures] [numeric](18, 9) NULL,
	[Cash_And_Cash_Equivalent] [numeric](18, 9) NULL,
	[CP_CD] [numeric](18, 9) NULL,
	[GOI_Securities] [numeric](18, 9) NULL,
	[MutualFunds_Debt] [numeric](18, 9) NULL,
	[Securitised_Debt] [numeric](18, 9) NULL,
	[ShortTerm_Debt] [numeric](18, 9) NULL,
	[Term_Deposits] [numeric](18, 9) NULL,
	[Treasury_Bills] [numeric](18, 9) NULL,
	[VRRatings] [bigint] NULL,
	[NetAssets_Rs_Cr] [numeric](18, 9) NULL,
	[AvgMktCap_Rs_Cr] [numeric](18, 9) NULL,
	[AvgMaturity_Yrs] [numeric](18, 9) NULL,
	[SourceFlag] [nvarchar](20) NULL,
	[Is_Deleted] [bit] NULL,
	[Portfolio_Dividend_Yield] [numeric](18, 9) NULL,
	[Churning_Ratio] [numeric](18, 9) NULL,
	[Portfolio_Sales_Growth_Estimated] [numeric](18, 9) NULL,
	[Portfolio_PAT_Growth_Estimated] [numeric](18, 9) NULL,
	[Portfolio_Earning_Growth_Estimated] [numeric](18, 9) NULL,
	[Portfolio_Forward_PE] [numeric](18, 9) NULL,
	[Created_By] [bigint] NULL,
	[Created_Date] [datetime] NULL,
	[Updated_By] [bigint] NULL,
	[Updated_Date] [datetime] NULL,
	[StandardDeviation_1Yr] [numeric](18, 9) NULL,
	[SharpeRatio_1Yr] [numeric](18, 9) NULL,
	[Beta_1Yr] [numeric](18, 9) NULL,
	[R_Squared_1Yr] [numeric](18, 9) NULL,
	[Alpha_1Yr] [numeric](18, 9) NULL,
	[Mean_1Yr] [numeric](18, 9) NULL,
	[Sortino_1Yr] [numeric](18, 9) NULL,
	[IsPortfolioProcessed] [bit] NOT NULL,
	[Portfolio_Date] [datetime] NULL,
	[IsRiskRatioProcessed] [bit] NOT NULL,
	[SCHEME_RETURNS_2YEAR] [numeric](18, 9) NULL,
	[SCHEME_RETURNS_10YEAR] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_2YEAR] [numeric](18, 9) NULL,
	[SCHEME_BENCHMARK_RETURNS_10YEAR] [numeric](18, 9) NULL,
	[AIF_COMMITEDCAPITAL_Rs_Cr] [numeric](18, 9) NULL,
	[AIF_DRAWDOWNCAPITAL_Rs_Cr] [numeric](18, 9) NULL,
	[AIF_CAPITALRETURNED_Rs_Cr] [numeric](18, 9) NULL,
	[AIF_INITIALCLOSUREDATE] [datetime] NULL,
	[AIF_FUNDCLOSUREDATE] [datetime] NULL,
	[AIF_ALLOTMENTDATE] [datetime] NULL,
	PRIMARY KEY CLUSTERED 
	(
		[FactSheet_Id] ASC
	)
) ON [PRIMARY]
GO

ALTER TABLE [Transactions].[FactSheet] ADD  DEFAULT ((0)) FOR [IsPortfolioProcessed]
GO

ALTER TABLE [Transactions].[FactSheet] ADD  DEFAULT ((0)) FOR [IsRiskRatioProcessed]
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Masters.Plan' , @level0type=N'SCHEMA',@level0name=N'Transactions', @level1type=N'TABLE',@level1name=N'FactSheet', @level2type=N'COLUMN',@level2name=N'Plan_Id'
GO


ALTER TABLE [Transactions].[FactSheet] ADD [Macaulay_Duration_Yrs] decimal(12,6) NULL
GO

ALTER TABLE [Transactions].[FactSheet] ADD [Yield_To_Maturity] decimal(12,6) NULL
GO



