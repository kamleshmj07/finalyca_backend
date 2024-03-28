USE [PMS_Base]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Transactions].[DebtScreener]') AND type in (N'U'))
DROP TABLE [Transactions].[DebtScreener]
GO


CREATE TABLE [Transactions].[DebtScreener](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[data_date] [datetime] NULL,
	[security_name] [varchar](100) NULL,
	[isin] [varchar](12) NULL,
	[security_type] [varchar](100) NULL,
	[bond_type] [varchar](100) NULL,
	[country] [varchar](2) NULL,
	[issuer] [varchar](70) NULL,
	[maturity_price] [numeric](16, 6) NULL,
	[maturity_based_on] [varchar](10) NULL,
	[maturity_benchmark_index] [varchar](100) NULL,
	[is_perpetual] [varchar](5) NULL,
	[on_tap_indicator] [bit] NULL,
	[coupon_type] [varchar](70) NULL,
	[interest_payment_frequency] [varchar](70) NULL,
	[is_cumulative] [bit] NULL,
	[compounding_frequency] [varchar](70) NULL,
	[min_investment_amount] [numeric](16, 6) NULL,
	[frn_index_benchmark] [varchar](10) NULL,
	[issuer_type] [varchar](50) NULL,
	[issue_size] [numeric](36, 6) NULL,
	[yield_at_issue] [numeric](16, 6) NULL,
	[maturity_structure] [varchar](50) NULL,
	[convention_method] [varchar](10) NULL,
	[interest_commencement_date] [date] NULL,
	[frn_type] [varchar](10) NULL,
	[markup] [numeric](16, 6) NULL,
	[minimum_interest_rate] [numeric](16, 6) NULL,
	[maximum_interest_rate] [numeric](16, 6) NULL,
	[is_guaranteed] [bit] NULL,
	[is_secured] [bit] NULL,
	[security_charge] [varchar](10) NULL,
	[security_collateral] [bit] NULL,
	[tier] [int] NULL,
	[is_upper] [bit] NULL,
	[is_sub_ordinate] [bit] NULL,
	[is_senior] [varchar](50) NULL,
	[is_callable] [bit] NULL,
	[is_puttable] [bit] NULL,
	[strip] [varchar](50) NULL,
	[is_taxable] [bit] NULL,
	[latest_applied_intpy_annual_coupon_rate] [numeric](16, 6) NULL,
	[sector_name] [varchar](200) NULL,
	[currency] [varchar](4) NULL,
	[maturity_date] [date] NULL,
	[interest_rate] [numeric](16, 6) NULL,
	[face_value] [numeric](16, 6) NULL,
	[paid_up_value] [numeric](16, 6) NULL,
	[crisil] [varchar](10) NULL,
	[care] [varchar](10) NULL,
	[fitch] [varchar](10) NULL,
	[icra] [varchar](10) NULL,
	[brickwork] [varchar](10) NULL,
	[sovereign] [varchar](10) NULL,
	[acuite] [varchar](10) NULL
	PRIMARY KEY CLUSTERED 
	(
		[id] ASC
	) ON [PRIMARY]
) ON [PRIMARY]
GO


