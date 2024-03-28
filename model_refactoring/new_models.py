from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, Index, Integer, Numeric, String, Table, Unicode, text, JSON, ForeignKey, Date
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

class AMC(Base):
    __tablename__ = 'AMC'

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(100))
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    code = Column(Unicode(100))
    cin = Column(Unicode(100), comment="Corporate Identification Number given by RoC India")
    srn = Column(Unicode(100), comment="SEBI Registration Number")
    background = Column(Unicode(500))
    description = Column(Unicode(500))
    logo = Column(Unicode(500))
    address = Column(Unicode(500))
    contact_numbers = Column(Unicode(100))
    contact_person = Column(Unicode(100))
    email = Column(Unicode(256))
    website = Column(Unicode(250))

    def __str__(self):
        return F"{self.name}"

class FundManager(Base):
    """
    No need to add AMC id as we are linking funds to fund managers and fund have AMC.
    """
    __tablename__ = 'FundManager'

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(200))
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    code = Column(Unicode(100))
    description = Column(Unicode(2000))
    image = Column(Unicode(500))
    designation = Column(Unicode(300))

    def __str__(self):
        return F"{self.name}"

class FundManagerHistory(Base):
    __tablename__ = 'FundManagerHistory'

    id = Column(BigInteger, primary_key=True)
    fund_id = Column(BigInteger, ForeignKey(u'Fund.id'), index=True)
    fundmanager_id = Column(BigInteger, ForeignKey(u'FundManager.id'), index=True)
    date_from = Column(DateTime)
    date_to = Column(DateTime)

    fund = relationship(u'Fund')
    fundmanager = relationship(u'FundManager')

class FundType(Base):
    __tablename__ = 'FundType'
    __table_args__ = {
        'comment': 'select from PMS, AIF, ULIP, MF. For MF, it will contain info like RTA etc.'
    }

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(100))
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    code = Column(Unicode(100))
    prop_schema = Column(JSON, comment="contains json schema for the details of fund type")

    def __str__(self):
        return F"{self.name}"

class Classification(Base):
    __tablename__ = 'Classification'
    __table_args__ = {
        'comment': 'For comparing the funds among each other. e.g. Equity: Large Cap or Debt: Corporate Bond. Mostly we refer to AMFI /SEBI code.'
    }

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(200))
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    code = Column(Unicode(100))

    def __str__(self):
        return F"{self.name}"

class BenchmarkIndex(Base):
    __tablename__ = 'BenchmarkIndex'
    __table_args__ = {
        'comment': 'List of indices used by fund manager for benchmarking their instruments'
    }

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(200))
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    short_name = Column(Unicode(100))
    long_name = Column(Unicode(200))
    code = Column(Unicode(100))
    description = Column(Unicode(200))
    co_code = Column(BigInteger)
    tri_co_code = Column(BigInteger)
    exchange = Column(Unicode(10))
    bse_code = Column(BigInteger)
    bse_group_name = Column(Unicode(10))
    nse_symbol = Column(Unicode(10))
    attribution_flag = Column(Boolean)
    index_type = Column(Unicode(3))

    def __str__(self):
        return F"{self.name}"

class IndexReturns(Base):
    __tablename__ = 'IndexReturns'

    id = Column(BigInteger, primary_key=True)
    benchmark_index_id = Column(BigInteger, ForeignKey(u'BenchmarkIndex.id'), index=True)
    ts = Column(DateTime)
    return_1week = Column(Float(53))
    return_1month = Column(Float(53))
    return_3month = Column(Float(53))
    return_6month = Column(Float(53))
    return_1year = Column(Float(53))
    return_3year = Column(Float(53))

    benchmark_index = relationship(u'BenchmarkIndex')

class PlanType(Base):
    __tablename__ = 'PlanType'
    __table_args__ = {
        'comment': 'start with no_plan (for PMS, AIF) and continue with any on-boarding plan e.g. regular, direct etc'
    }

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(200))
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    code = Column(Unicode(100))

    def __str__(self):
        return F"{self.name}"

class Plan(Base):
    __tablename__ = 'Plan'    

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(200))
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    code = Column(Unicode(100))
    fund_id = Column(BigInteger, ForeignKey(u'Fund.id'), index=True)
    plan_type_id = Column(BigInteger, ForeignKey(u'PlanType.id'), index=True)
    isin = Column(Unicode(100))
    amfi_code = Column(Unicode(100))
    amfi_name = Column(Unicode(200))
    
    plan_type = relationship(u'PlanType')
    fund = relationship(u'Fund')

    def __str__(self):
        return F"{self.name}"

class FactSheet(Base):
    __tablename__ = 'FactSheet'

    id = Column(BigInteger, primary_key=True)
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    plan_id = Column(BigInteger, ForeignKey(u'Plan.id'), index=True)
    ts = Column(DateTime)

    # IsPortfolioProcessed = Column(Boolean, nullable=False, server_default=text('((0))'))
    # IsRiskRatioProcessed = Column(Boolean, nullable=False, server_default=text('((0))'))
    # WeekHigh_52_Rs = Column(Numeric(18, 9))
    # WeekLow_52_Rs = Column(Numeric(18, 9))
    # TotalStocks = Column(Numeric(18, 9))
    # PortfolioP_BRatio = Column(Numeric(18, 9))
    # PortfolioP_ERatio = Column(Numeric(18, 9))
    # EarningsGrowth_3Yrs_Percent = Column(Numeric(18, 9))
    # AvgCreditRating = Column(Numeric(18, 9))
    # ModifiedDuration_yrs = Column(Numeric(18, 9))
    # StandardDeviation = Column(Numeric(18, 9))
    # SharpeRatio = Column(Numeric(18, 9))
    # Beta = Column(Numeric(18, 9))
    # R_Squared = Column(Numeric(18, 9))
    # Alpha = Column(Numeric(18, 9))
    # Mean = Column(Numeric(18, 9))
    # Sortino = Column(Numeric(18, 9))
    # Equity = Column(Numeric(18, 9))
    # Debt = Column(Numeric(18, 9))
    # Cash = Column(Numeric(18, 9))
    # RANKING_RANK_1MONTH = Column(BigInteger)
    # COUNT_1MONTH = Column(BigInteger)
    # RANKING_RANK_3MONTH = Column(BigInteger)
    # COUNT_3MONTH = Column(BigInteger)
    # RANKING_RANK_6MONTH = Column(BigInteger)
    # COUNT_6MONTH = Column(BigInteger)
    # RANKING_RANK_1YEAR = Column(BigInteger)
    # COUNT_1YEAR = Column(BigInteger)
    # RANKING_RANK_3YEAR = Column(BigInteger)
    # COUNT_3YEAR = Column(BigInteger)
    # RANKING_RANK_5YEAR = Column(BigInteger)
    # COUNT_5YEAR = Column(BigInteger)
    # SIP_RETURNS_1YEAR = Column(Numeric(18, 9))
    # SIP_RETURNS_3YEAR = Column(Numeric(18, 9))
    # SIP_RETURNS_5YEAR = Column(Numeric(18, 9))
    # SIP_RANKINGS_1YEAR = Column(BigInteger)
    # SIP_RANKINGS_3YEAR = Column(BigInteger)
    # SIP_RANKINGS_5YEAR = Column(BigInteger)
    # SCHEME_RETURNS_1MONTH = Column(Numeric(18, 9))
    # SCHEME_RETURNS_3MONTH = Column(Numeric(18, 9))
    # SCHEME_RETURNS_6MONTH = Column(Numeric(18, 9))
    # SCHEME_RETURNS_1YEAR = Column(Numeric(18, 9))
    # SCHEME_RETURNS_3YEAR = Column(Numeric(18, 9))
    # SCHEME_RETURNS_5YEAR = Column(Numeric(18, 9))
    # SCHEME_RETURNS_since_inception = Column(Numeric(18, 9))
    # SCHEME_BENCHMARK_RETURNS_1MONTH = Column(Numeric(18, 9))
    # SCHEME_BENCHMARK_RETURNS_3MONTH = Column(Numeric(18, 9))
    # SCHEME_BENCHMARK_RETURNS_6MONTH = Column(Numeric(18, 9))
    # SCHEME_BENCHMARK_RETURNS_1YEAR = Column(Numeric(18, 9))
    # SCHEME_BENCHMARK_RETURNS_3YEAR = Column(Numeric(18, 9))
    # SCHEME_BENCHMARK_RETURNS_5YEAR = Column(Numeric(18, 9))
    # SCHEME_BENCHMARK_RETURNS_SI = Column(Numeric(18, 9))
    # SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH = Column(Numeric(18, 9))
    # SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH = Column(Numeric(18, 9))
    # SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH = Column(Numeric(18, 9))
    # SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR = Column(Numeric(18, 9))
    # SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR = Column(Numeric(18, 9))
    # SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR = Column(Numeric(18, 9))
    # Risk_Grade = Column(Unicode(100))
    # Return_Grade = Column(Unicode(100))
    # Exit_Load = Column(Unicode(500))
    # ExpenseRatio = Column(Numeric(18, 9))
    # SOV = Column(Numeric(18, 9))
    # AAA = Column(Numeric(18, 9))
    # A1_Plus = Column(Numeric(18, 9))
    # AA = Column(Numeric(18, 9))
    # A_and_Below = Column(Numeric(18, 9))
    # Bill_Rediscounting = Column(Numeric(18, 9))
    # Cash_Equivalent = Column(Numeric(18, 9))
    # Term_Deposit = Column(Numeric(18, 9))
    # Unrated_Others = Column(Numeric(18, 9))
    # Bonds_Debentures = Column(Numeric(18, 9))
    # Cash_And_Cash_Equivalent = Column(Numeric(18, 9))
    # CP_CD = Column(Numeric(18, 9))
    # GOI_Securities = Column(Numeric(18, 9))
    # MutualFunds_Debt = Column(Numeric(18, 9))
    # Securitised_Debt = Column(Numeric(18, 9))
    # ShortTerm_Debt = Column(Numeric(18, 9))
    # Term_Deposits = Column(Numeric(18, 9))
    # Treasury_Bills = Column(Numeric(18, 9))
    # VRRatings = Column(BigInteger)
    # NetAssets_Rs_Cr = Column(Numeric(18, 9))
    # AvgMktCap_Rs_Cr = Column(Numeric(18, 9))
    # AvgMaturity_Yrs = Column(Numeric(18, 9))
    # SourceFlag = Column(Unicode(20))
    # Portfolio_Dividend_Yield = Column(Numeric(18, 9))
    # Churning_Ratio = Column(Numeric(18, 9))
    # Portfolio_Sales_Growth_Estimated = Column(Numeric(18, 9))
    # Portfolio_PAT_Growth_Estimated = Column(Numeric(18, 9))
    # Portfolio_Earning_Growth_Estimated = Column(Numeric(18, 9))
    # Portfolio_Forward_PE = Column(Numeric(18, 9))
    # StandardDeviation_1Yr = Column(Numeric(18, 9))
    # SharpeRatio_1Yr = Column(Numeric(18, 9))
    # Beta_1Yr = Column(Numeric(18, 9))
    # R_Squared_1Yr = Column(Numeric(18, 9))
    # Alpha_1Yr = Column(Numeric(18, 9))
    # Mean_1Yr = Column(Numeric(18, 9))
    # Sortino_1Yr = Column(Numeric(18, 9))
    # Portfolio_Date = Column(DateTime)
    # AIF_COMMITEDCAPITAL_Rs_Cr = Column(Numeric(18, 9))
    # AIF_DRAWDOWNCAPITAL_Rs_Cr = Column(Numeric(18, 9))
    # AIF_CAPITALRETURNED_Rs_Cr = Column(Numeric(18, 9))
    # AIF_INITIALCLOSUREDATE = Column(DateTime)
    # AIF_FUNDCLOSUREDATE = Column(DateTime)
    # AIF_ALLOTMENTDATE = Column(DateTime)

    plan = relationship(u'Plan')

class NAV(Base):
    __tablename__ = 'NAV'

    id = Column(BigInteger, primary_key=True)
    plan_id = Column(BigInteger, ForeignKey(u'Plan.id'), index=True)
    ts = Column(Date)
    nav = Column(Float(53))
    nav_portfolio_return = Column(Numeric(18, 2))
    nav_type = Column(Unicode(1))

    plan = relationship(u'Plan')

class Fund(Base):
    __tablename__ = 'Fund'

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(200))
    edit_history = Column(JSON)
    is_deleted = Column(Boolean)
    code = Column(Unicode(100), index=True)
    description = Column(Unicode(1000))
    website = Column(Unicode(100))
    old_name = Column(Unicode(200))
    amc_id = Column(BigInteger, ForeignKey(u'AMC.id'), index=True)
    fund_type_id = Column(BigInteger, ForeignKey(u'FundType.id'), index=True)
    classification_id = Column(BigInteger, ForeignKey(u'Classification.id'), index=True)
    benchmark_index_id = Column(BigInteger, ForeignKey(u'BenchmarkIndex.id'), index=True)
    details = Column(JSON, comment="contains details based on FundType.prop_schema filtered by fund_type_id")

    amc = relationship(u'AMC')
    fund_type = relationship(u'FundType')
    classification = relationship(u'Classification')
    benchmark_index = relationship(u'BenchmarkIndex')

    def __str__(self):
        return F"{self.name}"

class PortfolioAnalysis(Base):
    __tablename__ = 'PortfolioAnalysis'

    id = Column(BigInteger, primary_key=True)
    fund_id = Column(BigInteger, ForeignKey(u'Fund.id'), index=True)
    ts = Column(DateTime)
    attribute_type = Column(Unicode(200))
    attribute_text = Column(Unicode(200))
    attribute_value = Column(Numeric(18, 2))
    attribute_sub_text = Column(Unicode(200))

    fund = relationship(u'Fund')

class PortfolioHoldings(Base):
    __tablename__ = 'PortfolioHoldings'

    id = Column(BigInteger, primary_key=True)
    fund_id = Column(BigInteger, ForeignKey(u'Fund.id'), index=True)
    ts = Column(DateTime)
    security_id = Column(BigInteger, ForeignKey(u'Security.id'), index=True)
    current_weight = Column(Numeric(18, 2))
    difference_weight = Column(Numeric(18, 2))
    percentage_to_aum = Column(Numeric(30, 9))
    p_percentage_to_aum = Column(Numeric(30, 10))
    diff_percentage_to_aum = Column(Numeric(30, 10))
    value_in_inr = Column(Numeric(30, 10))
    p_value_in_inr = Column(Numeric(30, 10))
    is_increased = Column(Boolean)
    is_decreased = Column(Boolean)
    is_added = Column(Boolean)
    is_exited = Column(Boolean)

    purchase_date = Column(DateTime)

    fund = relationship(u'Fund')
    security = relationship(u'Security')

class SecurityType(Base):
    __tablename__ = 'SecurityType'
    __table_args__ = {
        'comment': 'This should be related to Asset Class and instrument type. It will have detailed category and sub category of the security types.'
    }

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(100))
    category = Column(Unicode(100))
    sub_category = Column(Unicode(100))

    def __str__(self):
        return F"{self.category}: {self.sub_category}"

class Security(Base):
    __tablename__ = 'Security'
    __table_args__ = {
        'comment': ''
    }

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(100))
    code = Column(Unicode(100))
    issuer_id = Column(BigInteger, ForeignKey(u'Issuer.id'), index=True)
    security_type_id = Column(BigInteger, ForeignKey(u'SecurityType.id'), index=True)
    isin = Column(Unicode(50))
    bse_code = Column(BigInteger)
    nse_symbol = Column(Unicode(20))
    bse_group_name = Column(Unicode(20))
    co_code = Column(BigInteger)
    risk_category = Column(Unicode(100))

    issuer = relationship(u'Issuer')
    security_type = relationship(u'SecurityType')

    def __str__(self):
        return F"{self.name}"

class Issuer(Base):
    __tablename__ = 'Issuer'
    __table_args__ = {
        'comment': 'We may need to identify the classification for the issuer.'
    }

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(100))
    code = Column(Unicode(100))
    sector_id = Column(BigInteger, ForeignKey(u'Sector.id'), index=True)
    external_code = Column(Unicode(100))
    is_in_house = Column(Boolean)
    address = Column(Unicode(250))
    pin_code = Column(Unicode(6))
    city = Column(Unicode(250))
    state = Column(Unicode(250))
    Country_Id = Column(Unicode(250))
    contact = Column(Unicode(10))
    fax = Column(Unicode(10))
    email = Column(Unicode(150))

    sector = relationship(u'Sector')

    def __str__(self):
        return F"{self.name}"

class Sector(Base):
    __tablename__ = 'Sector'

    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(200))
    code = Column(Unicode(100))
    bse_code = Column(Unicode(100))
    nse_code = Column(Unicode(100))
    amfi_code = Column(Unicode(100))

    def __str__(self):
        return F"{self.name}"
    
class IndexWeightage(Base):
    __tablename__ = 'IndexWeightage'

    id = Column(BigInteger, primary_key=True)
    benchmark_index_id = Column(BigInteger, ForeignKey(u'BenchmarkIndex.id'), index=True)
    security_id = Column(BigInteger, ForeignKey(u'Security.id'), index=True)
    ts = Column(DateTime)
    closing_price = Column(Float(53))
    no_of_shares = Column(Float(53))
    full_mkt_cap = Column(Float(53))
    ff_adj_factor = Column(Float(53))
    ff_mcap = Column(Float(53))
    index_weight = Column(Float(53))

    benchmark_index = relationship(u'BenchmarkIndex')
    security = relationship(u'Security')


# class ListingStatus(Base):
#     __tablename__ = 'ListingStatus'
#     __table_args__ = {'schema': 'Masters'}

#     ListingStatus_Id = Column(BigInteger, primary_key=True)
#     ListingStatus_Name = Column(Unicode(200))
#     ListingStatus_Code = Column(Unicode(100))
#     Is_Deleted = Column(Boolean)
#     Created_By = Column(BigInteger)
#     Created_Date = Column(DateTime)
#     Updated_By = Column(BigInteger)
#     Updated_Date = Column(DateTime)


# class MarketCap(Base):
#     __tablename__ = 'MarketCap'
#     __table_args__ = {'schema': 'Masters'}

#     MarketCap_Id = Column(BigInteger, primary_key=True)
#     MarketCap_Name = Column(Unicode(100))
#     MarketCap_Code = Column(Unicode(100))
#     Is_Deleted = Column(Boolean)
#     Created_By = Column(BigInteger)
#     Created_Date = Column(DateTime)
#     Updated_By = Column(BigInteger)
#     Updated_Date = Column(DateTime)

# class MFSecurity(Base):
#     __tablename__ = 'MF_Security'
#     __table_args__ = {
#         'comment': 'this could become part of the details in Fund. if the queries are slow, then think of making a separate table.'
#     }

#     fund_id = Column(BigInteger, ForeignKey(u'Fund.id'), index=True)
#     code = Column(Unicode(100))
#     AssetClass_Id = Column(BigInteger)
#     Status_Id = Column(BigInteger)
#     MF_Security_UnitFaceValue = Column(Numeric(18, 9))
#     MF_Security_OpenDate = Column(DateTime)
#     MF_Security_CloseDate = Column(DateTime)
#     MF_Security_ReopenDate = Column(DateTime)
#     MF_Security_PurchaseAvailable = Column(Boolean)
#     MF_Security_Redemption_Available = Column(Boolean)
#     MF_Security_SIP_Available = Column(Boolean)
#     MF_Security_Min_Purchase_Amount = Column(Numeric(18, 9))
#     MF_Security_Purchase_Multiplies_Amount = Column(Numeric(18, 9))
#     MF_Security_Add_Min_Purchase_Amount = Column(Numeric(18, 9))
#     MF_Security_Add_Purchase_Multiplies_Amount = Column(Numeric(18, 9))
#     MF_Security_Min_Redeem_Amount = Column(Numeric(18, 9))
#     MF_Security_Min_Redeem_Units = Column(Numeric(18, 9))
#     MF_Security_Trxn_Cut_Off_Time = Column(Unicode(10))
#     MF_Security_SIP_Frequency = Column(Unicode(500))
#     MF_SIP_Dates = Column(Unicode(2000))
#     MF_Security_SIP_Min_Amount = Column(Unicode(500))
#     MF_Security_SIP_Min_Agg_Amount = Column(Numeric(18, 9))
#     MF_Security_Maturity_Date = Column(DateTime)
#     MF_Security_Min_Balance_Unit = Column(Numeric(18, 9))
#     MF_Security_Maturity_Period = Column(BigInteger)
#     MF_Security_Min_Lockin_Period = Column(BigInteger)
#     MF_Security_Investment_Strategy = Column(Unicode(2000))
#     MF_Security_SIP_Min_Installment = Column(Unicode(200))
#     MF_Security_STP_Available = Column(Boolean)
#     MF_Security_STP_Frequency = Column(Unicode(100))
#     MF_Security_STP_Min_Install = Column(Unicode(100))
#     MF_Security_STP_Dates = Column(Unicode(2000))
#     MF_Security_STP_Min_Amount = Column(Unicode(500))
#     MF_Security_SWP_Available = Column(Boolean)
#     MF_Security_SWP_Frequency = Column(Unicode(100))
#     MF_Security_SWP_Min_Install = Column(Unicode(100))
#     MF_Security_SWP_Dates = Column(Unicode(2000))
#     MF_Security_SWP_Min_Amount = Column(Unicode(500))
#     Fees_Structure = Column(Unicode(1000))
#     INS_SFINCode = Column(Unicode(100))
#     INS_EquityMin = Column(Float(53))
#     INS_EquityMax = Column(Float(53))
#     INS_DebtMin = Column(Float(53))
#     INS_DebtMax = Column(Float(53))
#     INS_CommMin = Column(Float(53))
#     INS_CommMax = Column(Float(53))
#     INS_CashMoneyMarketMin = Column(Float(53))
#     INS_CashMoneyMarketMax = Column(Float(53))
#     INS_EquityDerivativesMin = Column(Float(53))
#     INS_EquityDerivativesMax = Column(Float(53))

# class Options(Base):
#     __tablename__ = 'Options'
#     __table_args__ = {'schema': 'Masters'}

#     Option_Id = Column(BigInteger, primary_key=True)
#     Option_Name = Column(Unicode(200))
#     Option_Code = Column(Unicode(100))
#     Is_Deleted = Column(Boolean)
#     Created_By = Column(BigInteger)
#     Created_Date = Column(DateTime)
#     Updated_By = Column(BigInteger)
#     Updated_Date = Column(DateTime)

# class Status(Base):
#     __tablename__ = 'Status'
#     __table_args__ = {'schema': 'Masters'}

#     Status_Id = Column(BigInteger, primary_key=True)
#     Status_Name = Column(Unicode(200))
#     Status_Code = Column(Unicode(100))
#     Is_Deleted = Column(Boolean)

# class SwitchAllowed(Base):
#     __tablename__ = 'SwitchAllowed'
#     __table_args__ = {'schema': 'Masters'}

#     SwitchAllowed_Id = Column(BigInteger, primary_key=True)
#     SwitchAllowed_Name = Column(Unicode(200))
#     SwitchAllowed_Code = Column(Unicode(100))
#     Is_Deleted = Column(Boolean)
#     Created_By = Column(BigInteger)
#     Created_Date = Column(DateTime)
#     Updated_By = Column(BigInteger)
#     Updated_Date = Column(DateTime)

# class ClosingValues(Base):
#     __tablename__ = 'ClosingValues'
#     __table_args__ = {'schema': 'Transactions'}

#     ClosingValues_Id = Column(BigInteger, primary_key=True)
#     BSE_Code = Column(BigInteger)
#     ISIN_Code = Column(Unicode(50))
#     Date_ = Column('Date', DateTime)
#     ST_EXCHNG = Column(Unicode(3))
#     Co_Code = Column(BigInteger)
#     HIGH = Column(Float(53))
#     LOW = Column(Float(53))
#     OPEN = Column(Float(53))
#     CLOSE = Column(Float(53))
#     TDCLOINDI = Column(Unicode(2))
#     VOLUME = Column(BigInteger)
#     NO_TRADES = Column(BigInteger)
#     NET_TURNOV = Column(Float(53))
#     Is_Deleted = Column(Boolean)
#     Created_By = Column(BigInteger)
#     Created_Date = Column(DateTime)
#     Updated_By = Column(BigInteger)
#     Updated_Date = Column(DateTime)

# class FactsheetAttribution(Base):
#     __tablename__ = 'Factsheet_Attribution'
#     __table_args__ = {'schema': 'Transactions'}

#     Attribution_Id = Column(BigInteger, primary_key=True)
#     Plan_Id = Column(BigInteger, nullable=False)
#     BenchmarkIndices_Id = Column(BigInteger, nullable=False)
#     Period = Column(String(1000, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
#     Dates = Column(String(1000, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
#     Response_Attribution = Column(TEXT(2147483647, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
#     Is_Deleted = Column(Boolean, nullable=False, server_default=text('((0))'))
#     Created_By = Column(BigInteger)
#     Created_Date = Column(DateTime, server_default=text('(getdate())'))
#     UpdatedBy = Column(BigInteger)
#     Updated_Date = Column(DateTime)

# class Fundamentals(Base):
#     __tablename__ = 'Fundamentals'
#     __table_args__ = {'schema': 'Transactions'}

#     Fundamentals_Id = Column(BigInteger, primary_key=True)
#     CO_CODE = Column(BigInteger)
#     ISIN_Code = Column(Unicode(50))
#     PriceDate = Column(DateTime)
#     PE = Column(Float(53))
#     EPS = Column(Float(53))
#     DivYield = Column(Float(53))
#     PBV = Column(Float(53))
#     mcap = Column(Float(53))
#     Is_Deleted = Column(Boolean)
#     Created_By = Column(BigInteger)
#     Created_Date = Column(DateTime)
#     Updated_By = Column(BigInteger)
#     Updated_Date = Column(DateTime)

# class PortfolioSectors(Base):
#     __tablename__ = 'Portfolio_Sectors'
#     __table_args__ = {'schema': 'Transactions'}

#     Portfolio_Sectors_Id = Column(BigInteger, primary_key=True)
#     Plan_Id = Column(BigInteger)
#     Portfolio_Date = Column(DateTime)
#     Sector_Code = Column(Unicode(100))
#     Sector_Name = Column(Unicode(200))
#     Sub_Sector_Name = Column(Unicode(200))
#     Percentage_To_AUM = Column(Numeric(18, 2))