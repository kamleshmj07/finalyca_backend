from datetime import datetime
from flask import Blueprint, current_app, jsonify, request
from fin_models.masters_models import Options
from sqlalchemy import desc, func, or_
from werkzeug.exceptions import BadRequest
from fin_models.transaction_models import PlanProductMapping
from utils import pretty_float, print_query
from fin_models import AMC, BenchmarkIndices, Classification, Fund, MFSecurity, Plans, Product, NAV, FactSheet,TRIReturns, UnderlyingHoldings, AssetClass

from .data_access import *

client_bp = Blueprint("client_bp", __name__)

# TODO: put it in JSONDecoder with decimal
def prettify_date(dt: datetime):
    return dt.strftime('%d %b %Y') if dt else None

@client_bp.route("/api/v1/client/factsheet", methods=['GET'])
def get_factsheet():
    # TODO: if ts_date is not provided, use max(ts_date) for the plan
    obj = dict()

    plan_id = request.args.get('plan_id', type=int, default=None)
    ts = request.args.get('ts', type=str, default=None)
    ts_date = datetime.strptime(ts, '%Y-%m-%d').date()

    q_sql_factsheet = current_app.store.db.query(FactSheet).filter(FactSheet.Plan_Id==plan_id).filter(FactSheet.Is_Deleted!=1)
    if ts_date:
        q_sql_factsheet = q_sql_factsheet.filter(FactSheet.TransactionDate==ts_date)
    else:
        q_sql_factsheet = q_sql_factsheet.filter(FactSheet.TransactionDate==ts_date)

    sql_factsheet = q_sql_factsheet.one_or_none()

    if not ts_date:
        ts_date = sql_factsheet.TransactionDate

    sql_plan1 = current_app.store.db.query(Plans).filter(Plans.Plan_Id==plan_id).one_or_none()

    sql_ppm = current_app.store.db.query(PlanProductMapping).filter(PlanProductMapping.Plan_Id==plan_id).filter(PlanProductMapping.Is_Deleted!=1).one_or_none()

    sql_mfs = current_app.store.db.query(MFSecurity).filter(MFSecurity.MF_Security_Id==sql_plan1.MF_Security_Id).filter(MFSecurity.
    Status_Id==1).one_or_none()
    
    sql_pr = current_app.store.db.query(Product).filter(Product.Product_Id==sql_ppm.Product_Id).filter(Product.Product_Code == sql_factsheet.SourceFlag).one_or_none()

    sql_f = current_app.store.db.query(Fund).filter(Fund.Fund_Id==sql_mfs.Fund_Id).one_or_none()

    sql_c = current_app.store.db.query(Classification).filter(Classification.Classification_Id==sql_mfs.Classification_Id).one_or_none()

    sql_a = current_app.store.db.query(AMC).filter(AMC.AMC_Id==sql_mfs.AMC_Id).filter(AMC.Is_Deleted != 1).one_or_none()
    
    sql_bi = current_app.store.db.query(BenchmarkIndices).filter(BenchmarkIndices.BenchmarkIndices_Id==sql_mfs.BenchmarkIndices_Id).one_or_none()

    sql_tr = current_app.store.db.query(TRIReturns).filter(TRIReturns.TRI_Co_Code==sql_bi.TRI_Co_Code).filter(TRIReturns.TRI_IndexDate==ts_date).one_or_none()

    sql_nav = current_app.store.db.query(NAV).filter(NAV.Plan_Id==plan_id).filter(NAV.NAV_Date==ts_date).filter(NAV.NAV_Type=='P').one_or_none()

    obj["Factsheet_id"] = sql_factsheet.FactSheet_Id
    obj["AsOnDate"] = prettify_date(sql_factsheet.TransactionDate)
    obj["Plan_Id"] = sql_factsheet.Plan_Id
    obj["Plan_Name"] = sql_plan1.Plan_Name
    obj["AssetClass_Id"] = sql_mfs.AssetClass_Id
    obj["Classification_Id"] = sql_mfs.Classification_Id
    obj["AMC_Id"] = sql_mfs.AMC_Id
    obj["Fund_Id"] = sql_mfs.Fund_Id
    obj["BenchmarkIndices_Name"] = sql_bi.BenchmarkIndices_Name
    obj["BenchmarkIndices_Id"] = sql_bi.BenchmarkIndices_Id
    pd_date = sql_factsheet.Portfolio_Date if sql_factsheet.Portfolio_Date else sql_factsheet.TransactionDate
    obj["Portfolio_Date"] = prettify_date(pd_date)
    obj["TransactionDate"] = prettify_date(sql_factsheet.TransactionDate)
    obj["Fees_Structure"] = sql_mfs.Fees_Structure
    obj["InceptionDate"] = prettify_date(sql_mfs.MF_Security_OpenDate)
    obj["HideAttribution"] = sql_f.HideAttribution
    obj["Top_Holding_ToBeShown"] = sql_f.Top_Holding_ToBeShown
    obj["HideHoldingWeightage"] = sql_f.HideHoldingWeightage
    
    obj["WeekHigh_52_Rs"] = pretty_float(sql_factsheet.WeekHigh_52_Rs)
    obj["WeekLow_52_Rs"] = pretty_float(sql_factsheet.WeekLow_52_Rs)
    obj["TotalStocks"] = pretty_float(sql_factsheet.TotalStocks)
    obj["PortfolioP_BRatio"] = pretty_float(sql_factsheet.PortfolioP_BRatio)
    obj["PortfolioP_ERatio"] = pretty_float(sql_factsheet.PortfolioP_ERatio)
    obj["EarningsGrowth_3Yrs_Percent"] = pretty_float(sql_factsheet.EarningsGrowth_3Yrs_Percent)
    obj["AvgCreditRating"] = pretty_float(sql_factsheet.AvgCreditRating)
    obj["ModifiedDuration_yrs"] = pretty_float(sql_factsheet.ModifiedDuration_yrs)
    obj["StandardDeviation"] = pretty_float(sql_factsheet.StandardDeviation)
    obj["SharpeRatio"] = pretty_float(sql_factsheet.SharpeRatio)
    obj["Beta"] = pretty_float(sql_factsheet.Beta)
    obj["R_Squared"] = pretty_float(sql_factsheet.R_Squared)
    obj["Alpha"] = pretty_float(sql_factsheet.Alpha)
    obj["Mean"] = pretty_float(sql_factsheet.Mean)
    obj["Sortino"] = pretty_float(sql_factsheet.Sortino)
    obj["StandardDeviation_1Yr"] = pretty_float(sql_factsheet.StandardDeviation_1Yr)
    obj["SharpeRatio_1Yr"] = pretty_float(sql_factsheet.SharpeRatio_1Yr)
    obj["Beta_1Yr"] = pretty_float(sql_factsheet.Beta_1Yr)
    obj["R_Squared_1Yr"] = pretty_float(sql_factsheet.R_Squared_1Yr)
    obj["Alpha_1Yr"] = pretty_float(sql_factsheet.Alpha_1Yr)
    obj["Mean_1Yr"] = pretty_float(sql_factsheet.Mean_1Yr)
    obj["Sortino_1Yr"] = pretty_float(sql_factsheet.Sortino_1Yr)
    obj["Equity"] = pretty_float(sql_factsheet.Equity)
    obj["Debt"] = pretty_float(sql_factsheet.Debt)
    obj["Cash"] = pretty_float(sql_factsheet.Cash)
    obj["RANKING_RANK_1MONTH"] = pretty_float(sql_factsheet.RANKING_RANK_1MONTH)
    obj["COUNT_1MONTH"] = pretty_float(sql_factsheet.COUNT_1MONTH)
    obj["RANKING_RANK_3MONTH"] = pretty_float(sql_factsheet.RANKING_RANK_3MONTH)
    obj["COUNT_3MONTH"] = pretty_float(sql_factsheet.COUNT_3MONTH)
    obj["RANKING_RANK_6MONTH"] = pretty_float(sql_factsheet.RANKING_RANK_6MONTH)
    obj["COUNT_6MONTH"] = pretty_float(sql_factsheet.COUNT_6MONTH)
    obj["RANKING_RANK_1YEAR"] = pretty_float(sql_factsheet.RANKING_RANK_1YEAR)
    obj["COUNT_1YEAR"] = pretty_float(sql_factsheet.COUNT_1YEAR)
    obj["RANKING_RANK_3YEAR"] = pretty_float(sql_factsheet.RANKING_RANK_3YEAR)
    obj["COUNT_3YEAR"] = pretty_float(sql_factsheet.COUNT_3YEAR)
    obj["RANKING_RANK_5YEAR"] = pretty_float(sql_factsheet.RANKING_RANK_5YEAR)
    obj["COUNT_5YEAR"] = pretty_float(sql_factsheet.COUNT_5YEAR)

    obj["SIP_RETURNS_1YEAR"] = pretty_float(sql_factsheet.SIP_RETURNS_1YEAR)
    obj["SIP_RETURNS_3YEAR"] = pretty_float(sql_factsheet.SIP_RETURNS_3YEAR)
    obj["SIP_RETURNS_5YEAR"] = pretty_float(sql_factsheet.SIP_RETURNS_5YEAR)
    obj["SIP_RANKINGS_1YEAR"] = pretty_float(sql_factsheet.SIP_RANKINGS_1YEAR)
    obj["SIP_RANKINGS_3YEAR"] = pretty_float(sql_factsheet.SIP_RANKINGS_3YEAR)
    obj["SIP_RANKINGS_5YEAR"] = pretty_float(sql_factsheet.SIP_RANKINGS_5YEAR)
    obj["SCHEME_RETURNS_1MONTH"] = pretty_float(sql_factsheet.SCHEME_RETURNS_1MONTH)
    obj["SCHEME_RETURNS_3MONTH"] = pretty_float(sql_factsheet.SCHEME_RETURNS_3MONTH)
    obj["SCHEME_RETURNS_6MONTH"] = pretty_float(sql_factsheet.SCHEME_RETURNS_6MONTH)
    obj["SCHEME_RETURNS_1YEAR"] = pretty_float(sql_factsheet.SCHEME_RETURNS_1YEAR)
    obj["SCHEME_RETURNS_3YEAR"] = pretty_float(sql_factsheet.SCHEME_RETURNS_3YEAR)
    obj["SCHEME_RETURNS_5YEAR"] = pretty_float(sql_factsheet.SCHEME_RETURNS_5YEAR)
    obj["SCHEME_RETURNS_since_inception"] = pretty_float(sql_factsheet.SCHEME_RETURNS_since_inception)

    is_TRI_Co_Code_valid= True
    if not sql_bi.TRI_Co_Code or sql_bi.TRI_Co_Code == "" or sql_bi.TRI_Co_Code == " ":
        is_TRI_Co_Code_valid = False

    if sql_pr.Product_Code == 'PMS' or not is_TRI_Co_Code_valid:
        obj["SCHEME_BENCHMARK_RETURNS_1MONTH"] = pretty_float(sql_factsheet.SCHEME_BENCHMARK_RETURNS_1MONTH)
    else:
        obj["SCHEME_BENCHMARK_RETURNS_1MONTH"] = pretty_float(sql_tr.Return_1Month)

    if sql_pr.Product_Code == 'PMS' or not is_TRI_Co_Code_valid:
        obj["SCHEME_BENCHMARK_RETURNS_3MONTH"] = pretty_float(sql_factsheet.SCHEME_BENCHMARK_RETURNS_3MONTH)
    else:
        obj["SCHEME_BENCHMARK_RETURNS_3MONTH"] = pretty_float(sql_tr.Return_3Month)

    if sql_pr.Product_Code == 'PMS' or not is_TRI_Co_Code_valid:
        obj["SCHEME_BENCHMARK_RETURNS_6MONTH"] = pretty_float(sql_factsheet.SCHEME_BENCHMARK_RETURNS_6MONTH)
    else:
        obj["SCHEME_BENCHMARK_RETURNS_6MONTH"] = pretty_float(sql_tr.Return_6Month)

    if sql_pr.Product_Code == 'PMS' or not is_TRI_Co_Code_valid:
        obj["SCHEME_BENCHMARK_RETURNS_1YEAR"] = pretty_float(sql_factsheet.SCHEME_BENCHMARK_RETURNS_1YEAR)
    else:
        obj["SCHEME_BENCHMARK_RETURNS_1YEAR"] = pretty_float(sql_tr.Return_1Year)

    if sql_pr.Product_Code == 'PMS' or not is_TRI_Co_Code_valid:
        obj["SCHEME_BENCHMARK_RETURNS_3YEAR"] = pretty_float(sql_factsheet.SCHEME_BENCHMARK_RETURNS_3YEAR)
    else:
        obj["SCHEME_BENCHMARK_RETURNS_3YEAR"] = pretty_float(sql_tr.Return_3Year)

    if sql_pr.Product_Code == 'PMS' or not is_TRI_Co_Code_valid:
        obj["SCHEME_BENCHMARK_RETURNS_5YEAR"] = pretty_float(sql_factsheet.SCHEME_BENCHMARK_RETURNS_5YEAR)
    else:
        obj["SCHEME_BENCHMARK_RETURNS_5YEAR"] = pretty_float(0.0)

    if sql_pr.Product_Code == 'PMS' or not is_TRI_Co_Code_valid:
        obj["SCHEME_BENCHMARK_RETURNS_SI"] = pretty_float(sql_factsheet.SCHEME_BENCHMARK_RETURNS_SI)
    else:
        obj["SCHEME_BENCHMARK_RETURNS_SI"] = pretty_float(0.0)

    obj["SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH"] = pretty_float(sql_factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH)
    obj["SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH"] = pretty_float(sql_factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH)
    obj["SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH"] = pretty_float(sql_factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH)
    obj["SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR"] = pretty_float(sql_factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR)
    obj["SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR"] = pretty_float(sql_factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR)
    obj["SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR"] = pretty_float(sql_factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR)

    obj["Active_RETURNS_1MONTH"] = pretty_float(sql_factsheet.SCHEME_RETURNS_1MONTH - sql_factsheet.SCHEME_BENCHMARK_RETURNS_1MONTH)
    obj["Active_RETURNS_3MONTH"] = pretty_float(sql_factsheet.SCHEME_RETURNS_3MONTH - sql_factsheet.SCHEME_BENCHMARK_RETURNS_3MONTH)
    obj["Active_RETURNS_6MONTH"] = pretty_float(sql_factsheet.SCHEME_RETURNS_6MONTH - sql_factsheet.SCHEME_BENCHMARK_RETURNS_6MONTH)
    obj["Active_RETURNS_1Year"] = pretty_float(sql_factsheet.SCHEME_RETURNS_1YEAR - sql_factsheet.SCHEME_BENCHMARK_RETURNS_1YEAR)
    obj["Active_RETURNS_3Year"] = pretty_float(sql_factsheet.SCHEME_RETURNS_3YEAR - sql_factsheet.SCHEME_BENCHMARK_RETURNS_3YEAR)
    obj["Active_RETURNS_5Year"] = pretty_float(sql_factsheet.SCHEME_RETURNS_5YEAR - sql_factsheet.SCHEME_BENCHMARK_RETURNS_5YEAR)
    obj["Active_RETURNS_SI"] = pretty_float(sql_factsheet.SCHEME_RETURNS_since_inception - sql_factsheet.SCHEME_BENCHMARK_RETURNS_SI)

    obj["Risk_Grade"] = sql_factsheet.Risk_Grade
    obj["Return_Grade"] = sql_factsheet.Return_Grade
    obj["Exit_Load"] = sql_factsheet.Exit_Load

    obj["ExpenseRatio"] = pretty_float(sql_factsheet.ExpenseRatio)
    obj["SOV"] = pretty_float(sql_factsheet.SOV)
    obj["AAA"] = pretty_float(sql_factsheet.AAA)
    obj["A1_Plus"] = pretty_float(sql_factsheet.A1_Plus)
    obj["AA"] = pretty_float(sql_factsheet.AA)
    obj["A_and_Below"] = pretty_float(sql_factsheet.A_and_Below)
    obj["Bill_Rediscounting"] = pretty_float(sql_factsheet.Bill_Rediscounting)
    obj["Cash_Equivalent"] = pretty_float(sql_factsheet.Cash_Equivalent)
    obj["Term_Deposit"] = pretty_float(sql_factsheet.Term_Deposit)
    obj["Unrated_Others"] = pretty_float(sql_factsheet.Unrated_Others)
    obj["Bonds_Debentures"] = pretty_float(sql_factsheet.Bonds_Debentures)
    obj["Cash_And_Cash_Equivalent"] = pretty_float(sql_factsheet.Cash_And_Cash_Equivalent)
    obj["CP_CD"] = pretty_float(sql_factsheet.CP_CD)
    obj["GOI_Securities"] = pretty_float(sql_factsheet.GOI_Securities)
    obj["MutualFunds_Debt"] = pretty_float(sql_factsheet.MutualFunds_Debt)
    obj["Securitised_Debt"] = pretty_float(sql_factsheet.Securitised_Debt)
    obj["ShortTerm_Debt"] = pretty_float(sql_factsheet.ShortTerm_Debt)
    obj["Term_Deposits"] = pretty_float(sql_factsheet.Term_Deposits)
    obj["Treasury_Bills"] = pretty_float(sql_factsheet.Treasury_Bills)
    obj["VRRatings"] = pretty_float(sql_factsheet.VRRatings)
    obj["NetAssets_Rs_Cr"] = pretty_float(sql_factsheet.NetAssets_Rs_Cr)
    obj["AvgMktCap_Rs_Cr"] = pretty_float(sql_factsheet.AvgMktCap_Rs_Cr)
    obj["AvgMaturity_Yrs"] = pretty_float(sql_factsheet.AvgMaturity_Yrs)
    obj["SourceFlag"] = sql_factsheet.SourceFlag
    obj["Is_Deleted"] = sql_factsheet.Is_Deleted
    obj["FundManager_Name"] = ""    # as per the DB Stored procedure
    obj["Classification_Name"] = sql_c.Classification_Name
    obj["MF_Security_Investment_Strategy"] = sql_mfs.MF_Security_Investment_Strategy
    obj["MF_Security_Min_Purchase_Amount"] = pretty_float(sql_mfs.MF_Security_Min_Purchase_Amount)
    # TODO: Convert following value to words. use some localization library instead of doing complex non-sense.
    obj["MF_Security_Min_Purchase_AmountinWords"] = sql_mfs.MF_Security_Min_Purchase_Amount
    obj["Portfolio_Dividend_Yield"] = pretty_float(sql_factsheet.Portfolio_Dividend_Yield)
    obj["Churning_Ratio"] = pretty_float(sql_factsheet.Churning_Ratio)
    obj["Portfolio_Sales_Growth_Estimated"] = pretty_float(sql_factsheet.Portfolio_Sales_Growth_Estimated)
    obj["Portfolio_PAT_Growth_Estimated"] = pretty_float(sql_factsheet.Portfolio_PAT_Growth_Estimated)
    obj["Portfolio_Earning_Growth_Estimated"] = pretty_float(sql_factsheet.Portfolio_Earning_Growth_Estimated)
    obj["Portfolio_Forward_PE"] = pretty_float(sql_factsheet.Portfolio_Forward_PE)
    obj["AMC_Logo"] = F"{current_app.config['IMAGE_PATH']}{sql_a.AMC_Logo}"
    obj["Product_Name"] = sql_pr.Product_Name
    obj["Product_Code"] = sql_pr.Product_Code
    obj["NAV"] = sql_nav.NAV if sql_nav else None
    obj["NAV_Date"] = sql_nav.NAV_Date if sql_nav else None

    frm_date = ts_date.replace(day=1) # get first date of the current month
    month_add=frm_date.month + 1
    if month_add > 12:
        to_date = frm_date.replace(year=frm_date.year + 1)
    else:
        to_date = frm_date.replace(month=frm_date.month + 1) # get first date of the next month

    Attribution_ToDate = current_app.store.db.query(func.max(UnderlyingHoldings.Portfolio_Date)).filter(UnderlyingHoldings.Fund_Id==sql_f.Fund_Id).filter(UnderlyingHoldings.Portfolio_Date<=ts_date).scalar()

    Attribution_FromDate = current_app.store.db.query(func.min(UnderlyingHoldings.Portfolio_Date)).filter(UnderlyingHoldings.Fund_Id==sql_f.Fund_Id).filter(UnderlyingHoldings.Portfolio_Date.between(frm_date, to_date)).scalar()

    if sql_bi.Attribution_Flag == 1 and Attribution_FromDate and Attribution_ToDate and sql_f.HideAttribution == 0:
        obj["Attribution_Flag"] = 1
    else:
        obj["Attribution_Flag"] = 0
    obj["Attribution_FromDate"] = Attribution_FromDate
    obj["Attribution_ToDate"] = Attribution_ToDate

    return jsonify(obj)

@client_bp.route("/api/v1/client/amc", methods=['GET'])
def get_client_amc():
    amc_list = list()

    # show only non-deleted AMC and which is having active plans
    sql_amcs = current_app.store.db.query(AMC).join(MFSecurity, MFSecurity.AMC_Id == AMC.AMC_Id).filter(MFSecurity.Status_Id == 1).filter(AMC.Is_Deleted != 1).group_by(AMC).order_by(AMC.AMC_Name).all()

    products = get_products()

    for sql_amc in sql_amcs:
        obj = dict()
        obj["id"] = sql_amc.AMC_Id
        obj["name"] = sql_amc.AMC_Name
        obj["code"] = sql_amc.AMC_Code
        obj["is_active"] = not sql_amc.Is_Deleted
        obj["description"] = sql_amc.AMC_Description
        obj["logo"] =  F"{current_app.config['IMAGE_PATH']}/{sql_amc.AMC_Logo}"
        obj["product_id"] = sql_amc.Product_Id
        obj["product_name"] = products[sql_amc.Product_Id]
        obj["address1"] = sql_amc.Address1
        obj["address2"] = sql_amc.Address2
        obj["website"] = sql_amc.Website_link
        obj["contact_numbers"] = sql_amc.Contact_Numbers
        obj["background"] = sql_amc.AMC_background
        obj["cin"] = sql_amc.Corporate_Identification_Number
        obj["sebi_nr"] = sql_amc.SEBI_Registration_Number
        obj["contact_person"] = sql_amc.Contact_Person
        obj["email_Id"] = sql_amc.Email_Id

        amc_list.append(obj)

    return jsonify(amc_list)

@client_bp.route("/api/v1/client/amc/details", methods=['GET'])
def get_client_amc_details():
    sebi_code = request.args.get('sebi_code')
    amc_id = request.args.get('AMC_Id', type=int, default=None)
    Product_Id = request.args.get('Product_Id', type=int, default=None)
    AssetClass_Id = request.args.get('AssetClass_Id', type=int, default=None)

    if not sebi_code and not amc_id:
        raise BadRequest(description="Please provide sebi code or AMC Id")

    # if not Product_Id:
    #     raise BadRequest(description="Please provide Product Id")

    products = get_products()    
    # in current version we can have multiple AMC of same name.
    amc_meta = dict()
    amc_lookup = dict()
    if sebi_code:
        amc_sqls = current_app.store.db.query(AMC).filter(AMC.SEBI_Registration_Number==sebi_code).filter(AMC.Product_Id==Product_Id).all()
    elif amc_id:
        # amc_sqls = current_app.store.db.query(AMC).filter(AMC.AMC_Id==amc_id).filter(AMC.Product_Id==Product_Id).all()
        amc_sqls = current_app.store.db.query(AMC).filter(AMC.AMC_Id==amc_id).all()


    for sql_amc in amc_sqls:
        amc_meta["id"] = sql_amc.AMC_Id
        amc_meta["name"] = sql_amc.AMC_Name
        amc_meta["code"] = sql_amc.AMC_Code
        amc_meta["is_active"] = not sql_amc.Is_Deleted
        amc_meta["description"] = sql_amc.AMC_Description
        amc_meta["logo"] =  F"{current_app.config['IMAGE_PATH']}/{sql_amc.AMC_Logo}"
        amc_meta["product_id"] = sql_amc.Product_Id
        amc_meta["product_name"] = products[sql_amc.Product_Id]
        # amc_meta["address"] = sql_amc.Address1 + sql_amc.Address2
        amc_meta["address1"] = sql_amc.Address1
        amc_meta["address2"] = sql_amc.Address2
        amc_meta["website"] = sql_amc.Website_link
        amc_meta["contact_numbers"] = sql_amc.Contact_Numbers
        amc_meta["background"] = sql_amc.AMC_background
        amc_meta["cin"] = sql_amc.Corporate_Identification_Number
        amc_meta["sebi_nr"] = sql_amc.SEBI_Registration_Number
        amc_meta["contact_person"] = sql_amc.Contact_Person
        amc_meta["email_Id"] = sql_amc.Email_Id
        amc_meta["facebook_url"] = sql_amc.Facebook_url
        amc_meta["linkedin_url"] = sql_amc.Linkedin_url
        amc_meta["twitter_url"] = sql_amc.Twitter_url

        id = sql_amc.AMC_Id
        name = sql_amc.AMC_Name
        scheme_category = products[sql_amc.Product_Id]
        amc_lookup[id] = scheme_category

    if not amc_lookup:
        raise BadRequest(description="No AMC was found with given SEBI registration Number.")

    # Find MF list and corresponding fund list
    list_of_funds = dict()
    mf_q = current_app.store.db.query(MFSecurity, Classification, Fund, AssetClass).join(Fund, MFSecurity.Fund_Id == Fund.Fund_Id).join(Classification, Classification.Classification_Id==MFSecurity.Classification_Id).join(AssetClass, AssetClass.AssetClass_Id==MFSecurity.AssetClass_Id).filter(MFSecurity.AMC_Id.in_(tuple(amc_lookup.keys()))).filter(MFSecurity.Status_Id==1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1)
    if AssetClass_Id:
        mf_q = mf_q.filter(MFSecurity.AssetClass_Id == AssetClass_Id)
    mf_plans = mf_q.all()
    for mf in mf_plans:
        list_of_funds[mf[0].Fund_Id] = {
        # list_of_funds[mf[0].MF_Security_Id] = {
            "amc_id": mf[0].AMC_Id,
            "asset_class_id": mf[0].AssetClass_Id,
            "asset_class_name": mf[3].AssetClass_Name,
            "classification_id": mf[1].Classification_Id,
            "classification_name": mf[1].Classification_Name,
            "fund_name": mf[2].Fund_Name,
            "fund_description": mf[2].Fund_Description,
            "fund_code": mf[2].Fund_Code,
            "fund_offerlink": mf[2].Fund_OfferLink,
        }

    # sql_funds = current_app.store.db.query(MFSecurity.Fund_Id, func.min(Plans.Plan_Id), Plans.Plan_Name).join(Plans, Plans.MF_Security_Id==MFSecurity.MF_Security_Id).join(Options, Options.Option_Id==Plans.Option_Id).filter(MFSecurity.Fund_Id.in_(tuple(list_of_funds.keys()))).filter(MFSecurity.Status_Id == 1).filter(Options.Option_Name.like("%G%")).filter(Plans.Is_Deleted == 0).filter(Plans.PlanType_Id == 1).group_by(MFSecurity.Fund_Id, Plans.Plan_Name).all()
    # funds = list()
    # for sql_obj in sql_funds:
    #     json_obj = dict()
    #     fund_id = sql_obj[0]
    #     json_obj["Plan_Id"] = sql_obj[1]
    #     json_obj["Plan_Name"] = sql_obj[2]
        
    #     amc_id = list_of_funds[fund_id]["amc_id"]
    #     scheme_category = amc_lookup[amc_id]
    #     json_obj["id"] = fund_id
    #     json_obj["name"] = list_of_funds[fund_id]["fund_name"] 
    #     json_obj["description"] = list_of_funds[fund_id]["fund_description"]
    #     json_obj["code"] = list_of_funds[fund_id]["fund_code"]
    #     json_obj["website"] = list_of_funds[fund_id]["fund_offerlink"]
    #     json_obj["classification_id"] = list_of_funds[fund_id]["classification_id"]
    #     json_obj["classification_name"] = list_of_funds[fund_id]["classification_name"]
    #     json_obj["scheme_category"] = scheme_category
    #     json_obj["fund_managers"] = get_fund_managers(fund_id)

    #     # json_obj["Plan_Id"] = sql_obj[1].Plan_Id
    #     # json_obj["Plan_Name"] = sql_obj[1].Plan_Name
        
    #     funds.append(json_obj)

    # TODO: Because of some reasons, above query of grouping with Fund id and Plan name is not working as expected. If we are able to optimize it, we can replace following 2 queries.
    # We just want the min plan id per fund
    qq = current_app.store.db.query(MFSecurity.Fund_Id, func.min(Plans.Plan_Id)).join(Plans, Plans.MF_Security_Id==MFSecurity.MF_Security_Id).join(Options, Options.Option_Id==Plans.Option_Id).filter(MFSecurity.Fund_Id.in_(tuple(list_of_funds.keys()))).filter(MFSecurity.Status_Id == 1).filter(Options.Option_Name.like("%G%")).filter(Plans.Is_Deleted == 0).filter(Plans.PlanType_Id == 1).group_by(MFSecurity.Fund_Id)
    # print_query(qq)
    sql_plans_ids = qq.all()

    plans_ids = list()
    for sql_plan_id in sql_plans_ids:
        fund_id = sql_plan_id[0]
        min_plan_id = sql_plan_id[1]
        plans_ids.append(min_plan_id)
    
    sql_funds = current_app.store.db.query(MFSecurity.Fund_Id, Plans.Plan_Id, Plans.Plan_Name).join(Plans, Plans.MF_Security_Id==MFSecurity.MF_Security_Id).filter(Plans.Plan_Id.in_(tuple(plans_ids))).all()
    funds = list()
    for sql_obj in sql_funds:
        json_obj = dict()
        fund_id = sql_obj[0]
        json_obj["Plan_Id"] = sql_obj[1]
        json_obj["Plan_Name"] = sql_obj[2]
        
        amc_id = list_of_funds[fund_id]["amc_id"]
        scheme_category = amc_lookup[amc_id]
        json_obj["id"] = fund_id
        json_obj["name"] = list_of_funds[fund_id]["fund_name"] 
        json_obj["description"] = list_of_funds[fund_id]["fund_description"]
        json_obj["code"] = list_of_funds[fund_id]["fund_code"]
        json_obj["website"] = list_of_funds[fund_id]["fund_offerlink"]
        json_obj["classification_id"] = list_of_funds[fund_id]["classification_id"]
        json_obj["asset_class_id"] = list_of_funds[fund_id]["asset_class_id"]
        json_obj["asset_class_name"] = list_of_funds[fund_id]["asset_class_name"]        
        json_obj["classification_name"] = list_of_funds[fund_id]["classification_name"]
        json_obj["scheme_category"] = scheme_category
        json_obj["fund_managers"] = get_fund_managers(fund_id, get_fund_manager_code=True)

        # json_obj["Plan_Id"] = sql_obj[1].Plan_Id
        # json_obj["Plan_Name"] = sql_obj[1].Plan_Name
        
        funds.append(json_obj)

    resp = dict()
    resp.update(amc_meta)
    resp["funds"] = funds
    return jsonify(resp)


    # products = dict()
    # sql_products = current_app.store.db.query(Product).all()
    # for sql_product in sql_products:
    #     products[sql_product.Product_Id] = sql_product.Product_Name

    # obj = dict()
    # sql_amc = current_app.store.db.query(AMC).filter(AMC.SEBI_Registration_Number==sebi_nr).one_or_none()
    
    # list_of_funds = dict()
    # mf_plans = current_app.store.db.query(MFSecurity).filter(MFSecurity.AMC_Id == sql_amc.AMC_Id).filter(MFSecurity.Status_Id==1).all()
    # for mf in mf_plans:
    #     list_of_funds[mf.Fund_Id] = mf.AMC_Id

    # sql_funds = current_app.store.db.query(Fund).filter(Fund.Fund_Id.in_(tuple(list_of_funds.keys()))).filter(Fund.Is_Deleted!=1).all()
    # resp = list()
    # for sql_obj in sql_funds:
    #     json_obj = dict()
    #     fund_id = sql_obj.Fund_Id
    #     amc_id = list_of_funds[fund_id]
    #     amc_obj = amc_lookup[amc_id]
    #     scheme_category = amc_obj["scheme"]
    #     json_obj["id"] = sql_obj.Fund_Id
    #     json_obj["name"] = sql_obj.Fund_Name
    #     json_obj["description"] = sql_obj.Fund_Description
    #     json_obj["code"] = sql_obj.Fund_Code
    #     json_obj["website"] = sql_obj.Fund_OfferLink
    #     json_obj["scheme_category"] = scheme_category
    #     resp.append(json_obj)


    # for sql_amc in sql_amcs:
    #     obj = dict()
    #     obj["id"] = sql_amc.AMC_Id
    #     obj["name"] = sql_amc.AMC_Name
    #     obj["code"] = sql_amc.AMC_Code
    #     obj["is_active"] = not sql_amc.Is_Deleted
    #     obj["description"] = sql_amc.AMC_Description
    #     obj["logo"] =  F"{current_app.config['IMAGE_PATH']}/{sql_amc.AMC_Logo}"
    #     obj["product_id"] = sql_amc.Product_Id
    #     obj["product_name"] = products[sql_amc.Product_Id]
    #     obj["address1"] = sql_amc.Address1
    #     obj["address2"] = sql_amc.Address2
    #     obj["website"] = sql_amc.Website_link
    #     obj["contact_numbers"] = sql_amc.Contact_Numbers
    #     obj["background"] = sql_amc.AMC_background
    #     obj["cin"] = sql_amc.Corporate_Identification_Number
    #     obj["sebi_nr"] = sql_amc.SEBI_Registration_Number
    #     obj["contact_person"] = sql_amc.Contact_Person
    #     obj["email_Id"] = sql_amc.Email_Id


    # return jsonify(obj)