from datetime import datetime
from flask import Blueprint, current_app, jsonify, request, send_file
from sqlalchemy import desc, or_, and_
from werkzeug.exceptions import BadRequest
from utils import pretty_float
from fin_models import AMC, BenchmarkIndices, Classification, Fund, FundManager, MFSecurity, Plans, Product, NAV, FactSheet, PortfolioSectors, TRIReturns, UnderlyingHoldings, PortfolioAnalysis
from .data_access import *

api_bp = Blueprint("api_bp", __name__)

from .utils import required_access_l1, required_access_l2, required_access_l3

@api_bp.route('/test')
@required_access_l3
def test_fun1():
    return jsonify("Testing if access level is working.")

@api_bp.route('/api/v1/amc/list')
def get_amc_list():
    resp = dict()

    amc_sql = current_app.store.db.query(AMC.SEBI_Registration_Number.distinct(), AMC.AMC_Name).filter(AMC.Is_Deleted != 1).filter(AMC.Product_Id.in_([4,5])).all()
    for sql_obj in amc_sql:
        resp[sql_obj[0]] = sql_obj[1]

    return jsonify(resp)

@api_bp.route('/api/v1/amc/details/<sebi_nr>')
def get_amc_details(sebi_nr):
    resp = dict()
    amc_id_list = list()

    amc_sql = current_app.store.db.query(AMC).filter(AMC.SEBI_Registration_Number == sebi_nr).filter(AMC.Is_Deleted != 1).all()
    for sql_obj in amc_sql:
        amc_id_list.append(sql_obj.AMC_Id)

        add = sql_obj.Address1 if sql_obj.Address1 else ""
        add = add + sql_obj.Address2 if sql_obj.Address2 else ""
        resp["name"] = sql_obj.AMC_Name
        resp["logo"] = sql_obj.AMC_Logo        
        resp["description"] = sql_obj.AMC_Description
        resp["sebi_code"] = sql_obj.SEBI_Registration_Number
        resp["background"] = sql_obj.AMC_background
        resp["address"] = add
        resp["website"] = sql_obj.Website_link
        resp["email"] = sql_obj.Email_Id
        resp["contact_number"] = sql_obj.Contact_Numbers
        resp["contact_person"] = sql_obj.Contact_Person

    sql_objs = current_app.store.db.query(Fund.Fund_Name, Product.Product_Name, Classification.Classification_Name, BenchmarkIndices.BenchmarkIndices_Name, MFSecurity.MF_Security_Id, AMC).join(MFSecurity, and_(MFSecurity.AMC_Id==AMC.AMC_Id, MFSecurity.Is_Deleted != 1)).join(Fund, and_(MFSecurity.Fund_Id==Fund.Fund_Id, Fund.Is_Deleted != 1)).join(Product, and_(AMC.Product_Id==Product.Product_Id, Product.Is_Deleted != 1)).join(Classification, and_(Classification.Classification_Id==MFSecurity.Classification_Id, Classification.Is_Deleted != 1)).join(BenchmarkIndices, and_(BenchmarkIndices.BenchmarkIndices_Id==MFSecurity.BenchmarkIndices_Id, BenchmarkIndices.Is_Deleted != 1)).filter(AMC.SEBI_Registration_Number == sebi_nr).filter(AMC.Is_Deleted != 1).all()

    objs = list()
    for sql_obj in sql_objs:
        obj = dict()
        obj["scheme"] = sql_obj[0]
        obj["type"] = sql_obj[1]
        obj["classification"] = sql_obj[2]
        obj["benchmark"] = sql_obj[3]
        mf_security_id = sql_obj[4]

        sql_results = current_app.store.db.query(FactSheet.TransactionDate, FactSheet.SCHEME_RETURNS_1MONTH, FactSheet.SCHEME_RETURNS_3MONTH, FactSheet.SCHEME_RETURNS_6MONTH, FactSheet.SCHEME_RETURNS_1YEAR, FactSheet.SCHEME_RETURNS_3YEAR, FactSheet.SCHEME_RETURNS_5YEAR, FactSheet.SCHEME_RETURNS_since_inception).join(Plans, FactSheet.Plan_Id == Plans.Plan_Id).join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id).filter(MFSecurity.MF_Security_Id==mf_security_id).filter(FactSheet.Is_Deleted != 1).order_by(FactSheet.TransactionDate.desc()).limit(1).all()

        if len(sql_results):
            sql_results= sql_results[0]
        else:
            # Do not use the scheme as we do not have any data
            continue

        obj["transaction_date"] = sql_results[0]
        obj["scheme_return_1_month"] = sql_results[1]
        obj["scheme_return_3_month"] = sql_results[2]
        obj["scheme_return_6_month"] = sql_results[3]
        obj["scheme_return_1_year"] = sql_results[4]
        obj["scheme_return_3_year"] = sql_results[5]
        obj["scheme_return_5_year"] = sql_results[6]
        obj["scheme_return_si"] = sql_results[7]

        objs.append(obj)
    
    resp["schemes"] = objs
    return jsonify(resp)


@api_bp.route('/api/v1/amc')
def get_amc():
    resp = list()

    amc_sql = current_app.store.db.query(AMC).filter(AMC.Is_Deleted != 1).all()
    for sql_obj in amc_sql:
        json_obj = dict()
        json_obj["id"] = sql_obj.AMC_Id
        json_obj["name"] = sql_obj.AMC_Name
        json_obj["description"] = sql_obj.AMC_Description
        json_obj["cin"] = sql_obj.Corporate_Identification_Number
        json_obj["sebi_code"] = sql_obj.SEBI_Registration_Number
        json_obj["background"] = sql_obj.AMC_background
        add = sql_obj.Address1 if sql_obj.Address1 else ""
        add = add + sql_obj.Address2 if sql_obj.Address2 else ""
        json_obj["address"] = add
        json_obj["website"] = sql_obj.Website_link
        json_obj["contact_number"] = sql_obj.Contact_Numbers
        json_obj["contact_person"] = sql_obj.Contact_Person
        json_obj["email"] = sql_obj.Email_Id
        resp.append(json_obj)

    return jsonify(resp)

@api_bp.route('/api/v1/scheme')
def get_schemes():
    sebi_code = request.args.get('sebi_code')
    
    # in current version we can have multiple AMC of same name.
    amc_lookup = dict()
    if sebi_code:
        amc_sqls = current_app.store.db.query(AMC).filter(AMC.SEBI_Registration_Number==sebi_code).all()
        for amc_sql in amc_sqls:
            id = amc_sql.AMC_Id
            name = amc_sql.AMC_Name
            scheme_id = amc_sql.Product_Id
            product_sql = current_app.store.db.query(Product).filter(Product.Product_Id==scheme_id).one_or_none()
            scheme_category = product_sql.Product_Name
            amc_lookup[id] = {"name": name, "scheme": scheme_category}

    if not amc_lookup:
        raise BadRequest(description="No AMC was found with given SEBI registration Number.")

    # Find MF list and corresponding fund list
    list_of_funds = dict()
    mf_plans = current_app.store.db.query(MFSecurity).filter(MFSecurity.AMC_Id.in_(tuple(amc_lookup.keys()))).filter(MFSecurity.Status_Id==1).all()
    for mf in mf_plans:
        list_of_funds[mf.Fund_Id] = mf.AMC_Id

    sql_funds = current_app.store.db.query(Fund).filter(Fund.Fund_Id.in_(tuple(list_of_funds.keys()))).filter(Fund.Is_Deleted!=1).all()
    resp = list()
    for sql_obj in sql_funds:
        json_obj = dict()
        fund_id = sql_obj.Fund_Id
        amc_id = list_of_funds[fund_id]
        amc_obj = amc_lookup[amc_id]
        scheme_category = amc_obj["scheme"]
        json_obj["id"] = sql_obj.Fund_Id
        json_obj["name"] = sql_obj.Fund_Name
        json_obj["description"] = sql_obj.Fund_Description
        json_obj["code"] = sql_obj.Fund_Code
        json_obj["website"] = sql_obj.Fund_OfferLink
        json_obj["scheme_category"] = scheme_category
        resp.append(json_obj)

    return jsonify(resp)

@api_bp.route('/api/v1/pms')
def get_pms_info():
    json_obj = dict()

    scheme_id = request.args.get('scheme_id', type=int)
    # ts = request.args.get('ts', type=str)
    # date = datetime.strptime(ts, '%Y%m%d').date()

    # Find the relavent plan id for the factsheet. for pms and aif we just read first value.
    sql_fund = current_app.store.db.query(Fund).filter(Fund.Fund_Id == scheme_id).first()

    sql_mf = current_app.store.db.query(MFSecurity).filter(MFSecurity.Fund_Id == scheme_id).first()

    sql_amc = current_app.store.db.query(AMC).filter(AMC.AMC_Id == sql_mf.AMC_Id).first()

    sql_classification = current_app.store.db.query(Classification).filter(Classification.Classification_Id == sql_mf.Classification_Id).first()

    sql_benchmark = current_app.store.db.query(BenchmarkIndices).filter(BenchmarkIndices.BenchmarkIndices_Id == sql_mf.BenchmarkIndices_Id).first()

    sql_plan = current_app.store.db.query(Plans).filter(Plans.MF_Security_Id == sql_mf.MF_Security_Id).first()

    # Always send the last one
    # sql_obj = current_app.store.db.query(FactSheet).filter(FactSheet.Plan_Id==sql_plan.Plan_Id).filter(FactSheet.TransactionDate==date).filter(FactSheet.Is_Deleted!=1).one_or_none()
    sql_objs = current_app.store.db.query(FactSheet).filter(FactSheet.Plan_Id==sql_plan.Plan_Id).filter(FactSheet.Is_Deleted!=1).order_by(desc(FactSheet.TransactionDate)).limit(1).all()

    if sql_objs:
        sql_obj = sql_objs[0]
        json_obj["name"] = sql_fund.Fund_Name
        json_obj["amc"] = sql_amc.AMC_Name
        json_obj["as_on_date"] = sql_obj.Portfolio_Date
        json_obj["net_assets"] = pretty_float(sql_obj.NetAssets_Rs_Cr)

        json_obj["fund_manager"] = list(get_fund_managers(scheme_id).values())

        json_obj["classification"] = sql_classification.Classification_Name
        json_obj["benchmark"] = sql_benchmark.BenchmarkIndices_Name
        json_obj["expense_ratio"] = sql_obj.ExpenseRatio
        json_obj["fee_structure"] = sql_mf.Fees_Structure
        json_obj["exit_load"] = sql_obj.Exit_Load
        
        sql_pas = current_app.store.db.query(PortfolioAnalysis).filter(PortfolioAnalysis.Plan_Id==sql_obj.Plan_Id).filter(PortfolioAnalysis.Portfolio_Date==sql_obj.Portfolio_Date).all()
        # invest = dict()
        # for sql_pa in sql_pas:
        #     if not sql_pa.Attribute_Sub_Text:
        #         continue
        #     if sql_pa.Attribute_Text not in invest:
        #         invest[sql_pa.Attribute_Text] = dict()
        #     invest[sql_pa.Attribute_Text][sql_pa.Attribute_Sub_Text] = sql_pa.Attribute_Value
        
        invest = {
            "Large Cap": {"Blend": 0.00, "Growth": 0.0, "Value": 0.0}, 
            "Mid Cap": {"Blend": 0.00, "Growth": 0.0, "Value": 0.0}, 
            "Small Cap": {"Blend": 0.00, "Growth": 0.0, "Value": 0.0}
        }
        for sql_pa in sql_pas:
            if not sql_pa.Attribute_Sub_Text:
                continue
            invest[sql_pa.Attribute_Text][sql_pa.Attribute_Sub_Text] = pretty_float(sql_pa.Attribute_Value)

        json_obj["investment_style"] = invest

        scheme_perfor = dict()
        scheme_perfor["3_months"] = pretty_float(sql_obj.SCHEME_RETURNS_3MONTH)
        scheme_perfor["6_months"] = pretty_float(sql_obj.SCHEME_RETURNS_6MONTH)
        scheme_perfor["12_months"] = pretty_float(sql_obj.SCHEME_RETURNS_1YEAR)
        scheme_perfor["3_yr"] = pretty_float(sql_obj.SCHEME_RETURNS_3YEAR)
        scheme_perfor["5_yr"] = pretty_float(sql_obj.SCHEME_RETURNS_5YEAR)
        scheme_perfor["since_inception"] = pretty_float(sql_obj.SCHEME_RETURNS_since_inception)
        json_obj["fund_performance"] = scheme_perfor

        benchmark_perfor = dict()
        benchmark_perfor["3_months"] = pretty_float(sql_obj.SCHEME_BENCHMARK_RETURNS_3MONTH)
        benchmark_perfor["6_months"] = pretty_float(sql_obj.SCHEME_BENCHMARK_RETURNS_6MONTH)
        benchmark_perfor["12_months"] = pretty_float(sql_obj.SCHEME_BENCHMARK_RETURNS_1YEAR)
        benchmark_perfor["3_yr"] = pretty_float(sql_obj.SCHEME_BENCHMARK_RETURNS_3YEAR)
        benchmark_perfor["5_yr"] = pretty_float(sql_obj.SCHEME_BENCHMARK_RETURNS_5YEAR)
        benchmark_perfor["since_inception"] = pretty_float(sql_obj.SCHEME_BENCHMARK_RETURNS_SI)
        json_obj["benchmark_performance"] = benchmark_perfor

        json_obj["benchmark_performance"] = benchmark_perfor
        json_obj["std"] = pretty_float(sql_obj.StandardDeviation)
        json_obj["alpha"] = pretty_float(sql_obj.Alpha)
        json_obj["beta"] = pretty_float(sql_obj.Beta)

        sql_nav = current_app.store.db.query(NAV)\
                                    .filter(NAV.Plan_Id==sql_obj.Plan_Id,
                                            NAV.NAV_Date==sql_obj.Portfolio_Date).one_or_none()

        json_obj["nav"] = sql_nav.NAV if sql_nav else None
        json_obj["aum"] = sql_obj.NetAssets_Rs_Cr

        sql_q = current_app.store.db.query(PortfolioSectors).filter(PortfolioSectors.Plan_Id==sql_obj.Plan_Id).filter(PortfolioSectors.Portfolio_Date==sql_obj.Portfolio_Date)
        # print_query(sql_q)
        sql_sectors = sql_q.all()
        sectors = dict()
        for sql_se in sql_sectors:
            if not sql_se.Is_Deleted:
                if sql_se.Sector_Name in sectors:
                    sectors[sql_se.Sector_Name] = pretty_float(sectors[sql_se.Sector_Name]) + pretty_float(sql_se.Percentage_To_AUM)
                else:
                    sectors[sql_se.Sector_Name] = pretty_float(sql_se.Percentage_To_AUM)
        json_obj["sectors"] = sectors

        holdings = dict()
        sql_holdings = current_app.store.db.query(UnderlyingHoldings).filter(UnderlyingHoldings.Fund_Id==scheme_id).filter(UnderlyingHoldings.Portfolio_Date==sql_obj.Portfolio_Date).filter(or_(UnderlyingHoldings.Is_Deleted==0, UnderlyingHoldings.Is_Deleted==None)).order_by(desc(UnderlyingHoldings.Percentage_to_AUM)).limit(5).all()
        for sql_holding in sql_holdings:
            holdings[sql_holding.Company_Security_Name] = pretty_float(sql_holding.Percentage_to_AUM)
        json_obj["holdings"] = holdings

    return jsonify(json_obj)

@api_bp.route('/api/v1/mf_plans')
def get_mf_plans():
    resp = list()
    scheme_id = request.args.get('scheme_id', type=int)

    mf_ids = list()
    sql_mf = current_app.store.db.query(MFSecurity).filter(MFSecurity.Fund_Id == scheme_id).all()
    for sql_m in sql_mf:
        mf_ids.append(sql_m.MF_Security_Id)

    sql_plans = current_app.store.db.query(Plans).filter(Plans.MF_Security_Id.in_(mf_ids)).all()
    for sql_plan in sql_plans:
        obj = dict()
        obj["name"] = sql_plan.Plan_Name
        obj["code"] = sql_plan.Plan_Code
        obj["type"] = sql_plan.PlanType_Id
        obj["option"] = sql_plan.Option_Id
        obj["isin"] = sql_plan.ISIN
        obj["amfi_code"] = sql_plan.AMFI_Code
        obj["amfi_name"] = sql_plan.AMFI_Name
        obj["rta_code"] = sql_plan.RTA_Code
        obj["rta_name"] = sql_plan.RTA_Name
        resp.append(obj)

    return jsonify(resp)

@api_bp.route('/api/v1/mf')
def get_mf_info():
    isin = request.args.get('isin', type=str)
    amfi_code = request.args.get('amfi_code', type=str)
    ts = request.args.get('ts', type=str)
    date = datetime.strptime(ts, '%Y%m%d').date()

    if not isin and not amfi_code:
        return BadRequest(description="Please provide ISIN or AMFI code for the mutual fund scheme.")

    if isin and amfi_code:
        return BadRequest(description="Please provide Either ISIN or AMFI code for the mutual fund scheme.")

    if not ts:
        return BadRequest(description="Please provide date for the factsheet the mutual fund scheme.")

    plan_query = current_app.store.db.query(Plans)
    if isin:
        plan_query = plan_query.filter(Plans.ISIN==isin)
    if amfi_code:
        plan_query = plan_query.filter(Plans.AMFI_Code==amfi_code)
    sql_plan = plan_query.first()

    # Find the relavent plan id for the factsheet. for pms and aif we just read first value.
    sql_mf = current_app.store.db.query(MFSecurity).filter(MFSecurity.MF_Security_Id == sql_plan.MF_Security_Id).first()

    sql_fund = current_app.store.db.query(Fund).filter(Fund.Fund_Id == sql_mf.Fund_Id).first()
    scheme_id = sql_fund.Fund_Id

    sql_amc = current_app.store.db.query(AMC).filter(AMC.AMC_Id == sql_mf.AMC_Id).first()

    sql_classification = current_app.store.db.query(Classification).filter(Classification.Classification_Id == sql_mf.Classification_Id).first()

    sql_benchmark = current_app.store.db.query(BenchmarkIndices).filter(BenchmarkIndices.BenchmarkIndices_Id == sql_mf.BenchmarkIndices_Id).first()

    sql_plan = current_app.store.db.query(Plans).filter(Plans.MF_Security_Id == sql_mf.MF_Security_Id).first()

    sql_obj = current_app.store.db.query(FactSheet).filter(FactSheet.Plan_Id==sql_plan.Plan_Id).filter(FactSheet.TransactionDate==date).filter(FactSheet.Is_Deleted!=1).one_or_none()

    json_obj = dict()

    if sql_obj:
        json_obj["name"] = sql_fund.Fund_Name
        json_obj["amc"] = sql_amc.AMC_Name
        json_obj["plan_name"] = sql_plan.Plan_Name
        json_obj["as_on_date"] = sql_obj.Portfolio_Date

        json_obj["net_assets"] = pretty_float(sql_obj.NetAssets_Rs_Cr)

        json_obj["fund_manager"] = list(get_fund_managers(scheme_id).values())

        json_obj["classification"] = sql_classification.Classification_Name
        json_obj["benchmark"] = sql_benchmark.BenchmarkIndices_Name
        json_obj["expense_ratio"] = sql_obj.ExpenseRatio
        json_obj["fee_structure"] = sql_mf.Fees_Structure
        json_obj["exit_load"] = sql_obj.Exit_Load
        
        sql_pas = current_app.store.db.query(PortfolioAnalysis).filter(PortfolioAnalysis.Plan_Id==sql_obj.Plan_Id).filter(PortfolioAnalysis.Portfolio_Date==sql_obj.Portfolio_Date).all()
        # invest = dict()
        # for sql_pa in sql_pas:
        #     if not sql_pa.Attribute_Sub_Text:
        #         continue
        #     if sql_pa.Attribute_Text not in invest:
        #         invest[sql_pa.Attribute_Text] = dict()
        #     invest[sql_pa.Attribute_Text][sql_pa.Attribute_Sub_Text] = sql_pa.Attribute_Value
        
        invest = {
            "Large Cap": {"Blend": 0.00, "Growth": 0.0, "Value": 0.0}, 
            "Mid Cap": {"Blend": 0.00, "Growth": 0.0, "Value": 0.0}, 
            "Small Cap": {"Blend": 0.00, "Growth": 0.0, "Value": 0.0}
        }
        for sql_pa in sql_pas:
            if not sql_pa.Attribute_Sub_Text:
                continue
            invest[sql_pa.Attribute_Text][sql_pa.Attribute_Sub_Text] = pretty_float(sql_pa.Attribute_Value)

        json_obj["investment_style"] = invest

        scheme_perfor = dict()
        scheme_perfor["1_month"] = pretty_float(sql_obj.SCHEME_RETURNS_1MONTH)
        scheme_perfor["3_months"] = pretty_float(sql_obj.SCHEME_RETURNS_3MONTH)
        scheme_perfor["6_months"] = pretty_float(sql_obj.SCHEME_RETURNS_6MONTH)
        scheme_perfor["12_months"] = pretty_float(sql_obj.SCHEME_RETURNS_1YEAR)
        scheme_perfor["3_yr"] = pretty_float(sql_obj.SCHEME_RETURNS_3YEAR)
        scheme_perfor["5_yr"] = pretty_float(sql_obj.SCHEME_RETURNS_5YEAR)
        scheme_perfor["since_inception"] = pretty_float(sql_obj.SCHEME_RETURNS_since_inception)
        json_obj["fund_performance"] = scheme_perfor

        sql_tri_returns = current_app.store.db.query(TRIReturns).filter(TRIReturns.TRI_Co_Code==sql_benchmark.TRI_Co_Code).filter(TRIReturns.TRI_IndexDate==sql_obj.TransactionDate).one_or_none()

        benchmark_perfor = dict()
        benchmark_perfor["1_month"] = pretty_float(sql_tri_returns.Return_1Month)
        benchmark_perfor["3_months"] = pretty_float(sql_tri_returns.Return_3Month)
        benchmark_perfor["6_months"] = pretty_float(sql_tri_returns.Return_6Month)
        benchmark_perfor["12_months"] = pretty_float(sql_tri_returns.Return_1Year)
        benchmark_perfor["3_yr"] = pretty_float(sql_tri_returns.Return_3Year)
        json_obj["benchmark_performance"] = benchmark_perfor

        category_perf = dict()
        category_perf["1_month"] = pretty_float(sql_obj.SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH)
        category_perf["3_months"] = pretty_float(sql_obj.SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH)
        category_perf["6_months"] = pretty_float(sql_obj.SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH)
        category_perf["12_months"] = pretty_float(sql_obj.SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR)
        category_perf["3_yr"] = pretty_float(sql_obj.SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR)
        category_perf["5_yr"] = pretty_float(sql_obj.SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR)
        json_obj["category_performance"] = category_perf

        json_obj["benchmark_performance"] = benchmark_perfor
        json_obj["beta"] = pretty_float(sql_obj.Beta)

        sql_nav = current_app.store.db.query(NAV)\
                                    .filter(NAV.Plan_Id==sql_obj.Plan_Id,
                                            NAV.NAV_Date==sql_obj.Portfolio_Date).one_or_none()

        json_obj["nav"] = sql_nav.NAV if sql_nav else None

        sql_q = current_app.store.db.query(PortfolioSectors).filter(PortfolioSectors.Plan_Id==sql_obj.Plan_Id).filter(PortfolioSectors.Portfolio_Date==sql_obj.Portfolio_Date)
        # print_query(sql_q)
        sql_sectors = sql_q.all()
        sectors = dict()
        for sql_se in sql_sectors:
            if not sql_se.Is_Deleted:
                if sql_se.Sector_Name in sectors:
                    sectors[sql_se.Sector_Name] = pretty_float(sectors[sql_se.Sector_Name]) + pretty_float(sql_se.Percentage_To_AUM)
                else:
                    sectors[sql_se.Sector_Name] = pretty_float(sql_se.Percentage_To_AUM)
        json_obj["sectors"] = sectors

        holdings = dict()
        sql_holdings = current_app.store.db.query(UnderlyingHoldings).filter(UnderlyingHoldings.Fund_Id==scheme_id).filter(UnderlyingHoldings.Portfolio_Date==sql_obj.Portfolio_Date).filter(or_(UnderlyingHoldings.Is_Deleted==0, UnderlyingHoldings.Is_Deleted==None)).order_by(desc(UnderlyingHoldings.Percentage_to_AUM)).limit(5).all()
        for sql_holding in sql_holdings:
            holdings[sql_holding.Company_Security_Name] = pretty_float(sql_holding.Percentage_to_AUM)

        json_obj["holdings"] = holdings

    return jsonify(json_obj)

@api_bp.route('/api/v1/aif')
def get_aif_info():
    scheme_id = request.args.get('scheme_id', type=int)
    ts = request.args.get('ts', type=str)
    msg = F"Wait till we implement the api to provide you factsheet for {scheme_id} for {ts}"
    return jsonify(msg)

@api_bp.route('/api/v1/user_guide', methods=['GET'])
def get_user_guide_pdf():   
    user_guide_pdf = "../sample/user_guide.pdf"
    return send_file(user_guide_pdf, attachment_filename='user_guide.pdf', as_attachment=True)
