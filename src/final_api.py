from datetime import date, datetime, timedelta
from math import trunc
from time import strptime
import pandas as pd
from dateutil import parser
from dateutil.relativedelta import relativedelta
from flask import Blueprint, current_app, jsonify, request, send_file
from itsdangerous import json
from sqlalchemy import and_, desc, extract, func, or_
from werkzeug.exceptions import BadRequest

from fin_models.transaction_models import PlanProductMapping, FactsheetAttribution, FundStocks
from fin_models.masters_models import AssetClass, Options, PlanType, HoldingSecurity, Sector
from fin_models import AMC, BenchmarkIndices, Classification, FundType, Fund, FundManager, MFSecurity, Plans, Product, NAV,\
                        FactSheet, PortfolioSectors, TRIReturns, UnderlyingHoldings, FactsheetAttribution, Sector, PlanProductMapping
from bizlogic.importer_helper import get_fundid_byplanid, get_plan_id, diff_month, generate_attributions, getbetweendate,\
                                    get_requestdata, get_sql_fund_byplanid, getfundmanager, get_fund_holdings,\
                                    get_compositiondata, get_sectorweightsdata, get_fundriskratio_data,\
                                    get_max_indexweightage_bydate, get_aumandfundcountbyproduct, get_aum_monthwise,\
                                    get_co_code_by_plan_id, get_portfolio_characteristics, get_marketcap_composition,\
                                    get_investmentstyle, get_fund_portfolio_change, get_fund_nav, get_rollingreturn,\
                                    get_attributions, attribution_validations, get_aum_monthwise,\
                                    get_business_day, investmentstyle_month_wise, marketcapcomposition_month_wise, get_instrumenttype,\
                                    calculate_portfolio_level_analysis, get_fund_manager_info_by_code, get_fundmanager_list, \
                                    get_portfolio_date, generate_active_rolling_returns, get_fund_manager_details,get_organization_whitelabel, get_default_attribution_dates
from bizlogic.fund_manager_analysis import fund_manager_fundactivity_by_exposure
from utils.time_func import last_date_of_month
from .utils import required_access_l1, required_access_l2, required_access_l3
from .data_access import *
from data.fundamentals import get_equity_fundamentals
from bizlogic.common_helper import get_fund_historic_performance, get_last_transactiondate, get_detailed_fund_holdings, schedule_email_activity
from src.factsheet_pdf_helper import get_factsheetpdf
from reports.utils import prepare_pdf_from_html
from data.holdings import get_fund_underlying_holdings 

final_api_bp = Blueprint("final_api_bp", __name__)

@final_api_bp.route("/api/v1/product_list", methods=['GET'])
@required_access_l1
def get_product_list():
    resp = list()
 
    sql_products = current_app.store.db.query(Product).filter(Product.Is_Deleted != 1).all()
    for sql_obj in sql_products:
        obj = dict()
        obj["product_id"] = sql_obj.Product_Id
        obj["product_name"] = sql_obj.Product_Name
        obj["product_code"] = sql_obj.Product_Code
        
        resp.append(obj)

    return jsonify(resp)

@final_api_bp.route("/api/v1/amc_list", methods=['GET'])
@required_access_l1
def get_amc_list():
    resp = list()

    product_code = request.args.get("product_code", default=None)

    sql_q = current_app.store.db.query(AMC.AMC_Id, AMC.AMC_Name, AMC.AMC_Logo, AMC.AMC_Code, AMC.SEBI_Registration_Number, Product.Product_Code).join(Product, AMC.Product_Id==Product.Product_Id).filter(AMC.Is_Deleted != 1)

    if product_code:
        sql_q = sql_q.filter(Product.Product_Code==product_code)
 
    amc_sql = sql_q.all()

    for sql_obj in amc_sql:
        obj = dict()
        obj["amc_id"] = sql_obj.AMC_Id
        obj["amc_name"] = sql_obj.AMC_Name
        obj["amc_logo"] = F"{current_app.config['IMAGE_PATH']}{sql_obj.AMC_Logo}"
        obj["amc_code"] = sql_obj.AMC_Code
        obj["amc_sebi_no"] = sql_obj.SEBI_Registration_Number
        obj["product_code"] = sql_obj.Product_Code
        
        resp.append(obj)

    return jsonify(resp)

@final_api_bp.route("/api/v1/amc_info", methods=['GET'])
@required_access_l1
def get_amc_info():
    obj = dict()

    amc_id = request.args.get("amc_id", default=None)
    amc_code = request.args.get("amc_code", default=None)

    if not amc_id and not amc_code:
        raise BadRequest("Please provide amc_id or amc_code")

    sql_q = current_app.store.db.query(AMC.AMC_Id, AMC.AMC_Name, AMC.AMC_Code, AMC.AMC_Description, AMC.Address1, AMC.Address2, AMC.Contact_Person, AMC.Contact_Numbers, AMC.Website_link, AMC.Email_Id, AMC.AMC_Logo, AMC.SEBI_Registration_Number, Product.Product_Code).join(Product, AMC.Product_Id==Product.Product_Id)

    sql_obj = None
    if amc_id:
        sql_obj = sql_q.filter(AMC.AMC_Id == amc_id).one_or_none()
    elif amc_code:
        # We are not sure if AMC Code is unique for PMS and AIF products
        sql_obj = sql_q.filter(AMC.AMC_Code == amc_code).first()

    if sql_obj:
        addr = sql_obj.Address1 if sql_obj.Address1 else ""
        addr += sql_obj.Address2 if sql_obj.Address2 else ""        

        obj["amc_id"] = sql_obj.AMC_Id
        obj["amc_name"] = sql_obj.AMC_Name
        obj["amc_code"] = sql_obj.AMC_Code
        obj["amc_description"] = sql_obj.AMC_Description
        obj["amc_address"] = addr
        obj["amc_contact_person"] = sql_obj.Contact_Person
        obj["amc_contact_number"] = sql_obj.Contact_Numbers
        obj["amc_website"] = sql_obj.Website_link
        obj["amc_email_id"] = sql_obj.Email_Id
        obj["amc_logo"] = F"{current_app.config['IMAGE_PATH']}{sql_obj.AMC_Logo}"
        obj["amc_sebi_no"] = sql_obj.SEBI_Registration_Number
        obj["product_code"] = sql_obj.Product_Code
    
    return jsonify(obj)

@final_api_bp.route("/api/v1/fund_manager_list", methods=['GET'])
@required_access_l1
def get_fund_manager_list():
    resp = list()

    amc_sql = current_app.store.db.query(FundManager.FundManager_Name, FundManager.FundManager_Code).filter(FundManager.Is_Deleted != 1).group_by(FundManager.FundManager_Name, FundManager.FundManager_Code).all()
    for sql_obj in amc_sql:
        obj = dict()
        obj["fund_manager_name"] = sql_obj[0]
        obj["fund_manager_code"] = sql_obj[1]
        
        resp.append(obj)

    return jsonify(resp)


@final_api_bp.route("/api/v1/fund_manager_info", methods=['GET'])
@required_access_l1
def get_fund_manager_info():
    resp = dict()

    fund_manager_code = request.args.get("fund_manager_code", default=None)
    only_active_fund = request.args.get("only_active", default=False)

    if not fund_manager_code:
        raise BadRequest("Please provide fund_manager_code.")

    resp = get_fund_manager_info_by_code(current_app.store.db, fund_manager_code, only_active_fund, current_app.config['IMAGE_PATH'])
    
    return jsonify(resp)


@final_api_bp.route("/api/v1/fund_managers", methods=['GET'])
@required_access_l1
def get_fund_managers():

    fundmanager_name = request.args.get("fundmanager_name", default=None)   # TODO Check if we need this parameter call
    amc_id = request.args.get("amc_id", default=None)
    product_id = request.args.get("product_id", default=None)
    today = datetime.today()

    # sql_subquery = current_app.store.db.query(FundManager.Fund_Id).filter(FundManager.FundManager_Code == )

    sql_fundmanagers = get_fundmanager_list(current_app.store.db, today, amc_id, product_id, fundmanager_name)

    df_fundmgr = pd.DataFrame(sql_fundmanagers)
    df_fundmgr.columns = map(str.lower, df_fundmgr.columns)

    if not df_fundmgr.empty:
        # rename as per the existing expected response
        df_fundmgr.rename(columns={'fundmanager_name': 'fund_manager_name',
                                'fundmanager_code': 'fund_manager_code',
                                'fundmanager_description': 'fund_manager_description',
                                'fundmanager_image': 'fund_manager_image',
                                'amc_name': 'fund_manager_amc',
                                'amc_id': 'fund_manager_amc_id',
                                'aum': 'fund_manager_aum',
                                'funds_managed': 'fund_manager_fund_count'}, inplace=True)

        # TODO: Once fund manager description is ingested by system, map this to appropriate column
        df_fundmgr['fund_manager_description'] = None

        result = df_fundmgr.to_json(orient="records")
        # get a list of fund_ids managed by the fund manager as a separate column
        json_resp = json.loads(result)
        for item in json_resp:
            fundlist = []
            sql_fund_list = current_app.store.db.query(FundManager.Fund_Id)\
                                        .select_from(FundManager)\
                                        .join(Fund, Fund.Fund_Id == FundManager.Fund_Id)\
                                        .join(MFSecurity, MFSecurity.Fund_Id == FundManager.Fund_Id)\
                                        .filter(MFSecurity.Status_Id == 1,
                                                FundManager.Is_Deleted != 1,                                                 
                                                FundManager.Funds_Managed > 0,
                                                FundManager.FundManager_Code == item['fund_manager_code'],
                                                or_(FundManager.DateTo == None, FundManager.DateTo >= today)).all()
            for sql_fund in sql_fund_list:
                fundlist.append(sql_fund.Fund_Id)
            
            item['fund_id'] = fundlist
        
        return jsonify(json_resp)
    
    return jsonify([])

@final_api_bp.route('/api/v1/fund_manager_detail', methods=['GET'])
@required_access_l1
def get_fund_manager_detail():
    fundmanager_code = request.args.get('fund_manager_code', type=str, default=None)
    ts = request.args.get('ts', type=int, default=0)

    if not fundmanager_code:
        raise BadRequest('Fund manager code is required.')

    resp = dict()

    resp = get_fund_manager_details(current_app.store.db, fundmanager_code, ts)    

    return jsonify(resp)


@final_api_bp.route('/api/v1/fund_manager_fundactivity', methods=['GET'])
@required_access_l2
def get_fund_manager_fundactivity():
    fundmanager_code = request.args.get('fund_manager_code', type=str, default=None)

    if not fundmanager_code:
        raise BadRequest('Fund manager code is required.')

    resp = dict()
    db_session = current_app.store.db
    resp["increase_exposure"] = fund_manager_fundactivity_by_exposure(db_session, "Increase_Exposure", fundmanager_code)
    resp["decrease_exposure"] = fund_manager_fundactivity_by_exposure(db_session, "Decrease_Exposure", fundmanager_code)
    resp["new_entrants"] = fund_manager_fundactivity_by_exposure(db_session, "New_Entrants", fundmanager_code)
    resp["complete_exit"] = fund_manager_fundactivity_by_exposure(db_session, "Complete_Exit", fundmanager_code)

    return jsonify(resp)


@final_api_bp.route('/api/v1/fund_manager_cash_level', methods=['GET'])
@required_access_l2
def fund_manager_cash_level():
    fundmanager_code = request.args.get('fund_manager_code', type=str, default=None)
    ts = request.args.get('ts', type=int, default=0)

    if not fundmanager_code:
        raise BadRequest('Fund manager code is required.')

    resp = list()   
    today = datetime.today()

    sql_fund = current_app.store.db.query(Plans.Plan_Id,
                                          Plans.Plan_Name,
                                          Fund.Fund_Id,
                                          Fund.Fund_Name,
                                          FundManager.Funds_Managed,
                                          FundManager.AUM)\
                                    .select_from(FundManager)\
                                    .join(Fund, Fund.Fund_Id == FundManager.Fund_Id)\
                                    .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                    .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1, Plans.PlanType_Id == 1)\
                                    .filter(or_(FundManager.DateTo >= today, FundManager.DateTo == None))\
                                    .filter(FundManager.FundManager_Code == fundmanager_code).filter(FundManager.Is_Deleted != 1).all()

    for fund in sql_fund:
        plan_wise_dict = dict()
        plan_wise_list = list()

        first = today.replace(day=1)
        last_month = first - timedelta(days=1)             
        plan_wise_dict["plan_name"] = fund.Plan_Name

        sql_factsheetdata = current_app.store.db.query(func.max(FactSheet.TransactionDate),
                                                       extract('year', FactSheet.TransactionDate),
                                                       extract('month', FactSheet.TransactionDate),
                                                       func.max(FactSheet.Cash))\
                                                .filter(FactSheet.Plan_Id == fund.Plan_Id, FactSheet.TransactionDate <= last_month, FactSheet.Is_Deleted != 1)\
                                                .group_by(extract('year', FactSheet.TransactionDate), extract('month', FactSheet.TransactionDate))\
                                                .order_by(extract('year', FactSheet.TransactionDate), extract('month', FactSheet.TransactionDate)).all()

        if sql_factsheetdata:
            for sql_factsheet in sql_factsheetdata:
                if sql_factsheet[0]:
                    data_dict = {}

                    enddate = last_date_of_month(sql_factsheet[1], sql_factsheet[2])
                    new_enddate = datetime.combine(enddate,datetime.time(datetime.today()))

                    if ts == 1:
                        data_dict["asofdate"] = trunc(new_enddate.timestamp()) * 1000
                    else:
                        data_dict["asofdate"] = enddate

                    cashlevel = current_app.store.db.query(FactSheet.Cash)\
                                                    .filter(FactSheet.Plan_Id == fund.Plan_Id, FactSheet.TransactionDate == sql_factsheet[0], FactSheet.Is_Deleted != 1).scalar()

                    if cashlevel:
                        data_dict["cash_level_percentage"] = cashlevel
                        plan_wise_list.append(data_dict)

        plan_wise_dict["cash_level_data"] = plan_wise_list
        resp.append(plan_wise_dict)

    return jsonify(resp)


@final_api_bp.route('/api/v1/fund_manager_consolidated_holdings', methods=['GET'])
@required_access_l3
def fund_manager_consolidated_holdings():
    fundmanager_code = request.args.get('fund_manager_code', type=str, default=None)
    is_top_5 = request.args.get('top_5', type=int, default=1)

    if not fundmanager_code:
        raise BadRequest('Fund manager code is required.')

    resp = dict()
    finalcons_holding_list = list()
    full_holding_list = list()

    today = datetime.today()
    sql_fund = current_app.store.db.query(Plans.Plan_Id,
                                          Plans.Plan_Name,
                                          Fund.Fund_Id,
                                          Fund.Fund_Name,
                                          FundManager.Funds_Managed,
                                          FundManager.AUM)\
                                    .select_from(FundManager)\
                                    .join(Fund, Fund.Fund_Id == FundManager.Fund_Id)\
                                    .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                    .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1, Plans.PlanType_Id == 1)\
                                    .filter(or_(FundManager.DateTo >= today, FundManager.DateTo == None))\
                                    .filter(FundManager.FundManager_Code == fundmanager_code).filter(FundManager.Is_Deleted != 1).all()

    if sql_fund:
        # get consolidated holdings
        holding_final_data = dict()
        holding_final_count = dict()

        unique_fund_id = list()
        fullholding_list = list()
        unique_fund_id_names = dict()
        
        total_aum = 0
        
        for fund in sql_fund:
            if not fund.Fund_Id in unique_fund_id:
                sql_fund = None
                top = None
                hideholdingweightage = False
                portfolio_date = None
                fund_id = fund.Fund_Id

                holding_query1 = current_app.store.db.query(UnderlyingHoldings.Portfolio_Date)\
                                                    .join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                    .filter(MFSecurity.Status_Id == 1, Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, UnderlyingHoldings.Is_Deleted != 1, Plans.Plan_Id == fund.Plan_Id)
                holding = holding_query1.order_by(desc(UnderlyingHoldings.Portfolio_Date)).first()
                if holding:        
                    portfolio_date = holding.Portfolio_Date

                sql_fund = current_app.store.db.query(Fund.HideHoldingWeightage,
                                                      Fund.Top_Holding_ToBeShown).filter(Fund.Fund_Id == fund_id, Fund.Is_Deleted != 1).first()

                if sql_fund.Top_Holding_ToBeShown != None:
                    top = int(sql_fund.Top_Holding_ToBeShown)

                if sql_fund.HideHoldingWeightage:
                    hideholdingweightage = sql_fund.HideHoldingWeightage

                lst_holdings_data = get_fund_underlying_holdings(current_app.store.db, fund_id, portfolio_date, top)

                temp_dict = dict()
                temp_dict["fund_id"] = fund.Fund_Id
                temp_dict["holding"] = lst_holdings_data
                temp_dict["hideholdingweightage"] = hideholdingweightage
                fullholding_list.append(temp_dict)

                transaction_date = get_last_transactiondate(current_app.store.db, fund.Plan_Id)

                sql_fund_aum = current_app.store.db.query(FactSheet.NetAssets_Rs_Cr)\
                                                    .filter(FactSheet.Is_Deleted != 1, FactSheet.Plan_Id == fund.Plan_Id, FactSheet.TransactionDate == transaction_date).scalar()

                if sql_fund_aum:
                    total_aum = total_aum + sql_fund_aum

                unique_fund_id_names[fund.Fund_Id] = fund.Fund_Name
                unique_fund_id.append(fund.Fund_Id)
                
        unique_isin = list()
        unique_isin_details = list()

        for holdingdata in fullholding_list:
            a_fund_id = holdingdata["fund_id"]
            a_fund_holding = holdingdata["holding"]

            for securitydata in a_fund_holding:
                if securitydata.get("ISIN_Code") == "":
                    continue

                if not securitydata.get("ISIN_Code") in unique_isin:
                    unique_isin.append(securitydata.get("ISIN_Code"))
                    isin_data = dict()
                    isin_data["isin_code"] = securitydata.get("ISIN_Code")
                    isin_data["instrument_type"] = securitydata.get("Instrument_Type")
                    isin_data["holdingsecurity_name"] = securitydata.get("HoldingSecurity_Name")
                    isin_data["risk_category"] = securitydata.get("Risk_Category")
                    isin_data["sector_names"] = securitydata.get("Sector_Name")
                    isin_data["equity_style"] = securitydata.get("Equity_Style")
                    unique_isin_details.append(isin_data)


                if not securitydata.get("ISIN_Code") in holding_final_data:
                    value_in_inr = securitydata.get("Value_in_INR") if securitydata.get("Value_in_INR") != None else 0

                    if holdingdata["hideholdingweightage"] == True: 
                        value_in_inr = 0

                    holding_final_data[securitydata.get("ISIN_Code")] = value_in_inr
                    holding_final_count[securitydata.get("ISIN_Code")] = 1
                else:
                    value_in_inr = holding_final_data[securitydata.get("ISIN_Code")]

                    sec_value_in_inr = securitydata.get("Value_in_INR")
                    if holdingdata["hideholdingweightage"] == True:
                        sec_value_in_inr = 0

                    holding_final_data[securitydata.get("ISIN_Code")] = sec_value_in_inr + value_in_inr
                    holding_final_count[securitydata.get("ISIN_Code")] = holding_final_count[securitydata.get("ISIN_Code")] + 1

                #add all holdings
                holding = dict()

                holding["isin_code"] = securitydata.get("ISIN_Code")
                holding["fund_id"] = a_fund_id
                holding["fund_name"] = unique_fund_id_names[a_fund_id]
                holding["percentage_to_aum"] = securitydata.get("Percentage_to_AUM")
                full_holding_list.append(holding)

        #Get consolidated holdings
        holding_final_data = sorted(holding_final_data.items(), key=lambda x: x[1], reverse=True)
        holding_final_data = holding_final_data[:5] if is_top_5 else holding_final_data # display only top 5 holdings unless is_top_5 is set 0
        for holding_data in holding_final_data:
            holding_dict = dict()
            sectorname = None
            instrument_type = None
            equity_style = None
            risk_category = None
            holdingsecurity_name = None
            
            isin = holding_data[0]
            value_in_inr = holding_data[1] / 10000000

            holding_percent_to_aum = (value_in_inr * 100) / total_aum
            holding_dict["isin_code"] = isin
            holding_dict["percent_to_aum"] = holding_percent_to_aum

            keyValList=[isin]
            isin_data = list(filter(lambda d: d['isin_code'] in keyValList, unique_isin_details))
            if isin_data:
                instrument_type = isin_data[0]["instrument_type"]
                holdingsecurity_name = isin_data[0]["holdingsecurity_name"]
                risk_category = isin_data[0]["risk_category"]
                sectorname = isin_data[0]["sector_names"]
                equity_style = isin_data[0]["equity_style"]

            holding_dict["holdingsecurity_name"] = holdingsecurity_name
            holding_dict["sector_name"] = sectorname
            holding_dict["instrument_type"] = instrument_type
            holding_dict["investment_style"] = equity_style
            holding_dict["risk_rating"] = risk_category
            holding_dict["holding_count"] = holding_final_count[isin]
            
            allholding_byisin = list(filter(lambda d: d['isin_code'] in keyValList, full_holding_list))
            holding_dict["all_holdings"] = allholding_byisin
            finalcons_holding_list.append(holding_dict)

    resp["consolidated_holdings"] = finalcons_holding_list

    return jsonify(resp)



@final_api_bp.route('/api/v1/fund_manager_history')
@required_access_l1
def get_fundmanager_history():
    # FundManager/M00
    scheme_code = request.args.get("scheme_code",type=str)
    scheme_id = request.args.get("scheme_id",type=int)
    plan_id = request.args.get("plan_id", type=int)

    if not scheme_code and not scheme_id and not plan_id:
        raise BadRequest("Parameters required.")

    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, None, None, scheme_id, scheme_code, None )
    
    resp = list()
    query_fundmanager = None
    if plan_id:
        resp = getfundmanager(current_app.store.db, plan_id)

    return jsonify(resp)

@final_api_bp.route("/api/v1/fund_list", methods=['GET'])
@required_access_l1
def get_fund_list():
    resp = list()

    amc_id = request.args.get("amc_id", type=int, default=None)
    amc_code = request.args.get("amc_code", default=None)
    product_code = request.args.get("product_code", default=None)
    product_id = request.args.get("product_id", default=None)

    sql_q = current_app.store.db.query(Fund.Fund_Id,
                                       AMC.AMC_Id,
                                       Fund.Fund_Name,
                                       Fund.Fund_Code,
                                       Product.Product_Code)\
                                .join(MFSecurity, and_(MFSecurity.Fund_Id==Fund.Fund_Id, MFSecurity.Is_Deleted != 1))\
                                .join(AMC, and_(MFSecurity.AMC_Id==AMC.AMC_Id))\
                                .join(Product, AMC.Product_Id==Product.Product_Id)

    if amc_id:
        sql_q = sql_q.filter(AMC.AMC_Id == amc_id)
    elif amc_code:
        sql_q = sql_q.filter(AMC.AMC_Code == amc_code)

    if product_id:
        sql_q = sql_q.filter(Product.Product_Id == product_id)
    elif product_code:
        sql_q = sql_q.filter(Product.Product_Code == product_code)

    sql_objs = sql_q.filter(Fund.Is_Deleted != 1).distinct().all()

    for sql_obj in sql_objs:
        obj = dict()
        obj['scheme_id'] = sql_obj.Fund_Id
        obj['amc_id'] = sql_obj.AMC_Id
        obj['product_code'] = sql_obj.Product_Code
        obj['scheme_code'] = sql_obj.Fund_Code
        obj['scheme_name'] = sql_obj.Fund_Name
        resp.append(obj)

    return jsonify(resp)


# PMS/AIF Scheme Details
@final_api_bp.route("/api/v1/fund_details", methods=['GET'])
@required_access_l1
def get_schemedetails():
    reqd_parameters = ["plan_id", "plan_code", "plan_name", "scheme_id", "scheme_code", "scheme_name", "isin", "amfi_code"]
    requestdata = {}
    for p in reqd_parameters:
        requestdata[p] = request.args.get(p)

    if not any(requestdata.get(key) for key in reqd_parameters):
        raise BadRequest("Atleast one of the following parameters are required : " + " | ".join(reqd_parameters))

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db,
                              requestdata["plan_code"],
                              requestdata["plan_name"],
                              requestdata["scheme_id"],
                              requestdata["scheme_code"],
                              requestdata["scheme_name"],
                              isin = requestdata.get("isin"),
                              amfi_code = requestdata.get("amfi_code"))

    
    transaction_date = request.args.get('date')
    if not transaction_date:
        transaction_date = get_last_transactiondate(current_app.store.db, plan_id)

    sql_factsheet = current_app.store.db.query(
                                                Fund.Fund_Id, 
                                                Fund.Fund_Code, 
                                                Fund.Fund_Name, 
                                                AMC.AMC_Code, 
                                                AMC.AMC_Id,
                                                AMC.AMC_Logo,
                                                Classification.Classification_Name,
                                                Classification.Classification_Id,
                                                MFSecurity.MF_Security_OpenDate, 
                                                BenchmarkIndices.BenchmarkIndices_Name, 
                                                MFSecurity.MF_Security_Min_Purchase_Amount, 
                                                FactSheet.Exit_Load, 
                                                MFSecurity.Fees_Structure, 
                                                MFSecurity.MF_Security_Investment_Strategy,
                                                Fund.fund_comments,
                                                FactSheet.ExpenseRatio,
                                                FactSheet.Risk_Grade,
                                                Plans.AMFI_Code,
                                                Plans.ISIN,
                                                Plans.ISIN2,
                                                Plans.RTA_Name,
                                                PlanType.PlanType_Name,
                                                MFSecurity.MF_Security_Min_Lockin_Period,
                                                AssetClass.AssetClass_Name,
                                                MFSecurity.MF_Security_Trxn_Cut_Off_Time,
                                                FundType.FundType_Name
                                               ).select_from(FactSheet).join(Plans, plan_id == FactSheet.Plan_Id) \
                                                                       .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id) \
                                                                       .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id).join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id) \
                                                                       .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id) \
                                                                       .join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id) \
                                                                       .join(PlanType, PlanType.PlanType_Id == Plans.PlanType_Id) \
                                                                       .join(FundType, FundType.FundType_Id == MFSecurity.FundType_Id) \
                                                                       .join(AssetClass, AssetClass.AssetClass_Id == MFSecurity.AssetClass_Id, isouter=True) \
                                                                       .filter(Plans.Is_Deleted != 1).filter(FactSheet.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1) \
                                                                       .filter(Fund.Is_Deleted != 1).filter(Classification.Is_Deleted != 1).filter(BenchmarkIndices.Is_Deleted != 1) \
                                                                       .filter(MFSecurity.Status_Id == 1).filter(Plans.Plan_Id == plan_id) \
                                                                       .filter(FactSheet.TransactionDate == transaction_date).one_or_none()
                                                                                                                                              
    res = dict()

    if sql_factsheet:
        res["scheme_id"] = sql_factsheet.Fund_Id
        res["scheme_code"] = sql_factsheet.Fund_Code
        res["scheme_name"] = sql_factsheet.Fund_Name
        res["amc_id"] = sql_factsheet.AMC_Id
        res["amc_code"] = sql_factsheet.AMC_Code
        res["amc_logo"] = F"{current_app.config['IMAGE_PATH']}{sql_factsheet.AMC_Logo}" if sql_factsheet.AMC_Logo else None
        res["scheme_classification"] = sql_factsheet.Classification_Name
        res["scheme_classification_id"] = sql_factsheet.Classification_Id
        res["scheme_assetclass"] = sql_factsheet.AssetClass_Name
        res["scheme_inception_date"] = sql_factsheet.MF_Security_OpenDate
        res["scheme_benchmark_name"] = sql_factsheet.BenchmarkIndices_Name
        res["scheme_min_investment"] = sql_factsheet.MF_Security_Min_Purchase_Amount
        res["scheme_exit_load"] = sql_factsheet.Exit_Load
        res["scheme_fee_structure"] = sql_factsheet.Fees_Structure
        res["scheme_objective"] = sql_factsheet.MF_Security_Investment_Strategy
        res["scheme_comments"] = sql_factsheet.fund_comments
        res["scheme_expense_ratio"] = sql_factsheet.ExpenseRatio
        res["scheme_isin"] = sql_factsheet.ISIN if sql_factsheet.ISIN else None
        res["scheme_isin2"] = sql_factsheet.ISIN2 if sql_factsheet.ISIN2 else None
        res["scheme_risk_grade"] = sql_factsheet.Risk_Grade
        res["scheme_amfi_code"] = sql_factsheet.AMFI_Code
        res["scheme_rta_name"] = sql_factsheet.RTA_Name
        res["scheme_plan_type"] = sql_factsheet.PlanType_Name
        res["scheme_min_lockin_period"] = sql_factsheet.MF_Security_Min_Lockin_Period
        res["scheme_transaction_cut_off_time"] = sql_factsheet.MF_Security_Trxn_Cut_Off_Time
        res["scheme_type"] = sql_factsheet.FundType_Name

        sql_fundmanagers = current_app.store.db.query(FundManager.FundManager_Name, FundManager.FundManager_Code).filter(FundManager.Fund_Id == sql_factsheet.Fund_Id).filter(FundManager.Is_Deleted != 1).all()
        fund_managers = list()
        fund_managers_code = list()

        if sql_fundmanagers:
            for sql_fundmanager in sql_fundmanagers:
                data = dict()
                fund_managers.append(sql_fundmanager.FundManager_Name)#need to remove this 

                data['fund_manager_name'] = sql_fundmanager.FundManager_Name
                data['fund_manager_code'] = sql_fundmanager.FundManager_Code
                fund_managers_code.append(data)

        res["scheme_fund_manager"] = fund_managers
        res["scheme_fund_manager_code"] = fund_managers_code

    return jsonify(res)


# PMS/AIF Fund Performance 
@final_api_bp.route("/api/v1/fund_performance", methods=['GET'])
@required_access_l1
def get_factsheet_performance():
    # Factsheet/M01
    transaction_date = request.args.get("date", type=str)

    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    factsheet_query1 = current_app.store.db.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1)
    
    if transaction_date:
        factsheet_query1 = factsheet_query1.filter(FactSheet.TransactionDate == transaction_date)

    factsheet = factsheet_query1.order_by(desc(FactSheet.TransactionDate)).first()

    res_factsheet = dict()
    if factsheet:
        if not transaction_date:
            transaction_date = factsheet.TransactionDate

        benchmarkdata = current_app.store.db.query(MFSecurity.MF_Security_Id, Plans.Plan_Name, Product.Product_Code, Product.Product_Id, BenchmarkIndices.Co_Code, BenchmarkIndices.TRI_Co_Code, BenchmarkIndices.BenchmarkIndices_Name, TRIReturns.Return_1Month, TRIReturns.Return_3Month, TRIReturns.Return_6Month, TRIReturns.Return_1Year, TRIReturns.Return_3Year, FactSheet.SCHEME_BENCHMARK_RETURNS_1MONTH, FactSheet.SCHEME_BENCHMARK_RETURNS_3MONTH, FactSheet.SCHEME_BENCHMARK_RETURNS_6MONTH, FactSheet.SCHEME_BENCHMARK_RETURNS_1YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_3YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_2YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_5YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_10YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_SI, Fund.Fund_Id, Fund.Fund_Name, Fund.Fund_Code).select_from(FactSheet).join(Plans, Plans.Plan_Id == FactSheet.Plan_Id).join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id).join(Product, Product.Product_Id == PlanProductMapping.Product_Id).join(MFSecurity,MFSecurity.MF_Security_Id == Plans.MF_Security_Id).join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id).join(Fund, Fund.Fund_Id==MFSecurity.Fund_Id).join(TRIReturns, and_(TRIReturns.TRI_Co_Code == BenchmarkIndices.TRI_Co_Code, TRIReturns.TRI_IndexDate == FactSheet.TransactionDate), isouter = True).filter(MFSecurity.Is_Deleted != 1).filter(MFSecurity.Status_Id == 1).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.TransactionDate == transaction_date).filter(FactSheet.Plan_Id == plan_id).first()

        nav = current_app.store.db.query(NAV.NAV)\
                                    .filter(NAV.NAV_Date == transaction_date,
                                            NAV.Plan_Id == plan_id,
                                            NAV.Is_Deleted != 1,
                                            NAV.NAV_Type == 'P').scalar()

        res_factsheet["scheme_id"] = benchmarkdata.Fund_Id
        res_factsheet["scheme_code"] = benchmarkdata.Fund_Code
        res_factsheet["date"] = factsheet.TransactionDate
        res_factsheet["nav"] = nav
        res_factsheet["aum"] = factsheet.NetAssets_Rs_Cr
        res_factsheet["plan_name"] = benchmarkdata.Plan_Name
        res_factsheet["benchmark_name"] = benchmarkdata.BenchmarkIndices_Name

        res_factsheet["scheme_ret_1m"] = factsheet.SCHEME_RETURNS_1MONTH
        res_factsheet["scheme_ret_3m"] = factsheet.SCHEME_RETURNS_3MONTH
        res_factsheet["scheme_ret_6m"] = factsheet.SCHEME_RETURNS_6MONTH
        res_factsheet["scheme_ret_1y"] = factsheet.SCHEME_RETURNS_1YEAR
        res_factsheet["scheme_ret_2y"] = factsheet.SCHEME_RETURNS_2YEAR
        res_factsheet["scheme_ret_3y"] = factsheet.SCHEME_RETURNS_3YEAR
        res_factsheet["scheme_ret_5y"] = factsheet.SCHEME_RETURNS_5YEAR
        res_factsheet["scheme_ret_10y"] = factsheet.SCHEME_RETURNS_10YEAR

        res_factsheet["scheme_ret_ince"] = factsheet.SCHEME_RETURNS_since_inception

        # Adding category returns
        res_factsheet["cat_ret_1m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH
        res_factsheet["cat_ret_3m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH
        res_factsheet["cat_ret_6m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH
        res_factsheet["cat_ret_1y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR
        res_factsheet["cat_ret_3y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR
        res_factsheet["cat_ret_5y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR

        product_code = benchmarkdata.Product_Code
        tri_co_code = benchmarkdata.TRI_Co_Code if benchmarkdata.TRI_Co_Code else ''

        if product_code == "PMS" or product_code == "AIF":
            res_factsheet["bm_ret_1m"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_1MONTH
            res_factsheet["bm_ret_3m"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_3MONTH
            res_factsheet["bm_ret_6m"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_6MONTH
            res_factsheet["bm_ret_1y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_1YEAR
            res_factsheet["bm_ret_2y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_2YEAR
            res_factsheet["bm_ret_3y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_3YEAR
            res_factsheet["bm_ret_5y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_5YEAR
            res_factsheet["bm_ret_10y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_10YEAR
            res_factsheet["bm_ret_ince"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_SI
        else:
            transactiondate1 = parser.parse(transaction_date) if type(transaction_date) == str else transaction_date
            transaction_date_1 = transactiondate1 - timedelta(days=1)
            transaction_date_2 = transactiondate1 - timedelta(days=2)

            co_code = get_co_code_by_plan_id(current_app.store.db, plan_id)

            benchmark_ret_query = current_app.store.db.query(TRIReturns.Return_1Month, TRIReturns.Return_3Month, TRIReturns.Return_6Month, TRIReturns.Return_1Year, TRIReturns.Return_3Year)
            if tri_co_code == '':
                benchmark_ret_query = benchmark_ret_query.join(BenchmarkIndices, TRIReturns.TRI_Co_Code == BenchmarkIndices.Co_Code).filter(BenchmarkIndices.Co_Code == benchmarkdata.Co_Code)
            else:
                benchmark_ret_query = benchmark_ret_query.join(BenchmarkIndices, TRIReturns.TRI_Co_Code == BenchmarkIndices.TRI_Co_Code).filter(BenchmarkIndices.Co_Code == co_code)

            benchmark_ret_data = benchmark_ret_query.filter(or_(TRIReturns.TRI_IndexDate == transaction_date, TRIReturns.TRI_IndexDate == transaction_date_1)).order_by(desc(TRIReturns.TRI_IndexDate)).first()

            if not benchmark_ret_data:
                benchmark_ret_data = benchmark_ret_query.filter(or_(TRIReturns.TRI_IndexDate == transaction_date, TRIReturns.TRI_IndexDate == transaction_date_2)).order_by(desc(TRIReturns.TRI_IndexDate)).first()

            if benchmark_ret_data:
                res_factsheet["bm_ret_1m"] = round(benchmark_ret_data.Return_1Month, 2) if benchmark_ret_data.Return_1Month else None
                res_factsheet["bm_ret_3m"] = round(benchmark_ret_data.Return_3Month, 2) if benchmark_ret_data.Return_3Month else None
                res_factsheet["bm_ret_6m"] = round(benchmark_ret_data.Return_6Month, 2) if benchmark_ret_data.Return_6Month else None
                res_factsheet["bm_ret_1y"] = round(benchmark_ret_data.Return_1Year, 2) if benchmark_ret_data.Return_1Year else None
                res_factsheet["bm_ret_2y"] = 0
                res_factsheet["bm_ret_3y"] = round(benchmark_ret_data.Return_3Year, 2) if benchmark_ret_data.Return_3Year else None
                res_factsheet["bm_ret_5y"] = 0
                res_factsheet["bm_ret_10y"] = 0
                res_factsheet["bm_ret_ince"] = 0
            else:
                res_factsheet["bm_ret_1m"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_1MONTH
                res_factsheet["bm_ret_3m"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_3MONTH
                res_factsheet["bm_ret_6m"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_6MONTH
                res_factsheet["bm_ret_1y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_1YEAR
                res_factsheet["bm_ret_2y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_2YEAR
                res_factsheet["bm_ret_3y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_3YEAR
                res_factsheet["bm_ret_5y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_5YEAR
                res_factsheet["bm_ret_10y"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_10YEAR
                res_factsheet["bm_ret_ince"] = benchmarkdata.SCHEME_BENCHMARK_RETURNS_SI

        if (benchmarkdata.Product_Id == 1 or benchmarkdata.Product_Id == 2) and tri_co_code != '': #MF or ULIP
            res_factsheet["category_name"] = "Category Average"
            res_factsheet["cat_ret_1m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH
            res_factsheet["cat_ret_3m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH
            res_factsheet["cat_ret_6m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH
            res_factsheet["cat_ret_1y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR
            res_factsheet["cat_ret_3y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR
            res_factsheet["cat_ret_5y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR
            res_factsheet["cat_ret_ince"] = 0

        #Active Returns
        scheme_return_1month = factsheet.SCHEME_RETURNS_1MONTH if factsheet.SCHEME_RETURNS_1MONTH else 0
        scheme_return_3month = factsheet.SCHEME_RETURNS_3MONTH if factsheet.SCHEME_RETURNS_3MONTH else 0
        scheme_return_6month = factsheet.SCHEME_RETURNS_6MONTH if factsheet.SCHEME_RETURNS_6MONTH else 0
        scheme_return_1year = factsheet.SCHEME_RETURNS_1YEAR if factsheet.SCHEME_RETURNS_1YEAR else 0
        scheme_return_3year = factsheet.SCHEME_RETURNS_3YEAR if factsheet.SCHEME_RETURNS_3YEAR else 0
        scheme_return_5year = factsheet.SCHEME_RETURNS_5YEAR if factsheet.SCHEME_RETURNS_5YEAR else 0
        scheme_return_10year = factsheet.SCHEME_RETURNS_10YEAR if factsheet.SCHEME_RETURNS_10YEAR else 0
        scheme_return_si = factsheet.SCHEME_RETURNS_since_inception if factsheet.SCHEME_RETURNS_since_inception else 0

        benchmark_return_1month = benchmarkdata.SCHEME_BENCHMARK_RETURNS_1MONTH if benchmarkdata.SCHEME_BENCHMARK_RETURNS_1MONTH else 0
        benchmark_return_3month = benchmarkdata.SCHEME_BENCHMARK_RETURNS_3MONTH if benchmarkdata.SCHEME_BENCHMARK_RETURNS_3MONTH else 0
        benchmark_return_6month = benchmarkdata.SCHEME_BENCHMARK_RETURNS_6MONTH if benchmarkdata.SCHEME_BENCHMARK_RETURNS_6MONTH else 0
        benchmark_return_1year = benchmarkdata.SCHEME_BENCHMARK_RETURNS_1YEAR if benchmarkdata.SCHEME_BENCHMARK_RETURNS_1YEAR else 0
        benchmark_return_3year = benchmarkdata.SCHEME_BENCHMARK_RETURNS_3YEAR if benchmarkdata.SCHEME_BENCHMARK_RETURNS_3YEAR else 0
        benchmark_return_5year = benchmarkdata.SCHEME_BENCHMARK_RETURNS_5YEAR if benchmarkdata.SCHEME_BENCHMARK_RETURNS_5YEAR else 0
        benchmark_return_10year = benchmarkdata.SCHEME_BENCHMARK_RETURNS_10YEAR if benchmarkdata.SCHEME_BENCHMARK_RETURNS_10YEAR else 0
        benchmark_return_si = benchmarkdata.SCHEME_BENCHMARK_RETURNS_SI if benchmarkdata.SCHEME_BENCHMARK_RETURNS_SI else 0
        
        res_factsheet["active_returns_1month"] = None if (not scheme_return_1month) or ( not benchmark_return_1month) else scheme_return_1month - benchmark_return_1month
        res_factsheet["active_returns_3month"] = None if (not scheme_return_3month) or ( not benchmark_return_3month) else scheme_return_3month - benchmark_return_3month
        res_factsheet["active_returns_6month"] = None if (not scheme_return_6month) or ( not benchmark_return_6month) else scheme_return_6month - benchmark_return_6month
        res_factsheet["active_returns_1year"] = None if (not scheme_return_1year) or ( not benchmark_return_1year) else scheme_return_1year - benchmark_return_1year
        res_factsheet["active_returns_3year"] = None if (not scheme_return_3year) or ( not benchmark_return_3year) else scheme_return_3year - benchmark_return_3year
        res_factsheet["active_returns_5year"] = None if (not scheme_return_5year) or ( not benchmark_return_5year) else scheme_return_5year - benchmark_return_5year
        res_factsheet["active_returns_10year"] = None if (not scheme_return_10year) or ( not benchmark_return_10year) else scheme_return_10year - benchmark_return_10year
        res_factsheet["active_returns_si"] = None if (not scheme_return_si) or ( not benchmark_return_si) else scheme_return_si - benchmark_return_si

        #Fund Ranking
        res_factsheet["ranking_rank_1month"] = factsheet.RANKING_RANK_1MONTH
        res_factsheet["ranking_rank_3month"] = factsheet.RANKING_RANK_3MONTH
        res_factsheet["ranking_rank_6month"] = factsheet.RANKING_RANK_6MONTH
        res_factsheet["ranking_rank_1year"] = factsheet.RANKING_RANK_1YEAR
        res_factsheet["ranking_rank_3year"] = factsheet.RANKING_RANK_3YEAR
        res_factsheet["ranking_rank_5year"] = factsheet.RANKING_RANK_5YEAR

        #Count
        res_factsheet["count_1month"] = factsheet.COUNT_1MONTH
        res_factsheet["count_3month"] = factsheet.COUNT_3MONTH
        res_factsheet["count_6month"] = factsheet.COUNT_6MONTH
        res_factsheet["count_1year"] = factsheet.COUNT_1YEAR
        res_factsheet["count_3year"] = factsheet.COUNT_3YEAR
        res_factsheet["count_5year"] = factsheet.COUNT_5YEAR

    return jsonify(res_factsheet)

# PMS/AIF Fund Yearly Performance ( factsheet - Performance & Risk Analysis - Financial Year - Performance)
@final_api_bp.route('/api/v1/fund_historic_performance', methods=['GET'])
@required_access_l1
def getperformance():
    transaction_date = request.args.get("date", type=str)
    performance_type = request.args.get("time_period_type", type=str) #CY or FY or MOM or POP
    from_date = request.args.get("from_date", type=str)
    to_date = request.args.get("to_date", type=str)
    requestdata = get_requestdata(request)

    resp = list()
    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )

    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if not performance_type:
        raise BadRequest("Performance type is required.")

    if performance_type == "POP":
        if not from_date:
            raise BadRequest("From date is required.")

        if not to_date:
            raise BadRequest("To date is required.")

    if plan_id:
        transaction_date_python = parser.parse(transaction_date) if transaction_date else None
        resp = get_fund_historic_performance(current_app.store.db, plan_id, from_date, to_date, performance_type, transaction_date_python)

    return jsonify(resp)


# PMS/AIF Fund Rolling Returns
@final_api_bp.route('/api/v1/fund_rolling_returns', methods=['GET'])
@required_access_l2
def getrollingreturns():
    transaction_date = request.args.get("date", type=str)    
    timeframe_in_yr = request.args.get("timeframe_in_yr", type=int) #1 or 3 or 5
    return_type = request.args.get("return_type", type=str) #annualized/absolute
    requestdata = get_requestdata(request)
    plan_id = requestdata["plan_id"]
    res = dict()

    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")
    
    if not timeframe_in_yr:
        raise BadRequest("timeframe_in_yr field is missing.")

    if plan_id:
        is_annualized_return = True if return_type == 'annualized' else False
        res = get_rollingreturn(current_app.store.db, plan_id, transaction_date, timeframe_in_yr, is_annualized_return = is_annualized_return)

    return jsonify(res)


@final_api_bp.route("/api/v1/fund_risk_ratios", methods=['GET'])
@required_access_l2
def getfundriskratio(): 
    transaction_date = request.args.get("date", type=str)
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")
    
    resp_dic = get_fundriskratio_data(current_app.store.db,plan_id,transaction_date)

    return jsonify(resp_dic)


@final_api_bp.route("/api/v1/detailed_fund_risk_ratios", methods=['GET'])
@required_access_l2
def get_detailed_fundriskratio():
    reqd_parameters = ["plan_id", "plan_code", "plan_name", "scheme_id", "scheme_code", "scheme_name", "date", "period_in_yrs"]
    requestdata = {}
    for p in reqd_parameters:
        requestdata[p] = request.args.get(p)

    if not any(requestdata.get(key) for key in reqd_parameters):
        raise BadRequest("Atleast one of the following parameters are required : " + " | ".join(reqd_parameters))

    if not (requestdata.get("period_in_yrs") and requestdata["period_in_yrs"].isdecimal()):
        raise BadRequest("The following parameter is required : period_in_yrs supports either of the following values [1,3]")

    db_session = current_app.store.db
    plan_id = requestdata["plan_id"]
    transaction_date = requestdata["date"]
    period_in_yrs = int(requestdata["period_in_yrs"])

    if not plan_id:
        plan_id = get_plan_id(db_session, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(db_session, plan_id)

    # TODO : Check if we need to refactor any of the following code to a separate function
    resp_structure = {}

    trxn_date = datetime.strptime(transaction_date, '%Y-%m-%d')
    date_3m = trxn_date - relativedelta(months=3) 
    date_6m = trxn_date - relativedelta(months=6)
    date_1y = trxn_date - relativedelta(months=12)
    date_filter = datetime.strptime(get_business_day(trxn_date, timeframe_in_days = 400), '%Y-%m-%d')       # get a date more than 1 year from today

    dates = {
        "latest_trxn_date": trxn_date,
        "3m_ago": datetime.combine(last_date_of_month(date_3m.year, date_3m.month), datetime.min.time()) ,
        "6m_ago": datetime.combine(last_date_of_month(date_6m.year, date_6m.month), datetime.min.time()),
        "1yr_ago": datetime.combine(last_date_of_month(date_1y.year, date_1y.month), datetime.min.time())
    }

    sql_factsheetdata = db_session.query(FactSheet.TotalStocks,
                                         FactSheet.Alpha_1Yr,
                                         FactSheet.Alpha,
                                         FactSheet.Beta_1Yr,
                                         FactSheet.Beta,
                                         FactSheet.Mean_1Yr,
                                         FactSheet.Mean,
                                         FactSheet.StandardDeviation_1Yr,
                                         FactSheet.StandardDeviation,
                                         FactSheet.R_Squared_1Yr,
                                         FactSheet.R_Squared,
                                         FactSheet.SharpeRatio_1Yr,
                                         FactSheet.SharpeRatio,
                                         FactSheet.Sortino_1Yr,
                                         FactSheet.Sortino,
                                         FactSheet.ModifiedDuration_yrs,
                                         FactSheet.PortfolioP_ERatio,
                                         FactSheet.TransactionDate,
                                         FactSheet.Plan_Id,
                                         FactSheet.Treynor_Ratio,
                                         FactSheet.Treynor_Ratio_1Yr
                                         )\
                                  .filter(and_(FactSheet.Is_Deleted != 1, FactSheet.Plan_Id == plan_id))\
                                  .filter(FactSheet.TransactionDate >= date_filter).all()

    # convert the query result to dataframe
    df = pd.DataFrame(sql_factsheetdata)
    df['asof'] = None

    # check if transactiondates , if not then reduce a business day.
    for k in dates.copy().keys():
        trxn_date_exists = False
        max_fallback = 10 # We will do maximum 10 days prior, if not found then we move to next logic
        while not trxn_date_exists and max_fallback != 0:
            if dates[k] in set(df['TransactionDate']):
                trxn_date_exists = True
                df.loc[df["TransactionDate"] == dates[k], 'asof'] = k
            else:
                dates[k] = dates[k] - timedelta(1)
                max_fallback -= 1

    df = df[~df['asof'].isnull()]       # filter the df with required dates
    df.set_index('asof', inplace=True)  # reset index
    df.columns = df.columns.str.lower()  # rename columns to lower case
    df['transactiondate'] = df['transactiondate'].dt.strftime('%Y-%m-%d')
    df = df.apply(pd.to_numeric, errors='coerce')
    df = df.round(2)

    one_yr = json.loads(df[["totalstocks","alpha_1yr","beta_1yr","mean_1yr","standarddeviation_1yr",
                            "r_squared_1yr","sharperatio_1yr","sortino_1yr","modifiedduration_yrs",
                            "treynor_ratio_1yr",
                            "portfoliop_eratio","transactiondate","plan_id"]].to_json(orient ='index'))
    
    three_yr = json.loads(df[["totalstocks","alpha","beta","mean","standarddeviation","r_squared",
                              "sharperatio","sortino","modifiedduration_yrs","portfoliop_eratio",
                              "treynor_ratio",
                              "transactiondate","plan_id"]].to_json(orient ='index'))

    # prepare response
    if period_in_yrs == 1:
        resp_structure["risk_ratios_1_yr"] = one_yr
    elif period_in_yrs == 3:
        resp_structure["risk_ratios_3_yr"] = three_yr
    elif period_in_yrs == 0:
        resp_structure["risk_ratios_1_yr"] = one_yr
        resp_structure["risk_ratios_3_yr"] = three_yr

    return jsonify(resp_structure)


# PMS/AIF Fund Portfolio Characteristics
@final_api_bp.route("/api/v1/portfolio_characteristics", methods=['GET'])
@required_access_l2
def get_portfolioCharacteristics():
    transaction_date = request.args.get("date", type=str)
    portfolio_date = request.args.get("pf_date", type=str)

    # date conversion
    transaction_date = datetime.strptime(transaction_date, '%Y-%m-%d').date() if transaction_date else None
    portfolio_date = datetime.strptime(portfolio_date, r'%d %b %Y').date() if portfolio_date else None

    requestdata = get_requestdata(request)
    db_session = current_app.store.db

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(db_session, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"])

    if plan_id == None:
        raise BadRequest("Request parameters required.")

    if not transaction_date:
        transaction_date = get_last_transactiondate(db_session, plan_id)

    if not portfolio_date:
        portfolio_date = get_portfolio_date(current_app.store.db, plan_id, transaction_date)

    portfolio_date_limit = transaction_date - timedelta(days=60)

    # TODO Need to implement calculations for debt fund ratios and then get rid of the following function.
    # Idea is to calculate the ratios and not depend on any data vendors
    # As of now have to combine the debt fund ratios from the following with the equity fund ratios from the other function call
    res = get_portfolio_characteristics(db_session, plan_id, transaction_date)

    # delta indicated the fallback date for which prices of securities will be fetched
    if res['auto_populate'] and portfolio_date >= portfolio_date_limit:
        dict_ratios = calculate_portfolio_level_analysis(db_session, plan_id, portfolio_date, delta=3)
        res.update(dict_ratios)
    else:
        res['median_mkt_cap'] = None

    return jsonify(res)


@final_api_bp.route("/api/v1/fund_marketcap_composition", methods=['GET'])
@required_access_l2
def getmarketcapcomposition():
    transaction_date = request.args.get("date", type=str)
    requestdata = get_requestdata(request)
    composition_for = request.args.get("composition_for", type=str, default='fund_level')

    db_session = current_app.store.db
    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(db_session, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"])

    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    resp = list()
    # no_value = "NA"
    # lst_response_keys = ['large_cap', 'mid_cap', 'small_cap', 'unlisted']

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(db_session, plan_id)
    
        resp = get_marketcap_composition(db_session, plan_id, transaction_date, composition_for)

        if len(resp) != 0:
            sql_fund = get_sql_fund_byplanid(db_session, plan_id)
            resp[0]["scheme_code"] = sql_fund.Fund_Code
            # # add 'NA' to missing keys
            # missing_keys = lst_response_keys - resp[0].keys()
            # resp[0].update(dict.fromkeys(missing_keys, no_value))


    return jsonify(resp)


@final_api_bp.route("/api/v1/fund_investment_style", methods=['GET'])
@required_access_l3
def getinvesymentstyle():
    transaction_date = request.args.get("date", type=str)

    requestdata = get_requestdata(request)
    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    respons = list()
    
    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(current_app.store.db, plan_id)

        respons = get_investmentstyle(current_app.store.db, plan_id, transaction_date)
    
    return jsonify(respons)


def previous_quarter(ref):
    if ref.month < 4:
        return  datetime(ref.year - 1, 12,31)
    elif ref.month < 7:
        return datetime(ref.year, 3,31) 
    elif ref.month < 10:
        return datetime(ref.year, 6,30) 
    return datetime(ref.year, 9,30) 


@final_api_bp.route("/api/v1/fund_portfolio_change", methods=['GET'])
@required_access_l3
def getportfoliochange():
    transaction_date = request.args.get("date", type=str)    
    change_category = request.args.get("change_category", type=str)
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if not change_category:
        raise BadRequest("change_category required.")

    resp = list()
    resp = get_fund_portfolio_change(current_app.store.db, plan_id, change_category, transaction_date)
        
    return jsonify(resp)


# PMS/AIF Fund Holdings
@final_api_bp.route("/api/v1/fund_holdings", methods=['GET'])
@required_access_l2
def get_fundholdings():
    portfolio_date = request.args.get("date", type=str)

    plan_id = None
    requestdata = get_requestdata(request)
    
    #TODO handle below handling from frontend
    portfolio_date = portfolio_date if portfolio_date and portfolio_date != 'null' else None

    if not requestdata["plan_id"]:
        plan_id = get_plan_id(current_app.store.db,
                              requestdata["plan_code"],
                              requestdata["plan_name"],
                              requestdata["scheme_id"],
                              requestdata["scheme_code"],
                              requestdata["scheme_name"],
                              isin = request.args.get("isin", type=str),
                              amfi_code = request.args.get("amfi_code", type=str))
    else:
        plan_id = requestdata["plan_id"]

    resp = list()

    if not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] and not plan_id:
        raise BadRequest("Parameters required.")

    if plan_id:
        resp = get_fund_holdings(current_app.store.db, plan_id, portfolio_date)

    return jsonify(resp)


# PMS/AIF Fund Sector Allocation
@final_api_bp.route("/api/v1/fund_sectors", methods=['GET'])
@required_access_l2
def get_sectorweights():
    '''
    Expected values:
        composition_for >> 'aif_cat3', 'fund_level', 'category_wise_all_funds', 'product_wise_all_funds'
    '''
    transaction_date = request.args.get("date", type=str)
    composition_for = request.args.get('composition_for', type=str, default='fund_level')

    resp = list()
    requestdata = get_requestdata(request)
    plan_id = requestdata["plan_id"]
    # product wise composition does not require plan id
    if composition_for != 'product_wise_all_funds':
        if not plan_id:
            plan_id = get_plan_id(current_app.store.db,
                                  requestdata["plan_code"],
                                  requestdata["plan_name"],
                                  requestdata["scheme_id"],
                                  requestdata["scheme_code"],
                                  requestdata["scheme_name"])

        if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"]:
            raise BadRequest("Parameters required.")

    # logic to set transaction date
    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(current_app.store.db, plan_id)
    elif composition_for == 'product_wise_all_funds' and transaction_date == None:
        # product wise composition requires to set transaction date
        # set transaction date to previous month end or 
        # previous to previous month end depending on data flowing till 15 of this month
        todays_date = datetime.now()
        prev_month_date = todays_date - timedelta(weeks=4)
        prev2p_month_date = todays_date - timedelta(weeks=8)
        transaction_date = last_date_of_month(prev_month_date.year, prev_month_date.month) if todays_date.day > 15 else last_date_of_month(prev2p_month_date.year, prev2p_month_date.month)


    resp = get_sectorweightsdata(current_app.store.db, plan_id, transaction_date, composition_for)

    return jsonify(resp)


# PMS/AIF Fund Portfolio Composition
@final_api_bp.route('/api/v1/fund_composition')
@required_access_l2
def get_composition():
    '''
    Expected values:
        composition_for >> 'aif_cat3', 'fund_level', 'category_wise_all_funds'
    '''
    # FactSheet/M02
    transaction_date = request.args.get("date", type=str)    
    composition_for = request.args.get('composition_for', type=str, default='fund_level')

    requestdata = get_requestdata(request)
    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(current_app.store.db, plan_id)   
    
    resp = get_compositiondata(current_app.store.db, plan_id, transaction_date, composition_for)

    return jsonify(resp)


@final_api_bp.route('/api/v1/asondate', methods=['GET'])
def asondate():
    plan_id = request.args.get("plan_id",type=int)

    if not plan_id:
        raise BadRequest(description="Plan id required.")
    
    resp = list()
    sql_asondate = current_app.store.db.query(FactSheet.Plan_Id,FactSheet.TransactionDate).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).order_by(desc(FactSheet.TransactionDate))

    for sql_obj in sql_asondate:
        json_obj = dict()
        json_obj["Plan_Id"] = sql_obj.Plan_Id
        json_obj["TransactionDate"] = sql_obj.TransactionDate.strftime("%B %d, %Y")

        resp.append(json_obj)

    return jsonify(resp)


# PMS/AIF Fund Attribution Analysis
@final_api_bp.route("/api/v1/fund_attribution", methods=['GET'])
@required_access_l3
def get_attributionanalysis():
    from_date = request.args.get("from_date", type=str)
    to_date = request.args.get("to_date", type=str)
    requestdata = get_requestdata(request)

    res = dict()
    attri_gsquare_resp = None
    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if not from_date:
        raise BadRequest("From date required.")
    
    if not to_date:
        raise BadRequest("To date required.")

    from_date = parser.parse(from_date)
    to_date = parser.parse(to_date)

    if from_date > to_date:
        raise BadRequest("From date cannot be greater than to date.")

    if plan_id:
        #check if factsheet is available for given to date
        sql_factsheet = current_app.store.db.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.TransactionDate == to_date).filter(FactSheet.Is_Deleted != 1).one_or_none()
        if not sql_factsheet:
            raise BadRequest("Factsheet not available for given date.")

        sql_attribution = current_app.store.db.query(FactsheetAttribution.Response_Attribution).filter(FactsheetAttribution.Plan_Id == plan_id).filter(FactsheetAttribution.Dates == "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]").filter(FactsheetAttribution.Is_Deleted != 1).all()

        sql_benchmark = current_app.store.db.query(MFSecurity.BenchmarkIndices_Id, MFSecurity.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(Plans.Plan_Id == plan_id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).one_or_none()

        if not sql_attribution:
            months = diff_month(to_date, from_date)
            period = "NA"
            if months < 12:
                period = str(months) + "M"
            elif months > 11 and months < 24:
                period = "1Y"
            elif months == 24:
                period = "2Y"
            elif months > 24:
                period = "3Y"

            #TODO call Gsquare api directly from here
            gsquare_url = current_app.config['GSQUARE_URL']
            attri_gsquare_resp = generate_attributions(from_date, to_date, plan_id, sql_benchmark.BenchmarkIndices_Id, period, gsquare_url, current_app.store.db)

            sql_attribution = current_app.store.db.query(FactsheetAttribution.Response_Attribution).filter(FactsheetAttribution.Plan_Id == plan_id).filter(FactsheetAttribution.Dates == "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]").filter(FactsheetAttribution.Is_Deleted != 1).all()

        if sql_attribution:
            # res = dict()
            attribution = json.loads(sql_attribution[0][0])
            attribution_summary = attribution['attribution_summary']

            res["scheme_id"] = sql_benchmark.Fund_Id
            
            res["portfolio_return"] = round(100 * attribution_summary[0]["fund_returns"], 2) if attribution_summary[0]["fund_returns"] else "NA"
            res["benchmark_return"] = round(100 * attribution_summary[0]["index_returns"], 2) if attribution_summary[0]["index_returns"] else "NA"
            res["active_returns"] = round(100 * attribution_summary[0]["excess_returns"], 2) if attribution_summary[0]["excess_returns"] else "NA"
            res["cash_contribution"] = round(100 * attribution_summary[0]["cash_active_contribution"], 2) if attribution_summary[0]["cash_active_contribution"] else "NA"
            res["allocation_effect"] = round(100 * attribution_summary[0]["sector_allocation"], 2) if attribution_summary[0]["sector_allocation"] else "NA"
            res["selection_effect"] = round(100 * attribution_summary[0]["stock_selection"], 2) if attribution_summary[0]["stock_selection"] else "NA"
            res["interaction_effect"] = round(100 * attribution_summary[0]["interaction"], 2) if attribution_summary[0]["interaction"] else "NA"
            res["timing_effect"] = round(100 * attribution_summary[0]["timing"], 2) if attribution_summary[0]["timing"] else "NA"
        else:
            if attri_gsquare_resp:
                raise BadRequest(F"Gsquare response - {attri_gsquare_resp}")

    return jsonify(res)


# PMS/AIF Fund Sector Attribution
@final_api_bp.route("/api/v1/fund_sector_attribution", methods=['GET'])
@required_access_l3
def get_sectorattribution():
    from_date = request.args.get("from_date", type=str)
    to_date = request.args.get("to_date", type=str)
    
    resp = list()
    resp_sector = list()
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if not from_date:
        raise BadRequest("From date required.")
    
    if not to_date:
        raise BadRequest("To date required.")

    from_date = parser.parse(from_date)
    to_date = parser.parse(to_date)

    if from_date > to_date:
        raise BadRequest("From date cannot be greater than to date.")

    if plan_id:
        #check if factsheet is available for given to date
        sql_factsheet = current_app.store.db.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.TransactionDate == to_date).filter(FactSheet.Is_Deleted != 1).one_or_none()
        if not sql_factsheet:
            raise BadRequest("Factsheet not available for given date.")

        sql_attribution = current_app.store.db.query(FactsheetAttribution.Response_Attribution).filter(FactsheetAttribution.Plan_Id == plan_id).filter(FactsheetAttribution.Dates == "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]").filter(FactsheetAttribution.Is_Deleted != 1).all()

        if not sql_attribution:
            sql_benchmark = current_app.store.db.query(MFSecurity.BenchmarkIndices_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(Plans.Plan_Id == plan_id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).one_or_none()

            months = diff_month(to_date, from_date)
            period = "NA"
            if months < 12:
                period = str(months) + "M"
            elif months > 11 and months < 24:
                period = "1Y"
            elif months == 24:
                period = "2Y"
            elif months > 24:
                period = "3Y"

            #TODO call Gsquare api directly from here
            gsquare_url = current_app.config['GSQUARE_URL']
            status_code = generate_attributions(from_date, to_date, plan_id, sql_benchmark.BenchmarkIndices_Id, period, gsquare_url, current_app.store.db)

            sql_attribution = current_app.store.db.query(FactsheetAttribution.Response_Attribution).filter(FactsheetAttribution.Plan_Id == plan_id).filter(FactsheetAttribution.Dates == "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]").filter(FactsheetAttribution.Is_Deleted != 1).all()

        if sql_attribution:
            fund_id = get_fundid_byplanid(current_app.store.db, plan_id)
            attribution_details = json.loads(sql_attribution[0][0])
            attributions = attribution_details['attribution_details'][0]
            for attribution in attributions:
                res = dict()
                res["scheme_id"] = fund_id
                res["sector_name"] = attribution["Sector"]
                res["contribution"] = attribution["active_contribution"]
                res["allocation"] = attribution["sector_allocation"]
                res["selection"] = attribution["stock_selection"]
                res["interaction"] = attribution["interaction"]

                if not attribution["Sector"] in resp_sector:
                    resp.append(res)
                    resp_sector.append(attribution["Sector"])

    return jsonify(resp)


@final_api_bp.route("/api/v1/get_attribution_data", methods=['GET'])
def get_attributiondata():
    from_date = request.args.get("from_date", type=str)
    to_date = request.args.get("to_date", type=str)
    benchmark_id = request.args.get("benchmark_id", type=str)
    
    requestdata = get_requestdata(request)

    resp = dict()
    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if not from_date:
        raise BadRequest("From date required.")
    
    if not to_date:
        raise BadRequest("To date required.")

    from_date = parser.parse(from_date)
    to_date = parser.parse(to_date)

    if from_date > to_date:
        raise BadRequest("From date cannot be greater than to date.")

    #validations
    validations = attribution_validations(current_app.store.db, plan_id, to_date)
    if validations:
        raise BadRequest(validations)

    if plan_id:
        gsquare_url = current_app.config['GSQUARE_URL']
        resp = get_attributions(current_app.store.db, from_date, to_date, plan_id, gsquare_url, benchmark_id)
            
    return resp


# FactSheet/M10          
@final_api_bp.route("/api/v1/get_nav", methods=['GET'])
@required_access_l2
def get_nav():
    transaction_date = request.args.get("date", type=str)
    dataset_type = request.args.get("dataset_type", type=str, default=None)

    plan_id = None
    resp = list()
    requestdata = get_requestdata(request)
    if not requestdata["plan_id"]:
        plan_id = get_plan_id(current_app.store.db,
                                requestdata["plan_code"],
                                requestdata["plan_name"],
                                requestdata["scheme_id"],
                                requestdata["scheme_code"],
                                requestdata["scheme_name"],
                                isin = request.args.get("isin", type=str),
                                amfi_code = request.args.get("amfi_code", type=str))
    else:
        plan_id = requestdata["plan_id"]

    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] and not plan_id:
        raise BadRequest("Parameters required.")

    if plan_id:
        resp = get_fund_nav(current_app.store.db, plan_id, transaction_date, dataset_type)

    return jsonify(resp)


@final_api_bp.route('/api/v1/product')
def get_product():
    # Product/M00
    product_id = request.args.get('product_id',type=int)

    resp = list()
    query_product = None

    query_product = current_app.store.db.query(Product.Product_Name, Product.Product_Code, Product.ProductCategory_Id, Product.ProductType_Id, Product.AssetClass_Id, Product.Issuer_Id).select_from(Product).join(PlanProductMapping, PlanProductMapping.Product_Id == Product.Product_Id).group_by(Product.Product_Name, Product.Product_Code, Product.ProductCategory_Id, Product.ProductType_Id, Product.AssetClass_Id, Product.Issuer_Id).filter(Product.Is_Deleted != 1)
 
    if product_id:
        query_product = query_product.filter(Product.Product_Id == product_id)

    sql_product = query_product.order_by(Product.Product_Name).all()

    for sql_obj in sql_product:
        json_obj = dict()
        json_obj["Product_Name"] = sql_obj.Product_Name
        json_obj["Product_Code"] = sql_obj.Product_Code
        json_obj["ProductCategory_Id"] = sql_obj.ProductCategory_Id
        json_obj["ProductType_Id"] = sql_obj.ProductType_Id
        json_obj["AssetClass_Id"] = sql_obj.AssetClass_Id
        json_obj["Issuer_Id"] = sql_obj.Issuer_Id       
         
        resp.append(json_obj)

    return jsonify(resp)


@final_api_bp.route("/api/v1/attributiondates")
def get_attributiondates():    
    transactiondate = request.args.get("date", type=str)
    fundid = None
    transactiondate1 = None

    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    fundid = get_fundid_byplanid(current_app.store.db, plan_id)
    
    if plan_id:
        if not transactiondate:
            transactiondate = get_last_transactiondate(current_app.store.db, plan_id)

    holding_query1 = current_app.store.db.query(UnderlyingHoldings.Portfolio_Date).select_from(UnderlyingHoldings).join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(MFSecurity.Status_Id == 1).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(UnderlyingHoldings.Is_Deleted != 1).filter(Plans.Plan_Id == plan_id).filter(UnderlyingHoldings.Portfolio_Date <= transactiondate)

    holding = holding_query1.order_by(desc(UnderlyingHoldings.Portfolio_Date)).first()
    if holding:        
        portfolio_date = holding.Portfolio_Date
        transactiondate1 = str(portfolio_date.year) + "-" + str(portfolio_date.month) + "-" + str(portfolio_date.day)
    
    if transactiondate1:
        transactiondate1 = strptime(transactiondate1, '%Y-%m-%d')
    else:
        transactiondate1 = strptime(transactiondate, '%Y-%m-%d')

    month1from = getbetweendate(0,0,transactiondate1)
    month1to = getbetweendate(0,0,transactiondate1,False)
    month3from = getbetweendate(2,0,transactiondate1)
    month3to = getbetweendate(2,0,transactiondate1,False)
    month6from = getbetweendate(5,0,transactiondate1)
    month6to = getbetweendate(5,0,transactiondate1,False)
    year1from = getbetweendate(1,1,transactiondate1)
    year1to = getbetweendate(1,1,transactiondate1,False)
    year2from = getbetweendate(1,2,transactiondate1)
    year2to = getbetweendate(1,2,transactiondate1,False)
    year3from = getbetweendate(1,3,transactiondate1)
    year3to = getbetweendate(1,3,transactiondate1,False)

    resp = list()

    index_weight_1m = get_max_indexweightage_bydate(current_app.store.db, month1to, plan_id)

    sql_1m = current_app.store.db.query(func.max(UnderlyingHoldings.Portfolio_Date).label("Portfolio_Date")).filter(UnderlyingHoldings.Portfolio_Date >= month1from).filter(UnderlyingHoldings.Portfolio_Date <= month1to).filter(UnderlyingHoldings.Fund_Id == fundid).filter(UnderlyingHoldings.Is_Deleted != 1).all()
    
    if sql_1m and index_weight_1m:
        if sql_1m[0][0]:
            json = dict()
            json["id"] = "1M"
            date = datetime(sql_1m[0][0].year, sql_1m[0][0].month,1)
            json["Value"] = "1M - (" + str(date.strftime('%d %b %Y')) + ")"
            resp.append(json)

    index_weight_3m = get_max_indexweightage_bydate(current_app.store.db, month3to, plan_id)

    sql_3m = current_app.store.db.query(func.max(UnderlyingHoldings.Portfolio_Date).label("Portfolio_Date")).filter(UnderlyingHoldings.Portfolio_Date >= month3from).filter(UnderlyingHoldings.Portfolio_Date <= month3to).filter(UnderlyingHoldings.Fund_Id == fundid).filter(UnderlyingHoldings.Is_Deleted != 1).all()

    if sql_3m and index_weight_3m:
        if sql_3m[0][0]:
            json = dict()
            json["id"] = "3M"
            date = datetime(sql_3m[0][0].year, sql_3m[0][0].month,1)
            json["Value"] = "3M - (" + str(date.strftime('%d %b %Y')) + ")"
            resp.append(json)

    index_weight_6m = get_max_indexweightage_bydate(current_app.store.db, month6to, plan_id)

    sql_6m = current_app.store.db.query(func.max(UnderlyingHoldings.Portfolio_Date).label("Portfolio_Date")).filter(UnderlyingHoldings.Portfolio_Date >= month6from).filter(UnderlyingHoldings.Portfolio_Date <= month6to).filter(UnderlyingHoldings.Fund_Id == fundid).filter(UnderlyingHoldings.Is_Deleted != 1).all()

    if sql_6m and index_weight_6m:
        if sql_6m[0][0]:
            json = dict()
            json["id"] = "6M"
            date = datetime(sql_6m[0][0].year, sql_6m[0][0].month,1)
            json["Value"] = "6M - (" + str(date.strftime('%d %b %Y')) + ")"
            resp.append(json)

    index_weight_1y = get_max_indexweightage_bydate(current_app.store.db, year1to, plan_id)

    sql_1y = current_app.store.db.query(func.max(UnderlyingHoldings.Portfolio_Date).label("Portfolio_Date")).filter(UnderlyingHoldings.Portfolio_Date >= year1from).filter(UnderlyingHoldings.Portfolio_Date <= year1to).filter(UnderlyingHoldings.Fund_Id == fundid).filter(UnderlyingHoldings.Is_Deleted != 1).all()

    if sql_1y and index_weight_1y:
        if sql_1y[0][0]:
            json = dict()
            json["id"] = "1Y"
            date = datetime(sql_1y[0][0].year, sql_1y[0][0].month,1)
            json["Value"] = "1Y - (" + str(date.strftime('%d %b %Y')) + ")"
            resp.append(json)

    index_weight_2y = get_max_indexweightage_bydate(current_app.store.db, year2to, plan_id)

    sql_2y = current_app.store.db.query(func.max(UnderlyingHoldings.Portfolio_Date).label("Portfolio_Date")).filter(UnderlyingHoldings.Portfolio_Date >= year2from).filter(UnderlyingHoldings.Portfolio_Date <= year2to).filter(UnderlyingHoldings.Fund_Id == fundid).filter(UnderlyingHoldings.Is_Deleted != 1).all()

    if sql_2y and index_weight_2y:
        if sql_2y[0][0]:  
            json = dict()
            json["id"] = "2Y"
            date = datetime(sql_2y[0][0].year, sql_2y[0][0].month,1)
            json["Value"] = "2Y - (" + str(date.strftime('%d %b %Y')) + ")"
            resp.append(json)

    index_weight_3y = get_max_indexweightage_bydate(current_app.store.db, year3to, plan_id)

    sql_3y = current_app.store.db.query(func.max(UnderlyingHoldings.Portfolio_Date).label("Portfolio_Date")).filter(UnderlyingHoldings.Portfolio_Date >= year3from).filter(UnderlyingHoldings.Portfolio_Date <= year3to).filter(UnderlyingHoldings.Fund_Id == fundid).filter(UnderlyingHoldings.Is_Deleted != 1).all()

    if sql_3y and index_weight_3y:
        if sql_3y[0][0]:
            json = dict()
            json["id"] = "3Y"
            date = datetime(sql_3y[0][0].year, sql_3y[0][0].month,1)
            json["Value"] = "3Y - (" + str(date.strftime('%d %b %Y')) + ")"
            resp.append(json)
    return jsonify(resp)


# FactSheet/GetMarketCapCompositionMonthWise
@final_api_bp.route("/api/v1/get_marketcapcomposition_month_wise")
def get_marketcapcomposition_month_wise():
    transaction_date = request.args.get("date", type=str)

    resp = list()
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(current_app.store.db, plan_id)

    resp = marketcapcomposition_month_wise(current_app.store.db, plan_id, transaction_date)
    
    return jsonify(resp)


# FactSheet/GetSectorWeightsMonthWise
@final_api_bp.route("/api/v1/get_sectorweights_month_wise")
def get_sectorweights_month_wise():
    transaction_date = request.args.get("date", type=str)
    
    resp = list()
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(current_app.store.db, plan_id)

    sql_factsheetdata = current_app.store.db.query(PortfolioSectors.Percentage_To_AUM.label('value'),
                                                   Sector.Sector_Name.label('sector_name'),
                                                   PortfolioSectors.Portfolio_Date,
                                                   extract('year', PortfolioSectors.Portfolio_Date).label('year'),
                                                   extract('month', PortfolioSectors.Portfolio_Date).label('month'))\
                                            .join(Sector, Sector.Sector_Code == PortfolioSectors.Sector_Code, isouter=True)\
                                            .filter(PortfolioSectors.Plan_Id == plan_id)\
                                            .filter(PortfolioSectors.Portfolio_Date <= transaction_date)\
                                            .filter(PortfolioSectors.Is_Deleted != 1)\
                                            .filter(Sector.Is_Deleted != 1)\
                                            .all()

    # convert the query result to dataframe 
    df = pd.DataFrame(sql_factsheetdata)

    if not df.empty:
        # get the latest dates for every month of each year
        latest_dates_by_mnth = df.sort_values('Portfolio_Date').groupby(['year', 'month']).tail(1).Portfolio_Date
        
        # filter for latest_dates_by_mnth and aggregate the % to aum
        df = df[df['Portfolio_Date'].isin(latest_dates_by_mnth)] \
                                    .groupby(['sector_name', 'year', 'month'], as_index=False) \
                                    .agg({'value': 'sum', 
                                        'sector_name': 'first', 
                                        'year': 'first', 
                                        'month': 'first'})
        
        result = df.to_json(orient="records")
        parsed = json.loads(result)
        return jsonify(parsed)
    else:
        return jsonify([])


# FactSheet/GetTrendAUMMonthWise
@final_api_bp.route("/api/v1/get_trend_aum_month_wise")
def get_trend_aum_month_wise():
    transaction_date = request.args.get("date", type=str)

    resp = list()
    requestdata = get_requestdata(request)
    
    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if plan_id:
       resp = get_aum_monthwise(current_app.store.db, plan_id, transaction_date)

    return jsonify(resp)


# FactSheet/GetInvestmentStyleMonthWise
@final_api_bp.route("/api/v1/get_investmentstyle_month_wise")
def get_investmentstyle_month_wise():
    transaction_date = request.args.get("date", type=str)
    
    resp = list()
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(current_app.store.db, plan_id)

    df = investmentstyle_month_wise(current_app.store.db, plan_id, transaction_date)

    if not df.empty:        
        result = df.to_json(orient="records")
        parsed = json.loads(result)

        return jsonify(parsed)
    else:
        return jsonify([])


@final_api_bp.route('/api/v1/plans')
@required_access_l1
def get_plans():
    reqd_parameters = ["plan_id", "plan_name", "product_id", "scheme_id", "isin", "amfi_code"] # "is_filter" ]
    requestdata = {}
    for p in reqd_parameters:
        requestdata[p] = request.args.get(p)

    # if not any(requestdata.get(key) for key in reqd_parameters):
    #     raise BadRequest("Atleast one of the following parameters are required : " + " | ".join(reqd_parameters))
 
    plan_id = requestdata.get('plan_id')
    plan_name = requestdata.get('plan_name')    
    product_id = requestdata.get('product_id')
    fund_id = requestdata.get('scheme_id')
    isin = requestdata.get('isin')
    amfi_code = requestdata.get('amfi_code')

    resp = list()

    query_plans = current_app.store.db.query(
                                                Plans.Plan_Id, 
                                                Plans.Plan_Name, 
                                                Plans.Plan_Code, 
                                                MFSecurity.MF_Security_Name,
                                                MFSecurity.Fund_Id, 
                                                PlanType.PlanType_Name, 
                                                Options.Option_Name, 
                                                Plans.Plan_DivReinvOption, 
                                                Plans.Plan_External_Map_Code, 
                                                Plans.Plan_Demat, 
                                                Plans.Plan_RTA_AMC_Code, 
                                                Plans.ISIN,
                                                Plans.ISIN2, 
                                                Plans.RTA_Code, 
                                                Plans.RTA_Name, 
                                                Plans.AMFI_Code, 
                                                Plans.AMFI_Name, 
                                                Product.Product_Id, 
                                                Product.Product_Name
                                            ).select_from(Plans) \
                                             .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id) \
                                             .join(Product, Product.Product_Id == PlanProductMapping.Product_Id) \
                                             .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id) \
                                             .join(PlanType, PlanType.PlanType_Id == Plans.PlanType_Id, isouter=True) \
                                             .join(Options, Options.Option_Id == Plans.Option_Id, isouter=True) \
                                             .join(AssetClass, AssetClass.AssetClass_Id == MFSecurity.AssetClass_Id, isouter=True) \
                                             .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1,
                                                     Product.Is_Deleted != 1, MFSecurity.Status_Id == 1)
    
    if plan_id:
        query_plans = query_plans.filter(Plans.Plan_Id == plan_id)

    if plan_name:
        query_plans = query_plans.filter(Plans.Plan_Name.like("%"+ plan_name +"%"))

    if product_id:
        query_plans = query_plans.filter(Product.Product_Id == product_id)

    if fund_id:
        query_plans = query_plans.filter(MFSecurity.Fund_Id == fund_id)

    if isin:
        query_plans = query_plans.filter(or_(Plans.ISIN == isin, Plans.ISIN2 == isin ))

    if amfi_code:
        query_plans = query_plans.filter(Plans.AMFI_Code == amfi_code)

    sql_plans = query_plans.order_by(Plans.Plan_Name).all()
    
    for sql_obj in sql_plans:
        json_obj = dict()
        json_obj["plan_id"] = sql_obj.Plan_Id
        json_obj["plan_name"] = sql_obj.Plan_Name
        json_obj["plan_code"] = sql_obj.Plan_Code
        json_obj["scheme_id"] = sql_obj.Fund_Id
        json_obj["scheme_name"] = sql_obj.MF_Security_Name
        json_obj["plantype_name"] = sql_obj.PlanType_Name
        json_obj["planoption_name"] = sql_obj.Option_Name
        json_obj["plan_divreinvoption"] = sql_obj.Plan_DivReinvOption
        json_obj["plan_demat"] = sql_obj.Plan_Demat
        json_obj["plan_rta_amc_code"] = sql_obj.Plan_RTA_AMC_Code
        json_obj["plan_isin"] = sql_obj.ISIN
        json_obj["plan_isin2"] = sql_obj.ISIN2
        json_obj["plan_rta_code"] = sql_obj.RTA_Code
        json_obj["plan_rta_name"] = sql_obj.RTA_Name
        json_obj["plan_amfi_code"] = sql_obj.AMFI_Code
        json_obj["plan_amfi_name"] = sql_obj.AMFI_Name
        json_obj["product_id"] = sql_obj.Product_Id
        json_obj["product_name"] = sql_obj.Product_Name
                   
        resp.append(json_obj)

    return jsonify(resp)


@final_api_bp.route("/api/v1/getdetailedportfolio")
def get_detailedportfolio():
    portfolio_date = request.args.get("portfolio_date",type=str)
    plan_id = request.args.get("plan_id",type=int)
    aif_category = request.args.get('aif_category',type=int,default=0)
    aif_sub_category = request.args.get("aif_sub_category",type=str)
    fund_id = request.args.get("fund_id",type=int)

    resp = list()
    if not plan_id:
        raise BadRequest(description="Required Parameter: <plan_id>")

    if not portfolio_date:
        raise BadRequest(description="Required Parameter: <portfolio_date>")

    resp = get_detailed_fund_holdings(current_app.store.db, plan_id, fund_id, portfolio_date)

    return jsonify(resp)


@final_api_bp.route("/api/v1/getfundstockdetails")
@required_access_l1
def get_fundstock_details():
    # FavoriteStocks/M06
    isin_code = request.args.get("isin_code", type=str)

    if not isin_code:
        raise BadRequest("ISIN Code is required.")

    resp = dict()
    mf_count = 0
    mf_aum = 0
    ulip_count = 0
    ulip_aum = 0
    pms_count = 0
    pms_aum = 0
    aif_count = 0
    aif_aum = 0

    db_session = current_app.store.db
    portfolio_date = db_session.query(func.max(FundStocks.Portfolio_Date).label("Portfolio_Date")).filter(FundStocks.ISIN_Code == isin_code).scalar()

    fundamental_data = get_equity_fundamentals(db_session, [isin_code], portfolio_date)

    mf_data = get_aumandfundcountbyproduct(db_session, isin_code, 1)

    if mf_data:
        mf_count = mf_data["count"]
        mf_aum = mf_data["aum"]

    ulip_data = get_aumandfundcountbyproduct(db_session, isin_code, 2)

    if ulip_data:
        ulip_count = ulip_data["count"]
        ulip_aum = ulip_data["aum"]

    pms_data = get_aumandfundcountbyproduct(db_session, isin_code, 4)

    if pms_data:
        pms_count = pms_data["count"]
        pms_aum = pms_data["aum"]

    aif_data = get_aumandfundcountbyproduct(db_session, isin_code, 5)

    if aif_data:
        aif_count = aif_data["count"]
        aif_aum = aif_data["aum"]

    if fundamental_data.shape[0] == 1:
        data = dict()
        data["isin_code"] = isin_code
        data["security_name"] = fundamental_data['security_name'].iloc[0]
        data["sector_name"] = fundamental_data['sector'].iloc[0]
        data["marketcap"] = fundamental_data['market_cap'].iloc[0]
        data["total_mf_funds"] = mf_count
        data["total_mf_aum"] = round(mf_aum, 2)
        data["total_ulip_funds"] = ulip_count
        data["total_ulip_aum"] = round(ulip_aum, 2)
        data["total_pms_funds"] = pms_count
        data["total_pms_aum"] = round(pms_aum, 2)
        data["total_aif_funds"] = aif_count
        data["total_aif_aum"] = round(aif_aum, 2)
        data["p_e"] = fundamental_data['pe'].iloc[0]
        data["e_p_s"] = fundamental_data['eps'].iloc[0]
        data["div_yield"] = fundamental_data['div_yld'].iloc[0]
        data["market_cap"] = fundamental_data['market_cap_cr'].iloc[0]
        data["fundamentals_asof_date"] = fundamental_data['pricedate'].iloc[0]

        resp["favorite_stock_detail"] = data

    # favorite_stock_funds
    sql_favoritestockfunds = current_app.store.db.query(FundStocks.Fund_Id, FundStocks.Plan_Id, FundStocks.Plan_Name, FundStocks.Product_Name, FundStocks.Product_Id, FundStocks.Percentage_to_AUM, FundStocks.Diff_Percentage_to_AUM, FundStocks.Purchase_Date, FundStocks.IncreaseExposure, FundStocks.DecreaseExposure).filter(FundStocks.ISIN_Code == isin_code).order_by(desc(FundStocks.Percentage_to_AUM)).all()

    favoritestockfunds_list = list()
    if sql_favoritestockfunds:
        for favoritestockfunds in sql_favoritestockfunds:
            data = dict()
            data["plan_id"] = favoritestockfunds.Plan_Id
            data["plan_name"] = favoritestockfunds.Plan_Name
            data["product_name"] = favoritestockfunds.Product_Name
            data["product_id"] = favoritestockfunds.Product_Id
            data["percentage_to_aum"] = favoritestockfunds.Percentage_to_AUM
            data["diff_percentage_to_aum"] = favoritestockfunds.Diff_Percentage_to_AUM
            data["purchase_date"] = favoritestockfunds.Purchase_Date
            data["increaseexposure"] = favoritestockfunds.IncreaseExposure
            data["decreaseexposure"] = favoritestockfunds.DecreaseExposure
            
            sql_fundmanagers = current_app.store.db.query(FundManager.FundManager_Name,
                                                          FundManager.FundManager_Code
                                                        )\
                                .filter(FundManager.Fund_Id == favoritestockfunds.Fund_Id)\
                                .filter(FundManager.Is_Deleted != 1)\
                                .filter(
                                    or_(FundManager.DateTo >= date.today(), FundManager.DateTo == None)
                                )\
                                .all()

            fundmanager_names = list()
            if sql_fundmanagers:
                for sql_fundmanager in sql_fundmanagers:
                    fundmanager = dict()
                    fundmanager["fund_manager_name"] = sql_fundmanager.FundManager_Name
                    fundmanager["fund_manager_code"] = sql_fundmanager.FundManager_Code
                    fundmanager_names.append(fundmanager)

            data["fund_manager"] = fundmanager_names
            favoritestockfunds_list.append(data)

    resp["favorite_stock_funds"] = favoritestockfunds_list
    
    # favorite_stock_funds_manager
    sql_favoritestockfundsmanager = current_app.store.db.query(FundStocks.Product_Name, 
                                                               FundStocks.Product_Id, 
                                                               FundManager.FundManager_Name, 
                                                               FundManager.FundManager_Code,
                                                               func.count(FundStocks.Product_Code)).select_from(FundStocks)\
                                                        .join(FundManager, FundManager.Fund_Id == FundStocks.Fund_Id)\
                                                        .filter(FundStocks.ISIN_Code == isin_code, 
                                                                FundManager.Is_Deleted != 1,
                                                                FundManager.DateTo == None)\
                                                        .group_by(FundManager.FundManager_Name, 
                                                                FundManager.FundManager_Code,
                                                                FundStocks.Product_Name, 
                                                                FundStocks.Product_Id)\
                                                        .order_by(desc(func.count(FundStocks.Product_Code)))\
                                                        .all()


    favoritestockfundsmanager_list = list()
    if sql_favoritestockfundsmanager:
        for favoritestockfundsmanager in sql_favoritestockfundsmanager:
            data = dict()
            data["product_name"] = favoritestockfundsmanager.Product_Name
            data["product_id"] = favoritestockfundsmanager.Product_Id
            data["fund_manager_name"] = favoritestockfundsmanager.FundManager_Name
            data["fund_manager_code"] = favoritestockfundsmanager.FundManager_Code
            data["total_funds"] = favoritestockfundsmanager[4]

            favoritestockfundsmanager_list.append(data)

    resp["favorite_stock_funds_manager"] = favoritestockfundsmanager_list
    

    return jsonify(resp)


@final_api_bp.route('/api/v1/funds_in_category')
@required_access_l2
def funds_in_category():
    classification_id = request.args.get('classification_id', type=int)
    lst_product_id = request.args.getlist('product_id', type=int)
    plan_type_id = request.args.get('plan_type_id', type=int)
    asset_class_id = request.args.get('asset_class_id', type=int)
    amc_id = request.args.get('amc_id', type=int)
    is_top_5 = request.args.get('top_5', type=int, default=1)
    order_by = request.args.get('order_by', type=str, default='scheme_return_1year')
    sort_order = request.args.get('asc', type=int, default=1)
    is_gold_plans = request.args.get('is_gold_plans', type=int, default=0) # for gold etf list on the Portfolio X-Ray performance tab

    resp = list()
    sql_plans = current_app.store.db.query(func.max(FactSheet.TransactionDate), Plans.Plan_Id) \
                                    .select_from(FactSheet).join(Plans, Plans.Plan_Id == FactSheet.Plan_Id) \
                                    .join(Options, Options.Option_Id == Plans.Option_Id) \
                                    .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id) \
                                    .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id) \
                                    .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                    .filter(Plans.Is_Deleted != 1, FactSheet.Is_Deleted != 1,
                                            MFSecurity.Is_Deleted != 1, Classification.Is_Deleted != 1,
                                            MFSecurity.Status_Id == 1).filter(Options.Option_Name.ilike("%G%"))
    
    if classification_id:
        sql_plans = sql_plans.filter(Classification.Classification_Id == classification_id)

    if asset_class_id:
        sql_plans = sql_plans.filter(Classification.AssetClass_Id == asset_class_id)

    if amc_id:
        sql_plans = sql_plans.filter(MFSecurity.AMC_Id == amc_id)

    if plan_type_id:
        sql_plans = sql_plans.filter(Plans.PlanType_Id == plan_type_id)

    if len(lst_product_id) > 0:
        sql_plans = sql_plans.filter(PlanProductMapping.Product_Id.in_(lst_product_id))

    # for gold etf list on the Portfolio X-Ray performance tab
    if is_gold_plans:
        sql_plans = sql_plans.filter(Plans.Plan_Name.like(r'%gold%'))

    sql_plans = sql_plans.group_by(Plans.Plan_Id).all()
    
    if sql_plans:
        for plan in sql_plans:
            date = plan[0]
            plan_id = plan[1]
            sql_factsheet = current_app.store.db.query(
                                                        Plans.Plan_Id, 
                                                        Plans.Plan_Name, 
                                                        AMC.AMC_Id, 
                                                        AMC.AMC_Name, 
                                                        AMC.AMC_Logo, 
                                                        Product.Product_Id, 
                                                        Product.Product_Code, 
                                                        Product.Product_Name, 
                                                        FactSheet.NetAssets_Rs_Cr, 
                                                        FactSheet.TransactionDate, 
                                                        FactSheet.SCHEME_RETURNS_1MONTH, 
                                                        FactSheet.SCHEME_RETURNS_3MONTH, 
                                                        FactSheet.SCHEME_RETURNS_6MONTH, 
                                                        FactSheet.SCHEME_RETURNS_1YEAR, 
                                                        FactSheet.SCHEME_RETURNS_2YEAR, 
                                                        FactSheet.SCHEME_RETURNS_3YEAR, 
                                                        FactSheet.SCHEME_RETURNS_5YEAR, 
                                                        FactSheet.SCHEME_RETURNS_10YEAR, 
                                                        FactSheet.SCHEME_RETURNS_since_inception, 
                                                        FactSheet.SCHEME_BENCHMARK_RETURNS_1MONTH, 
                                                        FactSheet.SCHEME_BENCHMARK_RETURNS_3MONTH, 
                                                        FactSheet.SCHEME_BENCHMARK_RETURNS_6MONTH, 
                                                        FactSheet.SCHEME_BENCHMARK_RETURNS_1YEAR, 
                                                        FactSheet.SCHEME_BENCHMARK_RETURNS_3YEAR, 
                                                        FactSheet.SCHEME_BENCHMARK_RETURNS_2YEAR, 
                                                        FactSheet.SCHEME_BENCHMARK_RETURNS_5YEAR, 
                                                        FactSheet.SCHEME_BENCHMARK_RETURNS_SI, 
                                                        FactSheet.ExpenseRatio,
                                                        Classification.Classification_Name,
                                                        BenchmarkIndices.TRI_Co_Code, 
                                                        BenchmarkIndices.BenchmarkIndices_Name,
                                                        MFSecurity.Classification_Id,
                                                        MFSecurity.AssetClass_Id
                                                       ).select_from(FactSheet) \
                                                        .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id) \
                                                        .join(Options, Options.Option_Id == Plans.Option_Id) \
                                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id) \
                                                        .join(Product, Product.Product_Id == PlanProductMapping.Product_Id) \
                                                        .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id) \
                                                        .join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id) \
                                                        .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id) \
                                                        .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id) \
                                                        .join(TRIReturns, and_(TRIReturns.TRI_Co_Code == BenchmarkIndices.TRI_Co_Code, TRIReturns.TRI_IndexDate == FactSheet.TransactionDate), isouter = True) \
                                                        .filter(Plans.Is_Deleted != 1).filter(FactSheet.Is_Deleted != 1) \
                                                        .filter(MFSecurity.Is_Deleted != 1).filter(Classification.Is_Deleted != 1) \
                                                        .filter(MFSecurity.Status_Id == 1).filter(Options.Option_Name.ilike("%G%")) \
                                                        .filter(Plans.Plan_Id == plan_id).filter(FactSheet.TransactionDate == date) \
                                                        .filter(AMC.Is_Deleted != 1).all()

            if sql_factsheet:
                data = dict()
                data["plan_id"] = sql_factsheet[0].Plan_Id
                data["plan_name"] = sql_factsheet[0].Plan_Name
                data["amc_id"] = sql_factsheet[0].AMC_Id
                data["amc_name"] = sql_factsheet[0].AMC_Name
                data["amc_logo"] = sql_factsheet[0].AMC_Logo
                data["product_id"] = sql_factsheet[0].Product_Id
                data["product_code"] = sql_factsheet[0].Product_Code
                data["product_name"] = sql_factsheet[0].Product_Name
                data["benchmark_name"] = sql_factsheet[0].BenchmarkIndices_Name
                data["as_on_date"] = sql_factsheet[0].TransactionDate
                data["aum"] = sql_factsheet[0].NetAssets_Rs_Cr
                data["scheme_return_1month"] = sql_factsheet[0].SCHEME_RETURNS_1MONTH
                data["scheme_return_3month"] = sql_factsheet[0].SCHEME_RETURNS_3MONTH
                data["scheme_return_6month"] = sql_factsheet[0].SCHEME_RETURNS_6MONTH
                data["scheme_return_1year"] = sql_factsheet[0].SCHEME_RETURNS_1YEAR
                data["scheme_return_2year"] = sql_factsheet[0].SCHEME_RETURNS_2YEAR
                data["scheme_return_3year"] = sql_factsheet[0].SCHEME_RETURNS_3YEAR
                data["scheme_return_5year"] = sql_factsheet[0].SCHEME_RETURNS_5YEAR
                data["scheme_return_10year"] = sql_factsheet[0].SCHEME_RETURNS_10YEAR
                data["scheme_return_si"] = sql_factsheet[0].SCHEME_RETURNS_since_inception
                data["classification_name"] = sql_factsheet[0].Classification_Name
                data["classification_id"] = sql_factsheet[0].Classification_Id
                data["asset_class_id"] = sql_factsheet[0].AssetClass_Id
                data["scheme_expense_ratio"] = sql_factsheet[0].ExpenseRatio


                product_code = sql_factsheet[0].Product_Code
                tri_co_code = sql_factsheet[0].TRI_Co_Code if sql_factsheet[0].TRI_Co_Code else ''

                if product_code == "PMS" or tri_co_code == '':
                    data["bm_ret_1m"] = sql_factsheet[0].SCHEME_BENCHMARK_RETURNS_1MONTH
                    data["bm_ret_3m"] = sql_factsheet[0].SCHEME_BENCHMARK_RETURNS_3MONTH
                    data["bm_ret_6m"] = sql_factsheet[0].SCHEME_BENCHMARK_RETURNS_6MONTH
                    data["bm_ret_1y"] = sql_factsheet[0].SCHEME_BENCHMARK_RETURNS_1YEAR
                    data["bm_ret_2y"] = sql_factsheet[0].SCHEME_BENCHMARK_RETURNS_2YEAR
                    data["bm_ret_3y"] = sql_factsheet[0].SCHEME_BENCHMARK_RETURNS_3YEAR
                    data["bm_ret_5y"] = sql_factsheet[0].SCHEME_BENCHMARK_RETURNS_5YEAR
                    data["bm_ret_ince"] = sql_factsheet[0].SCHEME_BENCHMARK_RETURNS_SI
                else:
                    transactiondate1 = date
                    transaction_date_1 = transactiondate1 - timedelta(days=1)
                    transaction_date_2 = transactiondate1 - timedelta(days=2)

                    co_code = get_co_code_by_plan_id(current_app.store.db, plan_id)

                    benchmark_ret_data = current_app.store.db.query(TRIReturns.Return_1Month, TRIReturns.Return_3Month, TRIReturns.Return_6Month, TRIReturns.Return_1Year, TRIReturns.Return_3Year).join(BenchmarkIndices, TRIReturns.TRI_Co_Code == BenchmarkIndices.TRI_Co_Code).filter(BenchmarkIndices.Co_Code == co_code).filter(or_(TRIReturns.TRI_IndexDate == date, TRIReturns.TRI_IndexDate == transaction_date_1)).order_by(desc(TRIReturns.TRI_IndexDate)).first()

                    if not benchmark_ret_data:
                        benchmark_ret_data = current_app.store.db.query(TRIReturns.Return_1Month, TRIReturns.Return_3Month, TRIReturns.Return_6Month, TRIReturns.Return_1Year, TRIReturns.Return_3Year).join(BenchmarkIndices, TRIReturns.TRI_Co_Code == BenchmarkIndices.TRI_Co_Code).filter(BenchmarkIndices.Co_Code == co_code).filter(TRIReturns.TRI_IndexDate == transaction_date_2).order_by(desc(TRIReturns.TRI_IndexDate)).first()

                    data["bm_ret_1m"] = round(benchmark_ret_data.Return_1Month, 2) if benchmark_ret_data.Return_1Month else None
                    data["bm_ret_3m"] = round(benchmark_ret_data.Return_3Month, 2) if benchmark_ret_data.Return_3Month else None
                    data["bm_ret_6m"] = round(benchmark_ret_data.Return_6Month, 2) if benchmark_ret_data.Return_6Month else None
                    data["bm_ret_1y"] = round(benchmark_ret_data.Return_1Year, 2) if benchmark_ret_data.Return_1Year else None
                    data["bm_ret_2y"] = 0
                    data["bm_ret_3y"] = round(benchmark_ret_data.Return_3Year, 2) if benchmark_ret_data.Return_3Year else None
                    data["bm_ret_5y"] = 0
                    data["bm_ret_ince"] = 0

                resp.append(data)

    if len(resp) == 0:
        return {}

    df = pd.DataFrame(resp)
    df.sort_values(by=[order_by], ascending=sort_order, inplace=True)
    # display only top 5 funds unless is_top_5 is set 0
    df = df.head(5) if is_top_5 else df
    # add formatting to df
    df['as_on_date'] = df['as_on_date'].dt.strftime('%d-%b-%Y')
    round_columns = ["bm_ret_1m","bm_ret_1y","bm_ret_2y","bm_ret_3m","bm_ret_3y","bm_ret_5y","bm_ret_6m","bm_ret_ince",
                    "scheme_return_10year","scheme_return_1month","scheme_return_1year","scheme_return_2year","scheme_return_3month",
                    "scheme_return_3year","scheme_return_5year","scheme_return_6month","scheme_return_si"]
    df[round_columns] = df[round_columns].apply(pd.to_numeric, errors='coerce')
    df[round_columns] = df[round_columns].round(2)

    result = df.to_json(orient="records")
    parsed = json.loads(result)

    return jsonify(parsed)


@final_api_bp.route('/api/v1/amc_aum_cash_held')
@required_access_l3
def get_amc_aum_cash_held():
    amc_id = request.args.get('amc_id', type=int)
    from_date = request.args.get('from_date', type=str)
    to_date = request.args.get('to_date', type=str)
    product_id = request.args.get('product_id', type=int)    

    if not from_date and not to_date:
        raise BadRequest("From date and To date are required.")

    db_session = current_app.store.db

    sql_factsheet_qry = db_session.query(MFSecurity.AMC_Id,
                                         MFSecurity.Fund_Id,
                                         FactSheet.Portfolio_Date.label('asof_date'),
                                         FactSheet.Portfolio_Date.label('trxn_date'),
                                         (FactSheet.Cash*FactSheet.NetAssets_Rs_Cr/100).label('cash'),\
                                         (FactSheet.NetAssets_Rs_Cr).label('aum'))\
                                    .select_from(FactSheet)\
                                    .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                                    .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                    .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                    .filter(Plans.Is_Deleted != 1, FactSheet.Is_Deleted != 1, MFSecurity.Is_Deleted != 1)\
                                    .filter(MFSecurity.Status_Id == 1, MFSecurity.AMC_Id == amc_id, PlanProductMapping.Product_Id == product_id)\
                                    .filter(FactSheet.Portfolio_Date >= from_date, FactSheet.Portfolio_Date <= to_date)\
                                    .distinct().order_by(FactSheet.Portfolio_Date).all()

    resp = {}
    df = pd.DataFrame(sql_factsheet_qry)
    if df.shape[0] == 0:
        return resp

    df.columns = map(str.lower, df.columns) # column names to lower case
    df['trxn_date'] = pd.to_datetime(df['trxn_date'])
    df = df.sort_values('trxn_date').drop_duplicates(['fund_id', 'asof_date'], keep='last')  # fetch latest factsheet as per transaction date

    df_ = df[['asof_date', 'cash', 'aum']].groupby('asof_date').sum()
    df_.reset_index(inplace=True)
    df_['asof_date'] = pd.to_datetime(df_['asof_date'])
    df_['asof_date_'] = df_['asof_date'].dt.strftime('%b %y')
    df_.drop(columns=['asof_date'], inplace=True)
    df_.rename(columns = {'asof_date_':'asof_date'}, inplace = True)

    resp['aum_cash_level'] = {}
    resp['aum_cash_level']['asof_date'] = df_['asof_date'].to_list()
    resp['aum_cash_level']['aum_data'] = df_['aum'].to_list()
    resp['aum_cash_level']['cash_data'] = df_['cash'].to_list()

    return resp


@final_api_bp.route('/api/v1/nfo_details')
@required_access_l1
def get_nfo_details():
    from_date = request.args.get('from_date', type=str)
    plan_type = request.args.get('plan_type', type=str, default='regular') # plan_type can be 'regular' or 'direct'
    resp = list()
    db_session = current_app.store.db

    sql_data = db_session.query(Fund.Fund_Name,
                                Plans.Plan_Name,
                                Plans.AMFI_Code,
                                Plans.AMFI_Name,
                                Plans.ISIN,
                                Plans.ISIN2,
                                MFSecurity.MF_Security_Investment_Strategy,
                                AMC.AMC_Name,
                                FundType.FundType_Name,
                                Classification.Classification_Name,
                                MFSecurity.MF_Security_OpenDate,
                                MFSecurity.MF_Security_CloseDate,
                                BenchmarkIndices.BenchmarkIndices_Name,
                                MFSecurity.MF_Security_Min_Purchase_Amount,
                                MFSecurity.Fees_Structure,
                                MFSecurity.Risk_Grade,
                                MFSecurity.MF_Security_Min_Lockin_Period,
                                MFSecurity.MF_Security_UnitFaceValue,
                                MFSecurity.MF_Security_Purchase_Multiplies_Amount,
                                MFSecurity.MF_Security_SIP_Min_Amount,
                                Plans.RTA_Name,
                                Plans.RTA_Code,
                                PlanType.PlanType_Id,
                                PlanType.PlanType_Name,
                                Fund.Fund_OfferLink,
                                Fund.Fund_manager)\
                         .select_from(Plans)\
                         .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                         .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                         .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                         .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id)\
                         .join(FundType, FundType.FundType_Id == MFSecurity.FundType_Id)\
                         .join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id)\
                         .join(PlanType, PlanType.PlanType_Id == Plans.PlanType_Id)\
                         .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                         .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, Fund.Is_Deleted != 1, Classification.Is_Deleted != 1)\
                         .filter(PlanProductMapping.Product_Id == 1, BenchmarkIndices.Is_Deleted != 1)\
                         .filter(MFSecurity.Status_Id == 1).filter(or_(MFSecurity.MF_Security_OpenDate >= from_date, MFSecurity.MF_Security_CloseDate >= from_date))\
                         .filter(FundType.Is_Deleted != 1)

    # plan type filter
    if plan_type == 'regular':
        sql_data = sql_data.filter(PlanType.PlanType_Id == 1).all()
    elif plan_type == 'direct':
        sql_data = sql_data.filter(PlanType.PlanType_Id == 2).all()
    else:
        sql_data = sql_data.all()

    if sql_data:
        for data in sql_data:
            res = dict()
            res["scheme_name"] = data.Fund_Name
            res["plan_name"] = data.Plan_Name
            res["amc_name"] = data.AMC_Name
            res["scheme_classification"] = data.Classification_Name
            res["scheme_open_date"] = data.MF_Security_OpenDate
            res["scheme_close_date"] = data.MF_Security_CloseDate
            res["scheme_reopen_date"] = data.MF_Security_CloseDate
            res["scheme_benchmark_name"] = data.BenchmarkIndices_Name
            res["scheme_min_investment"] = data.MF_Security_Min_Purchase_Amount
            res["scheme_min_purchase_multiplies_amount"] = data.MF_Security_Purchase_Multiplies_Amount
            res["scheme_sip_amount_options"] = data.MF_Security_SIP_Min_Amount
            res["scheme_exit_load"] = data.Fees_Structure
            res["scheme_objective"] = data.MF_Security_Investment_Strategy
            res["scheme_isin"] = data.ISIN if data.ISIN else None
            res["scheme_isin2"] = data.ISIN2 if data.ISIN2 else None
            res["scheme_risk_grade"] = data.Risk_Grade
            res["scheme_amfi_name"] = data.AMFI_Name if data.AMFI_Name else None
            res["scheme_amfi_code"] = data.AMFI_Code if data.AMFI_Code and data.AMFI_Code != '0' else None
            res["scheme_rta_name"] = data.RTA_Name if data.RTA_Name else None
            res["scheme_rta_code"] = data.RTA_Code if data.RTA_Code else None
            res["scheme_plan_type"] = data.PlanType_Name
            res["scheme_type"] = data.FundType_Name
            res["scheme_min_lockin_period"] = data.MF_Security_Min_Lockin_Period
            res["scheme_unit_face_value"] = data.MF_Security_UnitFaceValue
            res["scheme_fund_manager"] = data.Fund_manager

            resp.append(res)

    return jsonify(resp)

@final_api_bp.route("/api/v1/instrument_type", methods=['GET'])
@required_access_l2
def get_instrument_type():
    transaction_date = request.args.get("date", type=str)
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    resp = list()    

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(current_app.store.db, plan_id) 
    
        resp = get_instrumenttype(current_app.store.db, plan_id, transaction_date)

    return jsonify(resp)


# Active Rolling Returns
@final_api_bp.route('/api/v1/active_rolling_returns', methods=['GET'])
@required_access_l2
def get_active_rolling_returns():
    transaction_date = request.args.get("date", type=str)    
    timeframe_in_yr = request.args.get("timeframe_in_yr", type=int) #1 or 3 or 5
    requestdata = get_requestdata(request)
    plan_id = requestdata["plan_id"]
    benchmark_id = request.args.get("benchmark_id", type=int)
    
    res = dict()
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")
    
    if not timeframe_in_yr:
        raise BadRequest("timeframe_in_yr field is missing.")
    
    if plan_id:
        transaction_date = get_last_transactiondate(current_app.store.db, plan_id)

        res = generate_active_rolling_returns(current_app.store.db, plan_id, benchmark_id, timeframe_in_yr, transaction_date)

    return jsonify(res)

@final_api_bp.route("/api/v1/generate_factsheet_pdf", methods=['POST'])
@required_access_l2
def generate_factsheet_pdf():
    f = request.json

    plan_id = f["plan_id"] if "plan_id" in f else None 
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db,
                              f["plan_code"],
                              f["plan_name"],
                              f["scheme_id"],
                              f["scheme_code"],
                              f["scheme_name"],
                              isin = f.get("isin"),
                              amfi_code = f.get("amfi_code"))
    
    transaction_date = f["transaction_date"] if "transaction_date" in f else None
    if not transaction_date:
        transaction_date = get_last_transactiondate(current_app.store.db, plan_id).strftime('%Y-%m-%d')
    
    organization_id = f["organization_id"] if "organization_id" in f else None
    attribution_benchmark_id = f["attribution_benchmark_id"] if "attribution_benchmark_id" in f else None

    portfolio_date = None
    attribution_from = None
    attribution_to = None
    send_email = None
    fund_name = None
    client_name = None
    organization_name = None
    product_name = None
    rolling_return_type = None
    selection_dict = dict()

    portfolio_date = f["portfolio_date"] if "portfolio_date" in f else None
    portfolio_date = get_portfolio_date(current_app.store.db, plan_id, transaction_date) if not portfolio_date else portfolio_date
    
    if 'attribution_from' in f:
        attribution_from = f["attribution_from"]
    
    if 'attribution_to' in f:
        attribution_to = f["attribution_to"]

    if not attribution_from and not attribution_from:
        attr_dict = get_default_attribution_dates(current_app.store.db, res_dict=dict(), fundid=None, planid=plan_id, transaction_date=transaction_date, hide_attribution=None)
        if attr_dict['attribution_from'] and attr_dict['attribution_to']:
            attribution_from = attr_dict['attribution_from'].strftime('%Y-%m-%d')
            attribution_to = attr_dict['attribution_to'].strftime('%Y-%m-%d')
    
    if 'selection_data' in f:
        selection_dict = f["selection_data"]

    if not selection_dict:
        selection_dict["page_break"] = False
        selection_dict["section_1"] = True
        selection_dict["section_2"] = True
        selection_dict["portfolio_holdings"] = True
        selection_dict["performance_trend"] = True
        selection_dict["nav_movement"] = True
        selection_dict["rolling_return_1"] = True
        selection_dict["rolling_return_3"] = True
        selection_dict["rolling_return_5"] = True
        selection_dict["performance_graph"] = True
        selection_dict["risk_volatility_measures"] = True
        selection_dict["detailed_portfolio"] = True
        selection_dict["attribution"] = True
        selection_dict["active_rolling_return_1"] = True
        selection_dict["active_rolling_return_3"] = True
    
    if 'rolling_return_type' in f:
        rolling_return_type = f["rolling_return_type"] #annualized/absolute

    if 'is_email' in f:
        send_email = f["is_email"]

        if 'email_id' in f:
            email_id = f["email_id"]        
            fund_name = f["fund_name"]
            client_name = f["client_name"]
            organization_name = f["organization_name"]
            product_name = f["product_name"]
    
    is_annualized_return = True if rolling_return_type == 'annualized' else False
        
    #get oranization logo path
    image_path = current_app.config['IMAGE_PATH']
    whitelabel_dir = current_app.config['WHITELABEL_DIR']
    report_generation_path = current_app.config['REPORT_GENERATION_PATH']
    gsquare_url = current_app.config['GSQUARE_URL']
    
    file_name = get_factsheetpdf(current_app.store.db, plan_id, transaction_date, portfolio_date, attribution_from, attribution_to, organization_id, image_path, whitelabel_dir, report_generation_path, selection_dict, gsquare_url, attribution_benchmark_id, is_annualized_return=is_annualized_return)

    #TODO move this to lib
    if send_email:
        template_vars = dict()
        organization_whitelabel_data = get_organization_whitelabel(current_app.store.db, organization_id)
        template_vars["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{organization_whitelabel_data['logo']}"
        template_vars["fund_name"] = fund_name
        template_vars["product_name"] = product_name
        template_vars["customer_name"] = client_name
        template_vars["organization_name"] = organization_name
        transactiondate = datetime.strftime(datetime.strptime(transaction_date, '%Y-%m-%d'), '%d %b %Y')
        template_vars["transaction_date"] = transactiondate

        html_body = prepare_pdf_from_html(template_vars, 'pdf_factsheet_email_template.html', report_generation_path, True)
        subject = F"Fund fact sheet of {product_name} - {fund_name} as on {transactiondate}"

        attachements = list()
        attachements.append(file_name)
        schedule_email_activity(current_app.store.db, email_id, '', '', subject, html_body, attachements)
        
        resp = dict()
        resp["msg"] = 'Report shared successfully.'
        return resp

    return send_file(file_name, attachment_filename="factsheet.pdf")