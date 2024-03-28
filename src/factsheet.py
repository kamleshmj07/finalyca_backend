from datetime import date, datetime, timedelta
from datetime import datetime as dt
import json
import logging
import os
from dateutil.relativedelta import relativedelta
from dateutil import parser
import numpy as np
import sqlalchemy
from utils.utils import print_query, shift_date
from time import strptime
import itertools
import pandas as pd
import uuid
from operator import and_
from flask import Blueprint, current_app, jsonify, request, send_file
from fin_models.masters_models import AssetClass,Options, PlanType, HoldingSecurity, Sector
from bizlogic.fund_portfolio_analysis import get_equity_analysis_overview, get_equity_exposure
from fin_models.logics_models import *
from fin_models.servicemanager_models import *
from fin_models.transaction_models import PlanProductMapping, FundStocks, ClosingValues, IndexWeightage
from sebi_lib.models import *
from sqlalchemy import desc, extract,func, or_, text
from sqlalchemy.sql.expression import cast
from werkzeug.exceptions import BadRequest
from fin_models import AMC, BenchmarkIndices, Classification, Fund, FundManager, MFSecurity, Plans, Product, NAV, FactSheet, TRIReturns, UnderlyingHoldings, ContentUpload, ContentUploadType, Content, ContentType
from bizlogic.importer_helper import get_plan_id, get_fundid_byplanid, get_requestdata, get_performancetrend_data, get_commonstock_between_two_plans,\
                                     get_plan_overlap, get_trailing_return_and_riskanalysis,get_fund_underlying_holdings,\
                                     get_scheme_details, get_riskrating, get_fundcomparedata_planwise, get_organization_whitelabel, get_excel_report,\
                                     get_holdings_sector_data, get_portfolio_instrumentrating_data, get_portfolio_instrument_data, get_securedunsecured_data,\
                                     getbetweendate, get_business_day, get_fund_nav, get_latest_factsheet_query, get_fund_portfolio_movement_data_by_date
from bizlogic.common_helper import get_benchmarkdetails, calculate_benchmark_tri_returns, get_plan_meta_info, get_last_transactiondate,\
                                   get_navbydate, get_detailed_fund_holdings
from utils.time_func import last_date_of_month
from src.factsheet_pdf_helper import get_factsheetpdf, get_fundcomparepdf, get_fundoverlappdf
from uuid import uuid4
from src.utils import create_filter_obj, get_user_info
from src.upload_service import save_upload_request
from bizlogic.common_helper import schedule_email_activity
from tasks.sebi_export_website import get_growth_journey_by_sebi_nr, get_flow_journey_by_sebi_nr
from jinja2 import Environment, FileSystemLoader
from sqlalchemy.orm import aliased
from sebi_lib.utils import get_sebi_database_scoped_session
from reports.utils import prepare_pdf_from_html

factsheet_bp = Blueprint("factsheet_bp", __name__)

#Content_Upload/M00
@factsheet_bp.route("/api/v1/get_content_upload")
def get_content_upload():
    type_id = request.args.get("type_id", type=int)
    topcount = request.args.get("top_count", type=int)

    if not type_id:
        raise BadRequest("Type id required.")

    if not topcount:
        raise BadRequest("Top count required.")

    resp = list()

    sql_contentdata = current_app.store.db.query(ContentUpload.Content_Upload_Id, ContentUpload.Content_Upload_Type_Id, ContentUpload.Content_Upload_Name, ContentUpload.AMC_Id, AMC.AMC_Name, ContentUpload.Content_Upload_URL, ContentUploadType.Content_Upload_Type_Name).select_from(ContentUpload).join(ContentUploadType, ContentUploadType.Content_Upload_Type_Id == ContentUpload.Content_Upload_Type_Id).join(AMC, AMC.AMC_Id == ContentUpload.AMC_Id).filter(ContentUpload.Content_Upload_Type_Id == type_id).filter(ContentUpload.Is_Deleted != 1).filter(AMC.Is_Deleted != 1).filter(ContentUploadType.Is_Deleted != 1).order_by(desc(ContentUpload.Content_Upload_Id)).limit(topcount).all()

    if sql_contentdata:
        for sql_contentdt in sql_contentdata:
            data = dict()
            data["AMC_Id"] = sql_contentdt.AMC_Id
            data["AMC_Name"] = sql_contentdt.AMC_Name
            data["Content_Upload_Id"] = sql_contentdt.Content_Upload_Id
            data["Content_Upload_Name"] = sql_contentdt.Content_Upload_Name
            data["Content_Upload_Type_Name"] = sql_contentdt.Content_Upload_Type_Name
            data["Content_Upload_Type_Id"] = sql_contentdt.Content_Upload_Type_Id
            data["Content_Upload_URL"] = sql_contentdt.Content_Upload_URL
            
            resp.append(data)

    return jsonify(resp)


#Dashboard/M00
@factsheet_bp.route("/api/v1/dashboard")
def get_dashboard():
    is_fund_level = request.args.get("is_fund_level", type=int)

    resp = list()
    product_list = list()

    product_list.append(1)
    product_list.append(2)
    product_list.append(4)
    product_list.append(5)
    sql_data = None
    
    for productid in product_list:
        sql_data = current_app.store.db.query(Product.Product_Id, 
                                              Product.Product_Name, 
                                              Product.Product_Code, 
                                              Product.SortNo, 
                                              AMC.AMC_Id, 
                                              AMC.AMC_Name)\
                                                .select_from(Plans)\
                                                .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                                .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                                .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                                                .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                .filter(AMC.Is_Deleted != 1)\
                                                .filter(Product.Product_Id == productid)\
                                                .filter(MFSecurity.Status_Id == 1)
        if is_fund_level:
            sql_data = sql_data.group_by(Product.Product_Id, 
                                        Product.Product_Name, 
                                        Product.Product_Code, 
                                        Product.SortNo, 
                                        AMC.AMC_Id, 
                                        AMC.AMC_Name,
                                        Fund.Fund_Id)\
                            .order_by(Product.Product_Id).all()
        else:
            sql_data = sql_data.group_by(Product.Product_Id, 
                                        Product.Product_Name, 
                                        Product.Product_Code, 
                                        Product.SortNo, 
                                        AMC.AMC_Id, 
                                        AMC.AMC_Name)\
                            .order_by(Product.Product_Id).all()

        data = dict()
        data["product_id"] = sql_data[0].Product_Id
        data["product_name"] = sql_data[0].Product_Code
        data["count"] = len(sql_data)

        resp.append(data)

    return jsonify(resp)


@factsheet_bp.route("/api/v1/fund_compare")
def get_fundcompare():
    plans = request.args.getlist("plan_id", type=str) 
    dates = request.args.getlist("date", type=str)

    resp = list()
    if not plans:
        raise BadRequest("Plan id is required.")

    if not plans:
        raise BadRequest("Date is required.")

    plan_list = plans
    index = 0
    for plan in plan_list:
        data = dict()
        data = get_fundcomparedata_planwise(current_app.store.db, dates[index], plan)
            
        resp.append(data)
        index = index + 1

    return jsonify(resp)

#Content/M00
@factsheet_bp.route("/api/v1/news")
def get_news():
    type_id = request.args.get("type_id", type=int)
    topcount = request.args.get("top_count", type=int)
    is_front_dashboard = request.args.get("is_front_dashboard", type=int)
    amc_id = request.args.get("amc_id", default=None)
    product_id = request.args.get("product_id", default=None)
    headline = request.args.get("headline", default=None)

    if not type_id:
        raise BadRequest("Type id required.")

    if not topcount:
        raise BadRequest("Top count required.")

    if is_front_dashboard == None:
        raise BadRequest("Is front dashboard required.")

    resp = list()

    sql_contentdata = current_app.store.db.query(Content.Content_Header,AMC.AMC_Id, AMC.AMC_Name, Content.Content_DateTime, Content.Content_Id, Content.Content_Source, Content.Is_Front_Dashboard, ContentType.Content_Type_Name, Content.Content_Detail, Content.Images_URL, Content.Content_Name, Product.Product_Name).select_from(Content).join(ContentType, ContentType.Content_Type_Id == Content.Content_Type_Id).join(AMC, AMC.AMC_Id == Content.AMC_Id, isouter=True).join(Product, Product.Product_Id == Content.Product_Id).filter(Content.Content_Type_Id == type_id).filter(Content.Is_Deleted != 1).filter(ContentType.Is_Deleted != 1)

    if amc_id:
        sql_contentdata = sql_contentdata.filter(AMC.AMC_Id == amc_id)

    if product_id:
        sql_contentdata = sql_contentdata.filter(Product.Product_Id == product_id)

    if is_front_dashboard != None and is_front_dashboard != 0:
        sql_contentdata = sql_contentdata.filter(Content.Is_Front_Dashboard == is_front_dashboard)

    if headline != None and headline != '':
        sql_contentdata = sql_contentdata.filter(Content.Content_Header.like('%'+ headline + '%'))
    
    sql_contentdata = sql_contentdata.order_by(desc(Content.Content_DateTime)).limit(topcount).all()

    if sql_contentdata:
        for sql_contentdt in sql_contentdata:
            data = dict()
            data["AMC_Id"] = sql_contentdt.AMC_Id
            data["AMC_Name"] = sql_contentdt.AMC_Name
            data["Content_Header"] = sql_contentdt.Content_Header
            data["Content_DateTime"] = sql_contentdt.Content_DateTime
            data["Content_Id"] = sql_contentdt.Content_Id
            data["Content_Source"] = sql_contentdt.Content_Source
            data["Is_Front_Dashboard"] = sql_contentdt.Is_Front_Dashboard
            data["Content_Type_Name"] = sql_contentdt.Content_Type_Name
            data["Content_Detail"] = sql_contentdt.Content_Detail
            data["Images_URL"] = sql_contentdt.Images_URL
            data["Content_Name"] = sql_contentdt.Content_Name
            data["product_name"] = sql_contentdt.Product_Name
            
            resp.append(data)

    return jsonify(resp)

# Classification/M00
@factsheet_bp.route("/api/v1/classification")
def get_classification():
    resp = list()
    sql_data = current_app.store.db.query(Classification.Classification_Id, Classification.Classification_Name, Classification.Classification_Code, Classification.AssetClass_Id, AssetClass.AssetClass_Name).select_from(Classification).join(AssetClass, AssetClass.AssetClass_Id == Classification.AssetClass_Id).filter(AssetClass.Is_Deleted != 1).filter(Classification.Is_Deleted != 1).all()

    if sql_data:
        for data in sql_data:
            res = dict()
            res["Classification_Id"] = data.Classification_Id
            res["Classification_Name"] = data.Classification_Name
            res["Classification_Code"] = data.Classification_Code
            res["AssetClass_Id"] = data.AssetClass_Id
            res["AssetClass_Name"] = data.AssetClass_Name
            resp.append(res)

    return jsonify(resp)

# Factsheet/M00 - partial
@factsheet_bp.route("/api/v1/performance_trend")
def get_performance_trend():
    transaction_date = request.args.get("date", type=str)

    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")
    res_factsheet = get_performancetrend_data(current_app.store.db, plan_id, None)    

    return jsonify(res_factsheet)


@factsheet_bp.route("/api/v1/fund_overlap_v2")
def get_fundoverlap_v2():
    plans = request.args.getlist("plan_id", type=int) # list of plans comma separated
    
    if not len(plans):
        raise BadRequest("Plans required.")

    resp = dict()
    resp["overlap"] = get_plan_overlap(current_app.store.db, plans)
    resp["details"] = get_plan_meta_info(current_app.store.db, plans)
    trailing_return, riskanalysis = get_trailing_return_and_riskanalysis(current_app.store.db, plans)
    resp["trailing_returns"] = trailing_return
    resp["risk_analysis"] = riskanalysis

    return jsonify(resp)


@factsheet_bp.route("/api/v1/fund_overlap")
def get_fundoverlap():
    plans = request.args.getlist("plan_id", type=str) # list of plans comma separated

    if not len(plans):
        raise BadRequest("Plans required.")

    resp = dict()
    all_plans_details = list()
    allisin_data = dict()
    overlapisin_data = dict()
    sql_fund = None
    allisin_details = dict()
    plan_names = dict()
    plan_id_list = list(set(plans))
    db_session = current_app.store.db

    for plan_id in plan_id_list:
        plan_name = db_session.query(Plans.Plan_Name)\
                              .filter(Plans.Plan_Id == plan_id,
                                      Plans.Is_Deleted != 1).scalar()

        plan_names[int(plan_id)] = plan_name
        plans_details = dict()
        plans_details["plan_id"] = int(plan_id)

        fund_id = get_fundid_byplanid(db_session, plan_id)
        transaction_date = None
        portfolio_date = None

        # get last transactiondate and portfolio_Date
        factsheet_query = current_app.store.db.query(FactSheet)\
                                              .filter(FactSheet.Plan_Id == plan_id,
                                                      FactSheet.Is_Deleted != 1)\
                                              .order_by(desc(FactSheet.TransactionDate)).first()

        if factsheet_query:
            transaction_date = factsheet_query.TransactionDate
            portfolio_date = factsheet_query.Portfolio_Date

        allholding_data = list()
        if transaction_date and portfolio_date:
            # get holdings
            lst_holdings = get_fund_underlying_holdings(db_session, fund_id=fund_id, portfolio_date=portfolio_date, limit=None)

            if lst_holdings:
                for h in lst_holdings:
                    isin = h.get('ISIN_Code')
                    security_name = h.get('Company_Security_Name')
                    security_type = "Equity" if h.get('HoldingSecurity_Type') == "LISTED EQUITY" else h.get('Asset_Class')

                    holding_data = dict()
                    holding_data["security_name"] = security_name
                    holding_data["security_isin"] = isin
                    holding_data["security_percentage"] = h.get('Percentage_to_AUM')
                    holding_data["security_sector"] = h.get('Sector_Name')
                    allholding_data.append(holding_data)

                    if not isin in allisin_details.keys():
                        allisin_details[isin] = security_name

                    if isin:
                        if isin in allisin_data:
                            allisin_data[isin] = int(allisin_data[isin]) + 1
                        else:
                            allisin_data[isin] = 1

                    if security_type == "Equity" or security_type == "Partly Paid Equity":
                        if isin in overlapisin_data:
                            overlapisin_data[isin] = int(overlapisin_data[isin]) + 1
                        else:
                            overlapisin_data[isin] = 1

        plans_details["holdings"] = allholding_data
        all_plans_details.append(plans_details)

    #overlap
    common_totalstocks = 0
    percentportfoliooverlap_total_percent = 0
    for isin, count in allisin_data.items():
            if count == len(plan_id_list):
                common_totalstocks = common_totalstocks + 1

    final_data_list = list()
    final_data = dict()
    for plan in plan_id_list:
        overlap_data = dict()
        total_percent_to_aum = 0
        common_percent_to_aum = 0
        amc_logo = ""
        product_code = ""
        product_name = ""

        sql_fund1 = db_session.query(AMC.AMC_Logo,
                                     Product.Product_Code,
                                     Product.Product_Name)\
                                .select_from(AMC)\
                                .join(MFSecurity, AMC.AMC_Id == MFSecurity.AMC_Id)\
                                .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                .filter(Plans.Plan_Id == plan,
                                        MFSecurity.Is_Deleted != 1,
                                        AMC.Is_Deleted != 1,
                                        Plans.Is_Deleted != 1,
                                        PlanProductMapping.Is_Deleted != 1).first()

        if sql_fund1:
            amc_logo = sql_fund1.AMC_Logo
            product_code = sql_fund1.Product_Code
            product_name = sql_fund1.Product_Name

        for plans_detail in all_plans_details:
            if int(plan) == int(plans_detail["plan_id"]):
                holding_data1 = plans_detail["holdings"]
                overlap_data["plan_id"] = plan
                overlap_data["uncommon_totalstocks"] = len(holding_data1) - common_totalstocks
                overlap_data["totalstocks"] =  len(holding_data1)

                for holding_dt in holding_data1:
                    total_percent_to_aum = total_percent_to_aum + holding_dt["security_percentage"]

                for isin, count in allisin_data.items():
                    if count == len(plan_id_list):
                        for holding_dt in holding_data1:
                            if holding_dt["security_isin"] == isin:
                                percentportfoliooverlap_total_percent = percentportfoliooverlap_total_percent + holding_dt["security_percentage"]
                                common_percent_to_aum = common_percent_to_aum + holding_dt["security_percentage"]

        overlap_data["percent_to_aum"] = total_percent_to_aum
        overlap_data["common_percent_to_aum"] = common_percent_to_aum
        overlap_data["common_percent_to_aum_ucs"] = total_percent_to_aum - common_percent_to_aum
        overlap_data["amc_logo"] = amc_logo
        overlap_data["product_code"] = product_code
        overlap_data["product_name"] = product_name

        final_data_list.append(overlap_data)
    
    final_data["percentportfoliooverlap"] = percentportfoliooverlap_total_percent / len(plan_id_list)
    final_data["planwise_data"] = final_data_list
    final_data["totalstocks_cs"] = common_totalstocks
    resp["overlap"] = final_data

    # get_commonstock_betweenplans(plan_list)
    commonstock_between_two_plans_list = list()
    for a, b in itertools.combinations(plan_id_list, 2):
        planlst = list()
        planlst.append(a)
        planlst.append(b)

        commonstock_between_two_plans = get_commonstock_between_two_plans(planlst, all_plans_details)
        commonstock_between_two_plans_list.append(commonstock_between_two_plans)
    
    plana_dict = dict()
    for plan_a in plan_id_list:
        planb_dict = dict()
        
        for plan_b in plan_id_list:
            if int(plan_a) != int(plan_b):                
                planlst = list()
                planlst.append(plan_a)
                planlst.append(plan_b)
                
                if not str(plan_b) in planb_dict.keys():
                    overlapdata_ab = get_commonstock_between_two_plans(planlst, all_plans_details)
                    planb_dict[str(plan_b)] = {
                        "common_portfolio": overlapdata_ab["percentportfoliooverlap"], 
                        "common_stocks": overlapdata_ab["common_totalstocks"]
                        }

                    common_totalstocks
        
        if not str(plan_a) in plana_dict.keys():
            plana_dict[str(plan_a)] = planb_dict


    resp["commonstock_between_two_plans"] = plana_dict
    resp["plans"] = plan_names
    return jsonify(resp)


@factsheet_bp.route("/api/v1/equity_analysis")
def get_equity_analysis():
    product_id = request.args.get("product_id", type=int)
    classification_id = request.args.get("classification_id", type=int)
    sector_id = request.args.get("sector_id", type=int)
    market_cap = request.args.get("market_cap", type=str)
    is_front_dashboard = request.args.get("is_front_dashboard", type=int)
    
    # resp = get_equity_analysis_overview_old(current_app.store.db, product_id, classification_id, sector_id, market_cap)

    equities = get_equity_analysis_overview(current_app.store.db, product_id, classification_id, sector_id, market_cap)

    if is_front_dashboard:
        resp_dic = dict()
        column_types = ['increaseexposure', 'decreaseexposure', 'newstockforfund', 'exitstockforfund']
        market_caps = ['Large Cap', 'Mid Cap', 'Small Cap']
        products = ['mf', 'ulip', 'pms', 'aif']

        df_equities = pd.DataFrame(equities)
        # df_equities.to_csv('equities.csv')
        for column_type in column_types:
            column_type_dic = dict()

            for market_cap in market_caps:
                if not df_equities.empty:
                    mcaps_dict = dict()

                    for product in products:
                        res = df_equities.loc[df_equities['marketcap'] == market_cap]
                        df_mc = res.sort_values(by=[F'{column_type}_{product}'], ascending=False).head(5)
                        mcaps_dict[product] = df_mc.to_dict(orient="records")
                    
                    res = df_equities.loc[df_equities['marketcap'] == market_cap]
                    df_mc = res.sort_values(by=[F'{column_type}_total'], ascending=False).head(5)
                    mcaps_dict['all'] = df_mc.to_dict(orient="records")

                    column_type_dic[market_cap] = mcaps_dict
            
            #For all market cap
            all_dict = dict()
            for product in products:
                df_mc = df_equities.sort_values(by=[F'{column_type}_{product}'], ascending=False).head(5)
                all_dict[product] = df_mc.to_dict(orient="records")
            
            df_mc = df_equities.sort_values(by=[F'{column_type}_total'], ascending=False).head(5)
            all_dict['all'] = df_mc.to_dict(orient="records")
            
            column_type_dic['all'] = all_dict
            
            resp_dic[column_type] = column_type_dic
        
        return jsonify(resp_dic)              

    else:
        resp = list()
        for obj in equities:
            # resp.append(obj._mapping)
            resp.append(obj._asdict())
    
    return jsonify(resp)
    
@factsheet_bp.route("/api/v1/uploadtemplates")
def uploadtemplates():
    sql_templates = current_app.store.db.query(UploadTemplates).filter(UploadTemplates.Is_Deleted != 1).filter(or_(UploadTemplates.Status == 1, UploadTemplates.Enabled_Python == 1)).all()

    resp = list()
    if sql_templates:
        for sql_template in sql_templates:
            data = dict()
            data["uploadtemplates_id"] = sql_template.UploadTemplates_Id
            data["template_description"] = sql_template.Template_Description
            data["parameters"] = sql_template.Parameters
            data["template_name"] = sql_template.UploadTemplates_Name

            resp.append(data)

    return jsonify(resp)

@factsheet_bp.route("/api/v1/uploadrequest")
def all_uploadtemplates():
    sql_requests = current_app.store.db.query(UploadRequest.UploadRequest_Id, UploadTemplates.UploadTemplates_Name, UploadRequest.File_Name, UploadRequest.File_Url, UploadRequest.Request_Time, UploadRequest.Pick_Time, UploadRequest.Completion_Time, UploadRequest.Status, UploadRequest.Status_Message).select_from(UploadRequest).join(UploadTemplates, UploadTemplates.UploadTemplates_Id == UploadRequest.UploadTemplates_Id).filter(UploadTemplates.Is_Deleted != 1).order_by(desc(UploadRequest.UploadRequest_Id)).all()

    resp = list()
    if sql_requests:
        for sql_request in sql_requests:
            data = dict()
            data["uploadrequest_id"] = sql_request.UploadRequest_Id
            data["uploadtemplates_name"] = sql_request.UploadTemplates_Name
            data["file_name"] = sql_request.File_Name
            data["request_file"] = sql_request.File_Url + sql_request.File_Name
            data["request_time"] =str(sql_request.Request_Time)
            data["pick_time"] = str(sql_request.Pick_Time)
            data["completion_time"] = str(sql_request.Completion_Time)
            data["status"] = sql_request.Status
            data["status_message"] = sql_request.Status_Message

            response_filename = ""
            split_filename = sql_request.File_Name.rsplit('.',1)
            length = len(split_filename)

            for strng in split_filename: 
                if strng != split_filename[length - 1]:
                    response_filename = response_filename + strng
                
            response_filename = response_filename + "_R." + split_filename[length - 1]
            data["response_file"] = sql_request.File_Url + response_filename
            
            resp.append(data)

    return jsonify(resp)
    

@factsheet_bp.route("/api/v1/uploadrequest", methods = ["POST"])
def uploadrequest():

    resp = "File uploaded successfully"
    if request.method == 'POST':
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        user_id = user_info["id"]
        template_id = request.args.get("uploadtemplates_id", type=int)
        file_obj = request.files.get("upload")        
        if file_obj.mimetype == "text/csv":
            file_obj.filename = str(uuid4()) + ".csv"
        else:
            resp = "File format not allowed. only csv files allowed."

        id = save_upload_request(current_app.store.db, template_id, file_obj, user_id)

    return jsonify({"msg": resp})





@factsheet_bp.route("/api/v1/risk_rating", methods=['GET'])
def get_risk_rating():    
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
    
        resp = get_riskrating(current_app.store.db, plan_id, transaction_date)

    return jsonify(resp)


@factsheet_bp.route("/api/v1/scheme_details", methods=['GET'])
def scheme_details():    
    transaction_date = request.args.get("date", type=str, default=None)
    isin = request.args.get("isin", type=str, default=None)
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    db_session = current_app.store.db
    if not plan_id:
        plan_id = get_plan_id(db_session, requestdata["plan_code"], requestdata["plan_name"],
                              requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"], isin=isin)

    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] and not isin:
        raise BadRequest("Parameters Required: <plan_id> or <scheme_id> or <isin>")

    if not transaction_date:
        transaction_date = get_last_transactiondate(db_session, plan_id)

    res = get_scheme_details(db_session, plan_id, transaction_date)
    res['plan_id'] = plan_id

    return jsonify(res)


@factsheet_bp.route("/api/v1/favoritestock_funds", methods=['GET'])
def get_fundstock_details():
    isin_code = request.args.get("isin_code", type=str)
    product_id = request.args.get("product_id", type=int)    

    if not isin_code:
        raise BadRequest("isin code required.")

    favoritestockfunds_list = get_equity_exposure(current_app.store.db, isin_code, product_id)

    return jsonify(favoritestockfunds_list)


@factsheet_bp.route('/api/v1/security_list', methods=['GET'])
def security_list():
    holdingsecurity_type = request.args.get('holding_security_type', type=str, default=None)
    holdingsecurity_name = request.args.get('holding_security_name', type=str, default=None)

    resp = list()
    if not holdingsecurity_type:
        raise BadRequest('Holding Security Type is required.')

    if not holdingsecurity_name:
        raise BadRequest('Holding Security Name is required.')

    sql_security_query = current_app.store.db.query(HoldingSecurity.HoldingSecurity_Id,
                                                    HoldingSecurity.HoldingSecurity_Name,
                                                    HoldingSecurity.Instrument_Type,
                                                    HoldingSecurity.ISIN_Code,
                                                    HoldingSecurity.BSE_Code,
                                                    HoldingSecurity.NSE_Symbol,
                                                    HoldingSecurity.MarketCap,
                                                    Sector.Sector_Name,
                                                    Sector.Sector_Id)\
                                             .join(Sector, Sector.Sector_Id == HoldingSecurity.Sector_Id)\
                                             .filter(HoldingSecurity.Is_Deleted != 1,
                                                     HoldingSecurity.active != 0,
                                                     Sector.Is_Deleted != 1,
                                                     HoldingSecurity.HoldingSecurity_Name.like(holdingsecurity_name + '%'))

    if holdingsecurity_type.upper() == 'EQUITY':
        sql_security_query = sql_security_query.filter(HoldingSecurity.Instrument_Type == 'Equity',
                                                       HoldingSecurity.ISIN_Code.like('INE%'))
    else:
        sql_security_query = sql_security_query.filter(HoldingSecurity.Instrument_Type != 'Equity',
                                                       HoldingSecurity.ISIN_Code.not_like('INE%'))
    
    sql_security = sql_security_query.all()

    if sql_security:
        for sql_sec in sql_security:            
            exclude_security = False
            if len(sql_sec.HoldingSecurity_Name) > 10:
                dates = str(sql_sec.HoldingSecurity_Name).replace("/","-").strip()[-10:]
            try:
                # TODO: find a way to check if security has expired or not. 
                dates = datetime.strptime(dates, '%d-%m-%Y').date()
                today = date.today()
                if dates < today:
                    exclude_security = True
            except:
                pass
               
            if exclude_security == False:
                data = dict()
                data["security_id"] = sql_sec.HoldingSecurity_Id
                data["security_name"] = sql_sec.HoldingSecurity_Name
                data["security_type"] = sql_sec.Instrument_Type
                data["security_cap"] = sql_sec.MarketCap
                data["security_isin"] = sql_sec.ISIN_Code            
                data["security_sector_name"] =  sql_sec.Sector_Name   
                data["security_sector_id"] = sql_sec.Sector_Id
                
                resp.append(data)
    return jsonify(resp)

@factsheet_bp.route('/api/v1/fund_mining_filters', methods=['GET'])
def fund_mining_filters():
    resp = list()
    
    sql_subquery = current_app.store.db.query(FactSheet.Plan_Id, func.max(FactSheet.TransactionDate).label('max_transactiondate')).filter(FactSheet.Is_Deleted != 1).group_by(FactSheet.Plan_Id).subquery()

    sql_filter = current_app.store.db.query(
    func.coalesce(func.min(FactSheet.ExpenseRatio),0).label('Min_ExpenseRatio'), func.coalesce(func.max(FactSheet.ExpenseRatio),0).label('Min_ExpenseRatio'),

    func.coalesce(func.min(FactSheet.NetAssets_Rs_Cr),0).label('Min_NetAssets_Rs_Cr'), func.coalesce(func.max(FactSheet.NetAssets_Rs_Cr),0).label('Max_NetAssets_Rs_Cr'),

    func.coalesce(func.min(FactSheet.SCHEME_RETURNS_1MONTH),0).label('Min_SCHEME_RETURNS_1MONTH'), func.coalesce(func.max(FactSheet.SCHEME_RETURNS_1MONTH),0).label('max_SCHEME_RETURNS_1MONTH'),

    func.coalesce(func.min(FactSheet.SCHEME_RETURNS_3MONTH),0).label('Min_SCHEME_RETURNS_3MONTH'), func.coalesce(func.max(FactSheet.SCHEME_RETURNS_3MONTH),0).label('max_SCHEME_RETURNS_3MONTH'),

    func.coalesce(func.min(FactSheet.SCHEME_RETURNS_6MONTH),0).label('Min_SCHEME_RETURNS_6MONTH'), func.coalesce(func.max(FactSheet.SCHEME_RETURNS_6MONTH),0).label('max_SCHEME_RETURNS_6MONTH'),

    func.coalesce(func.min(FactSheet.SCHEME_RETURNS_1YEAR),0).label('Min_SCHEME_RETURNS_1YEAR'), func.coalesce(func.max(FactSheet.SCHEME_RETURNS_1YEAR),0).label('max_SCHEME_RETURNS_1YEAR'),

    func.coalesce(func.min(FactSheet.SCHEME_RETURNS_3YEAR),0).label('Min_SCHEME_RETURNS_3YEAR'), func.coalesce(func.max(FactSheet.SCHEME_RETURNS_3YEAR),0).label('max_SCHEME_RETURNS_3YEAR'),

    func.coalesce(func.min(FactSheet.SCHEME_RETURNS_5YEAR),0).label('Min_SCHEME_RETURNS_5YEAR'), func.coalesce(func.max(FactSheet.SCHEME_RETURNS_5YEAR),0).label('max_SCHEME_RETURNS_5YEAR'),

    func.coalesce(func.min(FactSheet.SCHEME_RETURNS_since_inception),0).label('Min_SCHEME_RETURNS_since_inception'), func.coalesce(func.max(FactSheet.SCHEME_RETURNS_since_inception),0).label('max_SCHEME_RETURNS_since_inception'),

    func.coalesce(func.min(FactSheet.AvgMaturity_Yrs),0).label('Min_AvgMaturity_Yrs'), func.coalesce(func.max(FactSheet.AvgMaturity_Yrs),0).label('Max_AvgMaturity_Yrs'),

    func.coalesce(func.min(FactSheet.ModifiedDuration_yrs),0).label('Min_ModifiedDuration_yrs'), func.coalesce(func.max(FactSheet.ModifiedDuration_yrs),0).label('Max_ModifiedDuration_yrs'),

    func.coalesce(func.min(FactSheet.StandardDeviation),0).label('Min_StandardDeviation'), func.coalesce(func.max(FactSheet.StandardDeviation),0).label('Max_StandardDeviation'),

    func.coalesce(func.min(FactSheet.SharpeRatio),0).label('Min_SharpeRatio'), func.coalesce(func.max(FactSheet.SharpeRatio),0).label('Max_SharpeRatio'),

    func.coalesce(func.min(FactSheet.Beta),0).label('Min_Beta'), func.coalesce(func.max(FactSheet.Beta),0).label('Max_Beta'),

    func.coalesce(func.min(FactSheet.R_Squared),0).label('Min_R_Squared'), func.coalesce(func.max(FactSheet.R_Squared),0).label('Max_R_Squared'),

    func.coalesce(func.min(FactSheet.Alpha),0).label('Min_Alpha'), func.coalesce(func.max(FactSheet.Alpha),0).label('Max_Alpha'),

    func.coalesce(func.min(FactSheet.Mean),0).label('Min_Mean'), func.coalesce(func.max(FactSheet.Mean),0).label('Max_Mean'),

    func.coalesce(func.min(FactSheet.Sortino),0).label('Min_Sortino'), func.coalesce(func.max(FactSheet.Sortino),0).label('Max_Sortino'),

    func.coalesce(func.min(FactSheet.PortfolioP_ERatio),0).label('Min_PortfolioP_ERatio'), func.coalesce(func.max(FactSheet.PortfolioP_ERatio),0).label('Max_PortfolioP_ERatio'),

    func.coalesce(func.min(FactSheet.TotalStocks),0).label('Min_TotalStocks'), func.coalesce(func.max(FactSheet.TotalStocks),0).label('Max_TotalStocks'),

    func.min(MFSecurity.MF_Security_OpenDate).label('Min_Age_yrs'),
    func.max(MFSecurity.MF_Security_OpenDate).label('Max_Age_yrs'),

    func.coalesce(func.min(FactSheet.SOV),0).label('Min_SOV'), func.coalesce(func.max(FactSheet.SOV),0).label('Max_SOV'),

    func.coalesce(func.min(FactSheet.AAA),0).label('Min_AAA'), func.coalesce(func.max(FactSheet.AAA),0).label('Max_AAA'),

    func.coalesce(func.min(FactSheet.A1_Plus),0).label('Min_A1_Plus'), func.coalesce(func.max(FactSheet.A1_Plus),0).label('Max_A1_Plus'),

    func.coalesce(func.min(FactSheet.AA),0).label('Min_AA'), func.coalesce(func.max(FactSheet.AA),0).label('Max_AA'),

    func.coalesce(func.min(FactSheet.A_and_Below),0).label('Min_A_and_Below'), func.coalesce(func.max(FactSheet.A_and_Below),0).label('Max_A_and_Below'),

    func.coalesce(func.min(FactSheet.Treynor_Ratio),0).label('Min_Treynor_Ratio'), func.coalesce(func.max(FactSheet.Treynor_Ratio),0).label('Max_Treynor_Ratio'),

    ).select_from(FactSheet).join(Plans, Plans.Plan_Id == FactSheet.Plan_Id).join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id).join(sql_subquery, and_(sql_subquery.c.Plan_Id == Plans.Plan_Id, sql_subquery.c.max_transactiondate == FactSheet.TransactionDate)).filter(FactSheet.Is_Deleted != 1).filter(MFSecurity.Status_Id== 1).filter(Plans.Is_Deleted != 1).all()

    if sql_filter:
        resp.append(create_filter_obj("ExpenseRatio", sql_filter[0][0], sql_filter[0][1], "%", '0.05'))
        resp.append(create_filter_obj("NetAssets", sql_filter[0][2], sql_filter[0][3], "Cr.", '500'))

        min_age = relativedelta(date.today(), sql_filter[0][41]).years
        max_age = relativedelta(date.today(), sql_filter[0][40]).years
        resp.append(create_filter_obj("Age", min_age, max_age, "Yrs", 5))

        resp.append(create_filter_obj("Returns_1M", sql_filter[0][4], sql_filter[0][5], "%", '0.5'))
        resp.append(create_filter_obj("Returns_3M", sql_filter[0][6], sql_filter[0][7], "%", '0.5'))
        resp.append(create_filter_obj("Returns_6M", sql_filter[0][8], sql_filter[0][9], "%", '0.5'))
        resp.append(create_filter_obj("Returns_1Y", sql_filter[0][10], sql_filter[0][11], "%", '0.5'))
        resp.append(create_filter_obj("Returns_3Y", sql_filter[0][12], sql_filter[0][13], "%", '0.5'))
        resp.append(create_filter_obj("Returns_5Y", sql_filter[0][14], sql_filter[0][15], "%", '0.5'))
        resp.append(create_filter_obj("Returns_SI", sql_filter[0][16], sql_filter[0][17], "%", '0.5'))
        resp.append(create_filter_obj("AvgMaturity", sql_filter[0][18], sql_filter[0][19], "Yrs", '5'))
        resp.append(create_filter_obj("ModifiedDuration", sql_filter[0][20], sql_filter[0][21], "Yrs", '5'))
        resp.append(create_filter_obj("StandardDeviation", sql_filter[0][22], sql_filter[0][23], "%", '0.5'))
        resp.append(create_filter_obj("SharpeRatio", sql_filter[0][24], sql_filter[0][25], "%", '0.5'))
        resp.append(create_filter_obj("Beta", sql_filter[0][26], sql_filter[0][27], "%", '0.5'))
        resp.append(create_filter_obj("RSquare", sql_filter[0][28], sql_filter[0][29], "%", '0.5'))
        resp.append(create_filter_obj("Mean", sql_filter[0][32], sql_filter[0][33], "%", '0.5'))
        resp.append(create_filter_obj("Sortino", sql_filter[0][34], sql_filter[0][35], "%", '0.5'))
        resp.append(create_filter_obj("Portfolio_PE_Ratio", sql_filter[0][36], sql_filter[0][37], "%", '0.5'))
        resp.append(create_filter_obj("Alpha", sql_filter[0][30], sql_filter[0][31], "%", '0.5'))
        resp.append(create_filter_obj("SOV", sql_filter[0][42], sql_filter[0][43], "%", '0.5'))
        resp.append(create_filter_obj("AAA", sql_filter[0][44], sql_filter[0][45], "%", '0.5'))
        resp.append(create_filter_obj("A1_Plus", sql_filter[0][46], sql_filter[0][47], "%", '0.5'))
        resp.append(create_filter_obj("AA", sql_filter[0][48], sql_filter[0][49], "%", '0.5'))
        resp.append(create_filter_obj("A_and_Below", sql_filter[0][50], sql_filter[0][51], "%", '0.5'))
        resp.append(create_filter_obj("TotalStocks", sql_filter[0][38], sql_filter[0][39], "", '10'))
        resp.append(create_filter_obj("TreynorRatio", sql_filter[0][52], sql_filter[0][53], "%", '0.5'))

        # replace null to 0 except TotalStocks_Unit
        # resp = {k: '0.00' if not v and k != 'TotalStocks_Unit' else v for k, v in resp.items() }

    return jsonify(resp)

def fund_mining_query(product_list, assetclass_list, classification_list, amc_list):
    max_pms_factsheet_date = current_app.store.db.query(func.max(FactSheet.TransactionDate).label('TransactionDate'))\
                      .join(Plans, (Plans.Plan_Id == FactSheet.Plan_Id) & (Plans.PlanType_Id == 1) & (Plans.Is_Deleted != 1))\
                      .join(PlanProductMapping, (Plans.Plan_Id == PlanProductMapping.Plan_Id) & (PlanProductMapping.Is_Deleted != 1) & (PlanProductMapping.Product_Id == 4))\
                      .join(MFSecurity, (Plans.MF_Security_Id == MFSecurity.MF_Security_Id) & (MFSecurity.Status_Id == 1) & (MFSecurity.Is_Deleted != 1))\
                      .join(Fund, and_(Fund.Fund_Id == MFSecurity.Fund_Id, Fund.Is_Deleted != 1))\
                      .join(Options, and_(Options.Option_Id == Plans.Option_Id, Options.Option_Name.like('%G%')))\
                      .filter(FactSheet.TransactionDate.isnot(None))\
                      .filter(FactSheet.Is_Deleted != 1).scalar()
    
    sql_subquery = current_app.store.db.query(FactSheet.Plan_Id, func.max(FactSheet.TransactionDate).label('max_transactiondate')).filter(FactSheet.Is_Deleted != 1, FactSheet.TransactionDate <= max_pms_factsheet_date).group_by(FactSheet.Plan_Id).subquery()

    sql_fundmanager_subquery = current_app.store.db.query(FundManager.Fund_Id, FundManager.FundManager_Name).filter(FundManager.Is_Deleted != 1).subquery()

    sql_query = current_app.store.db.query(FactSheet.FactSheet_Id, 
                                            FactSheet.Plan_Id, 
                                            Plans.Plan_Name, 
                                            Product.Product_Id, 
                                            MFSecurity.AssetClass_Id, 
                                            MFSecurity.Classification_Id, 
                                            MFSecurity.AMC_Id, 
                                            AMC.AMC_Logo, 
                                            AMC.AMC_Name, 
                                            Product.Product_Code, 
                                            Product.Product_Name, 
                                            MFSecurity.MF_Security_OpenDate, 
                                            FactSheet.TotalStocks, 
                                            FactSheet.PortfolioP_ERatio, 
                                            FactSheet.ModifiedDuration_yrs, 
                                            FactSheet.StandardDeviation, 
                                            FactSheet.SharpeRatio, 
                                            FactSheet.Beta, 
                                            FactSheet.R_Squared, 
                                            FactSheet.Alpha, 
                                            FactSheet.Mean, 
                                            FactSheet.Sortino, 
                                            FactSheet.SCHEME_RETURNS_1MONTH, 
                                            FactSheet.SCHEME_RETURNS_3MONTH, 
                                            FactSheet.SCHEME_RETURNS_6MONTH, 
                                            FactSheet.SCHEME_RETURNS_1YEAR, 
                                            FactSheet.SCHEME_RETURNS_2YEAR, 
                                            FactSheet.SCHEME_RETURNS_3YEAR, 
                                            FactSheet.SCHEME_RETURNS_5YEAR, 
                                            FactSheet.SCHEME_RETURNS_10YEAR,
                                            FactSheet.SCHEME_RETURNS_since_inception, 
                                            FactSheet.ExpenseRatio, 
                                            FactSheet.SOV, 
                                            FactSheet.AAA, 
                                            FactSheet.AA, 
                                            FactSheet.A1_Plus, 
                                            FactSheet.A_and_Below, 
                                            FactSheet.NetAssets_Rs_Cr, 
                                            FactSheet.AvgMaturity_Yrs, 
                                            Classification.Classification_Name, 
                                            sql_fundmanager_subquery.c.FundManager_Name, 
                                            FactSheet.TransactionDate,
                                            func.datediff(text('Day'), MFSecurity.MF_Security_OpenDate, FactSheet.TransactionDate).label('age_in_days'))\
                                                .select_from(FactSheet)\
                                                .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                                                .join(Options, and_(Options.Option_Id == Plans.Option_Id, Options.Option_Name.like("%G%")))\
                                                .join(PlanProductMapping, and_(PlanProductMapping.Plan_Id == Plans.Plan_Id, PlanProductMapping.Is_Deleted != 1))\
                                                .join(Product, and_(Product.Product_Id == PlanProductMapping.Product_Id, FactSheet.SourceFlag == Product.Product_Code))\
                                                .join(MFSecurity, and_(MFSecurity.MF_Security_Id == Plans.MF_Security_Id, MFSecurity.Status_Id == 1))\
                                                .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id)\
                                                .join(AMC, and_(AMC.AMC_Id == MFSecurity.AMC_Id, AMC.Is_Deleted != 1))\
                                                .join(sql_subquery, and_(sql_subquery.c.Plan_Id == Plans.Plan_Id, sql_subquery.c.max_transactiondate == FactSheet.TransactionDate))\
                                                .join(sql_fundmanager_subquery, sql_fundmanager_subquery.c.Fund_Id == MFSecurity.Fund_Id, isouter=True)\
                                                .filter(FactSheet.Is_Deleted != 1)

    if product_list:
        sql_query = sql_query.filter(Product.Product_Id.in_(product_list))
    
    if assetclass_list:
        sql_query = sql_query.filter(MFSecurity.AssetClass_Id.in_(assetclass_list))
    
    if classification_list:
        sql_query = sql_query.filter(MFSecurity.Classification_Id.in_(classification_list))

    if amc_list:
        sql_query = sql_query.filter(MFSecurity.AMC_Id.in_(amc_list))
    
    return sql_query

@factsheet_bp.route('/api/v1/fund_mining', methods=['POST'])
def fund_mining_data():
    f = request.json
    req = f["RequestObject"]

    if not req:
        raise BadRequest('Parameters required.')

    resp = list()

    product_list = req["Product"]
    assetclass_list = req["AssetClass"]
    classification_list = req["Classification"]
    amc_list = req["AMC"]
    queryfilters_list = req["QueryFilters"]
    sov_val = req["selection_val"]

    sql_query = fund_mining_query(product_list, assetclass_list, classification_list, amc_list)

    for queryfilters in queryfilters_list:
        if queryfilters["Filters"] == 'ExpenseRatio':
            sql_query = sql_query.filter(func.coalesce(FactSheet.ExpenseRatio,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.ExpenseRatio,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'NetAssets':
            sql_query = sql_query.filter(func.coalesce(FactSheet.NetAssets_Rs_Cr,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.NetAssets_Rs_Cr,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'Returns_1M':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SCHEME_RETURNS_1MONTH,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SCHEME_RETURNS_1MONTH,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'Returns_3M':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SCHEME_RETURNS_3MONTH,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SCHEME_RETURNS_3MONTH,0) <= queryfilters["To_Val"])

        elif queryfilters["Filters"] == 'Returns_6M':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SCHEME_RETURNS_6MONTH,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SCHEME_RETURNS_6MONTH,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'Returns_1Y':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SCHEME_RETURNS_1YEAR,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SCHEME_RETURNS_1YEAR,0) <= queryfilters["To_Val"])

        elif queryfilters["Filters"] == 'Returns_3Y':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SCHEME_RETURNS_3YEAR,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SCHEME_RETURNS_3YEAR,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'Returns_5Y':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SCHEME_RETURNS_5YEAR,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SCHEME_RETURNS_5YEAR,0) <= queryfilters["To_Val"])

        elif queryfilters["Filters"] == 'Returns_SI':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SCHEME_RETURNS_since_inception,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SCHEME_RETURNS_since_inception,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'AvgMaturity':
            sql_query = sql_query.filter(func.coalesce(FactSheet.AvgMaturity_Yrs,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.AvgMaturity_Yrs,0) <= queryfilters["To_Val"])

        elif queryfilters["Filters"] == 'ModifiedDuration':
            sql_query = sql_query.filter(func.coalesce(FactSheet.ModifiedDuration_yrs,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.ModifiedDuration_yrs,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'StandardDeviation':
            sql_query = sql_query.filter(func.coalesce(FactSheet.StandardDeviation,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.StandardDeviation,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'SharpeRatio':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SharpeRatio,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SharpeRatio,0) <= queryfilters["To_Val"])

        elif queryfilters["Filters"] == 'Beta':
            sql_query = sql_query.filter(func.coalesce(FactSheet.Beta,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.Beta,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'RSquare':
            sql_query = sql_query.filter(func.coalesce(FactSheet.R_Squared,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.R_Squared,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'Alpha':
            sql_query = sql_query.filter(func.coalesce(FactSheet.Alpha,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.Alpha,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'Mean':
            sql_query = sql_query.filter(func.coalesce(FactSheet.Mean,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.Mean,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'Sortino':
            sql_query = sql_query.filter(func.coalesce(FactSheet.Sortino,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.Sortino,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'Portfolio_PE_Ratio':
            sql_query = sql_query.filter(func.coalesce(FactSheet.PortfolioP_ERatio,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.PortfolioP_ERatio,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'TotalStocks':
            sql_query = sql_query.filter(func.coalesce(FactSheet.TotalStocks,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.TotalStocks,0) <= queryfilters["To_Val"])

        elif queryfilters["Filters"] == 'Age':
            sql_query = sql_query.filter(func.datediff(text('Year'),MFSecurity.MF_Security_OpenDate, date.today()) >= queryfilters["From_Val"]).filter(func.datediff(text('Year'),MFSecurity.MF_Security_OpenDate, date.today()) <= queryfilters["To_Val"])

        elif queryfilters["Filters"] == 'SOV' and sov_val == 'SOV':
            sql_query = sql_query.filter(func.coalesce(FactSheet.SOV,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.SOV,0) <= queryfilters["To_Val"])

        elif queryfilters["Filters"] == 'AAA' and sov_val == 'AAA':
            sql_query = sql_query.filter(func.coalesce(FactSheet.AAA,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.AAA,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'A1_Plus' and sov_val == 'A1_Plus':
            sql_query = sql_query.filter(func.coalesce(FactSheet.A1_Plus,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.A1_Plus,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'AA' and sov_val == 'AA':
            sql_query = sql_query.filter(func.coalesce(FactSheet.AA,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.AA,0) <= queryfilters["To_Val"])
        
        elif queryfilters["Filters"] == 'A_and_Below' and sov_val == 'A_and_Below':
            sql_query = sql_query.filter(func.coalesce(FactSheet.A_and_Below,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.A_and_Below,0) <= queryfilters["To_Val"])
            
        elif queryfilters["Filters"] == 'TreynorRatio':
            sql_query = sql_query.filter(func.coalesce(FactSheet.Treynor_Ratio,0) >= queryfilters["From_Val"]).filter(func.coalesce(FactSheet.Treynor_Ratio,0) <= queryfilters["To_Val"])
    
    sql_fund_mining = sql_query.all()
    all_plans = list()

    if sql_fund_mining:
        for fund_mining in sql_fund_mining:
            if not fund_mining.Plan_Id in all_plans:
                all_plans.append(fund_mining.Plan_Id)

                data = dict()

                data["plan_id"] = fund_mining.Plan_Id
                data["plan_name"] = fund_mining.Plan_Name
                data["amc_name"] = fund_mining.AMC_Name
                data["amc_id"] = fund_mining.AMC_Id
                data["product_id"] = fund_mining.Product_Id
                data["product_name"] = fund_mining.Product_Name
                data["product_code"] = fund_mining.Product_Code
                data["classification_name"] = fund_mining.Classification_Name
                data["amc_logo"] = fund_mining.AMC_Logo
                data["scheme_returns_1month"] = fund_mining.SCHEME_RETURNS_1MONTH
                data["scheme_returns_3month"] = fund_mining.SCHEME_RETURNS_3MONTH
                data["scheme_returns_6month"] = fund_mining.SCHEME_RETURNS_6MONTH
                data["scheme_returns_1year"] = fund_mining.SCHEME_RETURNS_1YEAR
                data["scheme_returns_2year"] = fund_mining.SCHEME_RETURNS_2YEAR
                data["scheme_returns_3year"] = fund_mining.SCHEME_RETURNS_3YEAR
                data["scheme_returns_5year"] = fund_mining.SCHEME_RETURNS_5YEAR
                data["scheme_returns_10year"] = fund_mining.SCHEME_RETURNS_10YEAR
                data["scheme_returns_ince"] = fund_mining.SCHEME_RETURNS_since_inception
                data["fund_manager_name"] = fund_mining.FundManager_Name
                data["aum"] = fund_mining.NetAssets_Rs_Cr
                data["transaction_date"] = fund_mining.TransactionDate

                resp.append(data)
    

    return jsonify(resp)

@factsheet_bp.route('/api/v1/equity_marketcap', methods=['GET'])
def equity_marketcap():

    resp = list()
    
    sql_equitydata = current_app.store.db.query(FundStocks.MarketCap).filter(FundStocks.MarketCap.not_in([" ", "-", "0"])).filter(FundStocks.MarketCap != None).distinct().order_by(FundStocks.MarketCap).all()

    for equitydata in sql_equitydata:
        data = dict()
        data["key"] = equitydata.MarketCap
        data["label"] = equitydata.MarketCap

        resp.append(data)
    
    return jsonify(resp)


@factsheet_bp.route('/api/v1/get_plans', methods=['GET'])
def get_plans():
    plan_name = request.args.get('plan_name', type=str, default=None)

    resp = list()

    if not plan_name:
        raise BadRequest('Plan Name is required.')

    sql_plans = current_app.store.db.query(Plans.Plan_Id, Plans.Plan_Name).join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(MFSecurity.Status_Id == 1).filter(Plans.Plan_Name.like('%' + plan_name + '%')).all() 

    for plan in sql_plans:
        data = dict()
        data["key"] = plan.Plan_Id
        data["label"] = plan.Plan_Name

        resp.append(data)
    
    return jsonify(resp)

    
@factsheet_bp.route('/api/v1/get_plan_type', methods=['GET'])
def get_plan_type():
    resp = list()
    sql_plantype = current_app.store.db.query(PlanType).filter(PlanType.Is_Deleted != 1).all() 

    for plantype in sql_plantype:
        data = dict()
        data["plantype_id"] = plantype.PlanType_Id
        data["plantype"] = plantype.PlanType_Name

        resp.append(data)    
    
    return jsonify(resp)


@factsheet_bp.route("/api/v1/get_benchmark_nav", methods=['GET'])
def get_benchmark_nav():
    transaction_date = request.args.get("date", type=str)
    benchmark_id = request.args.get("benchmark_id", type=int)
    plan_id = request.args.get("plan_id", type=int)

    resp = list()

    # if not transaction_date:
    #     raise BadRequest("Required parameter: <date>")

    if not benchmark_id:
        raise BadRequest("Required parameter: <benchmark_id>")

    fund_inception_date = None
    if plan_id:
        fund_inception_date = current_app.store.db.query(MFSecurity.MF_Security_OpenDate).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(MFSecurity.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).filter(Plans.Plan_Id == plan_id).scalar()
            
    sql_navdata = current_app.store.db.query(NAV.NAV_Date,
                                             NAV.NAV)\
                                      .filter(NAV.NAV_Type=="I",
                                              NAV.Plan_Id == benchmark_id,
                                              NAV.Is_Deleted != 1)\
                                              
    if transaction_date:
        sql_navdata = sql_navdata.filter(NAV.NAV_Date <= transaction_date)
        
    if fund_inception_date:
        sql_navdata = sql_navdata.filter(NAV.NAV_Date >= fund_inception_date)
                                      
    sql_navdata = sql_navdata.order_by(NAV.NAV_Date).all() 

    if sql_navdata:
        for sql_nav in sql_navdata:
            res = dict()
            res["nav_date"] = sql_nav.NAV_Date
            res["nav"] = sql_nav.NAV

            resp.append(res)

    return jsonify(resp)



@factsheet_bp.route("/api/v1/generate_fundcompare_pdf", methods=['POST'])
def generate_fundcompare_pdf():
    f = request.json

    plan_list = f["plans_list"]
    organization_id = f["organization_id"]
    send_email = None
    client_name = None
    organization_name = None

    if 'is_email' in f:
        send_email = f["is_email"]

        if 'email_id' in f:
            email_id = f["email_id"]  
            client_name = f["client_name"]
            organization_name = f["organization_name"]

    #get path
    image_path = current_app.config['IMAGE_PATH']
    whitelabel_dir = current_app.config['WHITELABEL_DIR']
    report_generation_path = current_app.config['REPORT_GENERATION_PATH']

    file_name = get_fundcomparepdf(current_app.store.db, organization_id, image_path, whitelabel_dir, report_generation_path, plan_list)

    #TODO move this to lib
    if send_email:
        template_vars = dict()
        organization_whitelabel_data = get_organization_whitelabel(current_app.store.db, organization_id)
        template_vars["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{organization_whitelabel_data['logo']}"        
        template_vars["customer_name"] = client_name
        template_vars["organization_name"] = organization_name        
        template_vars["report_name"] = "Fund Comparison Report"

        html_body = prepare_pdf_from_html(template_vars, 'pdf_fundcompare_email_template.html', report_generation_path, True)

        subject = F"Fund Compare Report"

        attachements = list()
        attachements.append(file_name)
        schedule_email_activity(current_app.store.db, email_id, '', '', subject, html_body, attachements)
        
        resp = dict()
        resp["msg"] = 'Report shared successfully.'
        return resp

    return send_file(file_name, attachment_filename="fundcompare.pdf")

@factsheet_bp.route("/api/v1/generate_fundoverlap_pdf", methods=['POST'])
def generate_fundoverlap_pdf():
    f = request.json

    plan_list = f["plans_list"]
    organization_id = f["organization_id"]
    send_email = None
    client_name = None
    organization_name = None
    if f:
        if 'is_email' in f:
            send_email = f["is_email"]

            if 'email_id' in f:
                email_id = f["email_id"]  
                client_name = f["client_name"]
                organization_name = f["organization_name"]
        
    #get path
    image_path = current_app.config['IMAGE_PATH']
    whitelabel_dir = current_app.config['WHITELABEL_DIR']
    generatereport_dir = current_app.config['REPORT_GENERATION_PATH']

    file_name = get_fundoverlappdf(current_app.store.db, organization_id, image_path, whitelabel_dir, generatereport_dir, plan_list)

    #TODO move this to lib
    if send_email:
        template_vars = dict()
        organization_whitelabel_data = get_organization_whitelabel(current_app.store.db, organization_id)
        template_vars["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{organization_whitelabel_data['logo']}"        
        template_vars["customer_name"] = client_name
        template_vars["organization_name"] = organization_name        
        template_vars["report_name"] = "Fund Overlap Report"

        environment = Environment(loader=FileSystemLoader('./src/templates'), keep_trailing_newline=False, trim_blocks=True, lstrip_blocks=True)

        template = environment.get_template('pdf_fundcompare_email_template.html')
        html_body = template.render(template_vars)
        subject = F"Fund Overlap Report"

        attachements = list()
        attachements.append(file_name)
        schedule_email_activity(current_app.store.db, email_id, '', '', subject, html_body, attachements)

        resp = dict()
        resp["msg"] = 'Report shared successfully.'
        return resp
        
    return send_file(file_name, attachment_filename="fundoverlap.pdf")


@factsheet_bp.route("/api/v1/export_navmovements", methods=['POST'])
def export_navmovements():
    f = request.json
    start_date = f["start_date"]
    end_date = f["end_date"]
    plan_id = f["plan_id"]

    resp = list()

    if not start_date:
        raise BadRequest("Required Payload Parameter: <start_date> is required.")

    if not end_date:
        raise BadRequest("Required Payload Parameter: <end_date> is required.")

    if not plan_id:
        raise BadRequest("Required Payload Parameter: <plan_id> is required.")

    resp = get_fund_nav(current_app.store.db, plan_id, None, None, start_date, end_date)

    df = pd.DataFrame(resp)

    # get path for template
    api_path = current_app.config['DOC_ROOT_PATH']
    whitelabel_dir = current_app.config['WHITELABEL_DIR']
    generatereport_dir = current_app.config['GENERATEREPORT_DIR']

    logo_path = F"{api_path}/{whitelabel_dir}/logo.png"
    report_url = os.path.abspath(F"./{generatereport_dir}/")
    file_path = F"{report_url}/{str(uuid.uuid4())}.xlsx"

    file_path = get_excel_report(df, file_path, logo_path, 'NAV Movements')

    return send_file(file_path)

# FactSheet/get_portfolio_instrument_data_month_wise
@factsheet_bp.route("/api/v1/get_portfolio_instrument_data_month_wise")
def get_portfolio_instrument_data_month_wise():
    portfolio_date = request.args.get("date", type=str)

    resp = list()
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")
    
    resp = get_portfolio_instrument_data(current_app.store.db, plan_id, portfolio_date)

    return jsonify(resp)


# FactSheet/get_portfolio_instrumentrating_data_month_wise
@factsheet_bp.route("/api/v1/get_portfolio_instrumentrating_data_month_wise")
def get_portfolio_instrumentrating_data_month_wise():
    portfolio_date = request.args.get("date", type=str)

    resp = list()
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    resp = get_portfolio_instrumentrating_data(current_app.store.db, plan_id, portfolio_date)

    return jsonify(resp)

# FactSheet/get_portfolio_sector_data_month_wise
@factsheet_bp.route("/api/v1/get_portfolio_sector_data_month_wise")
def get_portfolio_sector_data_month_wise():
    portfolio_date = request.args.get("date", type=str)

    resp = list()
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    resp = get_holdings_sector_data(current_app.store.db, plan_id, portfolio_date)

    return jsonify(resp)


# FactSheet/get_secured_unsecured_data
@factsheet_bp.route("/api/v1/get_secured_unsecured_data")
def get_secured_unsecured_data():
    portfolio_date = request.args.get("date", type=str)

    resp = list()
    requestdata = get_requestdata(request)

    plan_id = requestdata["plan_id"]
    if not plan_id:
        plan_id = get_plan_id(current_app.store.db, requestdata["plan_code"], requestdata["plan_name"], requestdata["scheme_id"], requestdata["scheme_code"], requestdata["scheme_name"] )
    
    if not requestdata["plan_id"] and not requestdata["plan_code"] and not requestdata["plan_name"] and not requestdata["scheme_id"] and not requestdata["scheme_code"] and not requestdata["scheme_name"] :
        raise BadRequest("Parameters required.")

    if not portfolio_date:
        #get last Portfolio_Date
        factsheet_query = current_app.store.db.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).order_by(desc(FactSheet.TransactionDate)).first()

        if factsheet_query:     
            portfolio_date = factsheet_query.Portfolio_Date

    resp = get_securedunsecured_data(current_app.store.db, plan_id, portfolio_date)        

    return jsonify(resp)

@factsheet_bp.route("/api_layer/Gsquare/M00", methods = ["POST"])
def gsquare_m00():
    request_data = get_gsquare_requestobject(request.data) if request.data else None

    resp_dict = dict()
    resp = list()
    logging.warning("API - Gsquare/M00 - start")
    # logging.basicConfig(filename='new.log', encoding='utf-8', level=logging.INFO, format='%(message)s')

    plans = request_data["RequestObject"]["Plans"]
    period_dates = request_data["RequestObject"]["Dates"]
    benchmark_indices = request_data["RequestObject"]["BenchmarkIndices"]
    period = request_data["RequestObject"]["Period"]

    index = 0
    for benchmark_id in benchmark_indices:
        for dates in period_dates:
            att_date = datetime.strptime(dates, '%d-%m-%Y')

            # get benchmark details
            benchmark_details = get_benchmarkdetails(current_app.store.db, benchmark_id)

            if benchmark_details:
                date = strptime(dates, '%d-%m-%Y')
                monthfrom = None
                if period == '1M':
                    monthfrom = getbetweendate(0,0,date)
                elif period == '3M':
                    monthfrom = getbetweendate(2,0,date)
                elif period == '6M':
                    monthfrom = getbetweendate(5,0,date)
                elif period == '1Y':
                    monthfrom = getbetweendate(0,1,date)
                elif period == '2Y':
                    monthfrom = getbetweendate(0,2,date)
                elif period == '3Y':
                    monthfrom = getbetweendate(0,3,date)

                from_date = last_date_of_month(monthfrom.year, monthfrom.month)
                to_date = last_date_of_month(date[0], date[1])

                if benchmark_details.TRI_Co_Code:
                    tri_returns_data = current_app.store.db.query(TRIReturns)\
                                                        .filter(TRIReturns.TRI_Co_Code == benchmark_details.TRI_Co_Code,
                                                        TRIReturns.Is_Deleted != 1, 
                                                        TRIReturns.TRI_IndexDate == att_date)\
                                                        .first()

                    if tri_returns_data:
                        per_ret = {
                            '1M': tri_returns_data.Return_1Month,
                            '3M': tri_returns_data.Return_3Month,
                            '6M': tri_returns_data.Return_6Month,
                            '1Y': tri_returns_data.Return_1Year,
                            '2Y': '',
                            '3Y': tri_returns_data.Return_3Year,
                        }

                        if period == '2Y':
                            per_ret["2Y"] = calculate_benchmark_tri_returns(current_app.store.db, benchmark_details.TRI_Co_Code, from_date, to_date)

                        data = dict()
                        data["BenchmarkIndices_Id"] = benchmark_id
                        data["BenchmarkIndices_Name"] = benchmark_details.BenchmarkIndices_Name
                        data["NAV_Date"] = att_date.strftime('%Y-%m-%d')
                        data["NAV"] = get_navbydate(current_app.store.db, benchmark_id, att_date, 'I')
                        data["Returns_Value"] =  per_ret[period]
                        resp.append(data)

                else:
                    cum_return = calculate_benchmark_tri_returns(current_app.store.db, benchmark_details.Co_Code, from_date, to_date)
                    
                    if cum_return:
                        data = dict()
                        data["BenchmarkIndices_Id"] = benchmark_id
                        data["BenchmarkIndices_Name"] = benchmark_details.BenchmarkIndices_Name
                        data["NAV_Date"] = att_date.strftime('%Y-%m-%d')
                        data["NAV"] = 10 if index == 0 else (10+((10*cum_return)/100))
                        data["Returns_Value"] = cum_return
                        resp.append(data)

            index = index + 1
    # a = pd.DataFrame(resp)
    # a.to_csv('new_resp_ind.csv')

    resp_dict["ResponseMessage"] = "Index NAV Information Fetched Successfully."
    resp_dict["ResponseObject"] = resp
    resp_dict["ResponseStatus"] = 1
    logging.warning("API - Gsquare/M00 - End")
    return jsonify(resp_dict)

@factsheet_bp.route("/api_layer/Gsquare/M01", methods = ["POST"])
def gsquare_m01():
    request_data = get_gsquare_requestobject(request.data) if request.data else None
    logging.warning("API - Gsquare/M01 - start")
    period = request_data["RequestObject"]["Period"]
    resp_dict = dict()
    resp = list()
    for plans in request_data["RequestObject"]["Plans"]:
        for dates in request_data["RequestObject"]["Dates"]:
            dates = datetime.strptime(dates, '%d-%m-%Y')

            plan_details = get_plan_meta_info(current_app.store.db, [plans])

            sql_factsheet = current_app.store.db.query(FactSheet)\
                                                        .filter(FactSheet.TransactionDate == dates, 
                                                                FactSheet.Is_Deleted != 1, 
                                                                FactSheet.Plan_Id == plans)\
                                                        .one_or_none()

            if sql_factsheet:
                per_ret = {
                            '1M':sql_factsheet.SCHEME_RETURNS_1MONTH,
                            '3M':sql_factsheet.SCHEME_RETURNS_3MONTH,
                            '6M':sql_factsheet.SCHEME_RETURNS_6MONTH,
                            '1Y':sql_factsheet.SCHEME_RETURNS_1YEAR,
                            '2Y':sql_factsheet.SCHEME_RETURNS_2YEAR,
                            '3Y':sql_factsheet.SCHEME_RETURNS_3YEAR,
                        }
                
                data = dict()
                data["Plan_Id"] = plans                    
                data["Plan_Code"] = plan_details[str(plans)]['plan_code']
                data["Plan_Name"] = plan_details[str(plans)]['plan_name']
                data["NAV_Date"] = dates.strftime('%Y-%m-%d')
                data["NAV"] = get_navbydate(current_app.store.db, plans, dates, 'P')
                data["Returns_Value"] = per_ret[period]
                resp.append(data)
    # a = pd.DataFrame(resp)
    # a.to_csv('new_resp_ind.csv')
    resp_dict["ResponseMessage"] = "Fund NAV Information Fetched Successfully."
    resp_dict["ResponseObject"] = resp
    resp_dict["ResponseStatus"] = 1 
    logging.warning("API - Gsquare/M01 - End")

    return jsonify(resp_dict) 


@factsheet_bp.route("/api_layer/Gsquare/M02", methods = ["POST"])
def gsquare_m02():
    request_data = get_gsquare_requestobject(request.data) if request.data else None
    
    logging.warning("API - Gsquare/M02 - start")
    resp_dict = dict()
    resp = list()
    plan_id = None
    plan_code = None
    plan_name = None

    for plans in request_data["RequestObject"]["Plans"]:
        for dates in request_data["RequestObject"]["Dates"]:
            portfolio_date = None
            dates = datetime.strptime(dates, '%d-%m-%Y')

            plan_details = get_plan_meta_info(current_app.store.db, [plans])

            holdings = current_app.store.db.query(UnderlyingHoldings.Portfolio_Date)\
                                                    .join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                    .filter(MFSecurity.Status_Id == 1)\
                                                    .filter(Plans.Is_Deleted != 1)\
                                                    .filter(MFSecurity.Is_Deleted != 1)\
                                                    .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                                    .filter(Plans.Plan_Id == plans)\
                                                    .filter(extract('year', UnderlyingHoldings.Portfolio_Date) == dates.year)\
                                                    .filter(extract('month', UnderlyingHoldings.Portfolio_Date) == dates.month)\
                                                    .order_by(desc(UnderlyingHoldings.Portfolio_Date))\
                                                    .first()

            if holdings:
                portfolio_date = holdings.Portfolio_Date

            holdings_data = get_detailed_fund_holdings(current_app.store.db, plans, None, portfolio_date, get_full_holding=True)

            if holdings_data:
                for holding in holdings_data:
                    plan_id = plans
                    plan_code = plan_details[str(plans)]['plan_code']
                    plan_name = plan_details[str(plans)]['plan_name']

                    data = dict()
                    data["Plan_Id"] = plans                    
                    data["Plan_Code"] = plan_details[str(plans)]['plan_code']
                    data["Plan_Name"] = plan_details[str(plans)]['plan_name']
                    data["Date"] = dates.strftime('%Y-%m-%d')
                    data["ISIN"] = holding['isin_code']
                    data["Weight_in_Percentage"] = holding['percentage_to_aum']
                    resp.append(data)
        
    resp_df = pd.DataFrame(resp)
    if not resp_df.empty:
        all_isin = resp_df['ISIN'].unique()
        for isin in all_isin:
           for dates in request_data["RequestObject"]["Dates"]:
                dates = datetime.strptime(dates, '%d-%m-%Y')
                not_available = resp_df[(resp_df.ISIN == isin) & (resp_df.Date == dates.strftime('%Y-%m-%d'))].empty #check if it exists in df
                if not_available and isin:
                    data = dict()
                    data["Plan_Id"] = plan_id                    
                    data["Plan_Code"] = plan_code
                    data["Plan_Name"] = plan_name
                    data["Date"] = dates.strftime('%Y-%m-%d')
                    data["ISIN"] = isin
                    data["Weight_in_Percentage"] = 0
                    resp.append(data)

    #remove duplicates if any need to handle it properly through data
    new_resp = list()
    for i in range(len(resp)):
        if resp[i] not in resp[i + 1:]:
            new_resp.append(resp[i])
    
    # a = pd.DataFrame(resp)
    # a.to_csv('new_resp_ind.csv')

    resp_dict["ResponseMessage"] = "Fund Constituents Information Fetched Successfully."
    resp_dict["ResponseObject"] = resp
    resp_dict["ResponseStatus"] = 1 

    logging.warning("API - Gsquare/M02 - End")

    return jsonify(resp_dict) 


@factsheet_bp.route("/api_layer/Gsquare/M03", methods = ["POST"])
def gsquare_m03():
    request_data = get_gsquare_requestobject(request.data) if request.data else None
    
    logging.warning("API - Gsquare/M03 - start")
    resp_dict = dict()
    resp = list()
    isin_code = request_data["RequestObject"]["ISIN_Code"]
    
    if isin_code:
        security_details = current_app.store.db.query(HoldingSecurity.HoldingSecurity_Id, 
                                                      HoldingSecurity.HoldingSecurity_Name, 
                                                      HoldingSecurity.Instrument_Type, 
                                                      HoldingSecurity.ISIN_Code, 
                                                      HoldingSecurity.BSE_Code, 
                                                      HoldingSecurity.NSE_Symbol, 
                                                      HoldingSecurity.MarketCap, 
                                                      Sector.Sector_Name)\
                                                .join(Sector, Sector.Sector_Id == HoldingSecurity.Sector_Id)\
                                                .filter(HoldingSecurity.ISIN_Code.in_(isin_code),
                                                        HoldingSecurity.Is_Deleted != 1,
                                                        HoldingSecurity.active != 0)\
                                                .order_by(desc(HoldingSecurity.HoldingSecurity_Name))\
                                                .all()

        for data in security_details:
            data_dict = dict()
            data_dict["ISIN_Code"] = data["ISIN_Code"]
            data_dict["Symbol"] = data["ISIN_Code"]
            data_dict["Name"] = data["HoldingSecurity_Name"]
            data_dict["Sector"] = data["Sector_Name"]
            data_dict["Capitalization"] = None
            data_dict["Asset_Class"] = 'Equity'
            resp.append(data_dict)

    # a = pd.DataFrame(resp)
    # a.to_csv('new_resp_ind.csv')
    
    resp_dict["ResponseMessage"] = "Scrip Master Information Fetched Successfully."
    resp_dict["ResponseObject"] = resp
    resp_dict["ResponseStatus"] = 1 
    logging.warning("API - Gsquare/M03 - End")

    return jsonify(resp_dict) 


@factsheet_bp.route("/api_layer/Gsquare/M04", methods = ["POST"])
def gsquare_m04():
    request_data = get_gsquare_requestobject(request.data) if request.data else None
    
    logging.warning("API - Gsquare/M04 - start")
    resp_dict = dict()
    resp = list()

    for benchmark_id in request_data["RequestObject"]["BenchmarkIndices"]:
        for dates in request_data["RequestObject"]["Dates"]:
            dates = datetime.strptime(dates, '%d-%m-%Y')
            dates_4 = dates - timedelta(days=4)

            res = get_index_constituents(benchmark_id, dates_4, dates)
            if not res:
                BenchmarkIndices1 = aliased(BenchmarkIndices)
                sql_non_tri_index = current_app.store.db.query(BenchmarkIndices1.BenchmarkIndices_Id)\
                                                            .select_from(BenchmarkIndices)\
                                                            .join(BenchmarkIndices1, BenchmarkIndices1.TRI_Co_Code == BenchmarkIndices.Co_Code)\
                                                            .filter(BenchmarkIndices.BenchmarkIndices_Id == benchmark_id,
                                                                    BenchmarkIndices.Is_Deleted != 1, 
                                                                    BenchmarkIndices1.Is_Deleted != 1, 
                                                                    BenchmarkIndices.Co_Code != None, 
                                                                    BenchmarkIndices1.TRI_Co_Code != None)\
                                                            .scalar()

                res = get_index_constituents(sql_non_tri_index, dates_4, dates)
            
            if res:
                resp += res

    resp_df = pd.DataFrame(resp)
    if not resp_df.empty:
        bm_name = resp_df['BenchmarkIndices_Name'][0]
        all_isin = resp_df['ISIN'].unique()
        for isin in all_isin:
           for dates in request_data["RequestObject"]["Dates"]:
                dates = datetime.strptime(dates, '%d-%m-%Y')
                not_available = resp_df[(resp_df.ISIN == isin) & (resp_df.Date == dates.strftime('%Y-%m-%d'))].empty #check if it exists in df
                if not_available and isin:
                    data_dict = dict()
                    data_dict["BenchmarkIndices_Name"] = bm_name
                    data_dict["Date"] = dates.strftime('%Y-%m-%d')
                    data_dict["ISIN"] = isin
                    data_dict["Weight_in_Percentage"] = 0
                    resp.append(data_dict)
                    
    # a = pd.DataFrame(resp)
    # a.to_csv('new_resp_ind.csv')

    resp_dict["ResponseMessage"] = "Index Constituents Information Fetched Successfully."
    resp_dict["ResponseObject"] = resp
    resp_dict["ResponseStatus"] = 1 
    logging.warning("API - Gsquare/M04 - End")

    return jsonify(resp_dict) 


def get_index_constituents(benchmark_id, from_date, to_date):
    resp = list()

    max_index_date = current_app.store.db.query(func.max(IndexWeightage.WDATE))\
                                                .select_from(IndexWeightage)\
                                                .join(BenchmarkIndices, BenchmarkIndices.Co_Code == cast(IndexWeightage.Index_CO_CODE, sqlalchemy.String))\
                                                .filter(IndexWeightage.Is_Deleted != 1, 
                                                        BenchmarkIndices.BenchmarkIndices_Id == benchmark_id, 
                                                        IndexWeightage.WDATE >= from_date, 
                                                        IndexWeightage.WDATE <= to_date)\
                                                .scalar()

    if max_index_date:                
        sql_index_data = current_app.store.db.query(BenchmarkIndices.BenchmarkIndices_Id, 
                                                    BenchmarkIndices.BenchmarkIndices_Name, 
                                                    HoldingSecurity.ISIN_Code, 
                                                    IndexWeightage.WEIGHT_INDEX, 
                                                    IndexWeightage.CO_CODE)\
                                            .select_from(BenchmarkIndices)\
                                            .join(IndexWeightage, cast(IndexWeightage.Index_CO_CODE, sqlalchemy.String) == BenchmarkIndices.Co_Code)\
                                            .join(HoldingSecurity, HoldingSecurity.Co_Code == cast(IndexWeightage.CO_CODE, sqlalchemy.String))\
                                            .filter(BenchmarkIndices.BenchmarkIndices_Id == benchmark_id, 
                                                    IndexWeightage.Is_Deleted != 1, 
                                                    HoldingSecurity.ISIN_Code != None, 
                                                    HoldingSecurity.Is_Deleted != 1, 
                                                    HoldingSecurity.active != 0, 
                                                    IndexWeightage.WDATE == max_index_date)\
                                            .all()

        if sql_index_data:
            for index_data in sql_index_data:
                data_dict = dict()
                data_dict["BenchmarkIndices_Name"] = index_data.BenchmarkIndices_Name
                data_dict["Date"] =  to_date.strftime('%Y-%m-%d')
                data_dict["ISIN"] = index_data.ISIN_Code 
                data_dict["Weight_in_Percentage"] = index_data.WEIGHT_INDEX
                resp.append(data_dict)

    return resp


@factsheet_bp.route("/api_layer/Gsquare/M05", methods = ["POST"])
def gsquare_m05():
    request_data = get_gsquare_requestobject(request.data) if request.data else None

    logging.warning("API - Gsquare/M05 - start")
    resp_dict = dict()
    resp = list()
    all_isin = request_data["RequestObject"]["ISIN_Code"]
    
    for dates in request_data["RequestObject"]["Dates"]:
        dates = datetime.strptime(dates, '%d-%m-%Y')
        
        if all_isin:    
            max_closing_nav_dates = current_app.store.db.query(HoldingSecurity.Co_Code, func.max(ClosingValues.Date_).label('max_closingvalues_date'))\
            .join(HoldingSecurity, HoldingSecurity.Co_Code == ClosingValues.Co_Code)\
            .filter(ClosingValues.Is_Deleted != 1, 
                    ClosingValues.ST_EXCHNG == 'NSE',
                    HoldingSecurity.Is_Deleted != 1,
                    HoldingSecurity.active != 0,
                    HoldingSecurity.ISIN_Code.in_(all_isin),
                    ClosingValues.Date_<=dates)\
            .group_by(HoldingSecurity.Co_Code).subquery()

            closing_nav = current_app.store.db.query(HoldingSecurity.ISIN_Code, 
                                                        ClosingValues.CLOSE,
                                                        ClosingValues.Date_)\
                                                    .join(max_closing_nav_dates, 
                                                            and_(max_closing_nav_dates.c.Co_Code == ClosingValues.Co_Code, max_closing_nav_dates.c.max_closingvalues_date == ClosingValues.Date_))\
                                                    .join(HoldingSecurity, HoldingSecurity.Co_Code == max_closing_nav_dates.c.Co_Code)\
                                                    .filter(ClosingValues.Is_Deleted != 1, 
                                                            ClosingValues.ST_EXCHNG == 'NSE',
                                                            HoldingSecurity.Is_Deleted != 1,
                                                            HoldingSecurity.active != 0).distinct().all()#used distinct as many isin having diff isin - historically. Need to change this logic in constituents as well.
            
            for data in closing_nav:
                data_dict = dict()
                data_dict["ISIN_Code"] = data["ISIN_Code"]
                data_dict["Date"] =  dates.strftime('%Y-%m-%d')
                data_dict["CLOSE"] = data["CLOSE"]
                resp.append(data_dict)
    # a = pd.DataFrame(resp)
    # a.to_csv('new_resp_ind.csv')
    resp_dict["ResponseMessage"] = "Security Closing Information Fetched Successfully."
    resp_dict["ResponseObject"] = resp
    resp_dict["ResponseStatus"] = 1
    logging.warning("API - Gsquare/M05 - End")

    return jsonify(resp_dict) 


#TODO move this to finalyca_lib
def get_gsquare_requestobject(request_data):
    b = request_data.decode('utf-8')
    b = b.replace('\\', '').replace('"[', '[').replace(']"', ']').replace('"["', '["').replace('"]"', '"]').replace('"{', '{').replace('}"', '}').replace('\r\n', '')
    return json.loads(b)


@factsheet_bp.route("/api/v1/get_benchmark_list", methods=['GET'])
def get_benchmarklist():
    list_for = request.args.get("list_for", type=str)
    today = dt.today()
    today = today.replace(day=1)
    last_month = today - timedelta(days=1)
    two_year_ago = datetime.strptime(get_business_day(last_month - relativedelta(years=2), timeframe_in_days = 0), '%Y-%m-%d') 
    five_year_ago = datetime.strptime(get_business_day(last_month - relativedelta(years=5), timeframe_in_days = 0), '%Y-%m-%d') 
    db_session = current_app.store.db

    if list_for == '' or list_for == None:
        raise BadRequest("Required parameter : 'list_for'. Expected values : attribution | timeseries | overlap")

    # master query for all active in-use benchmarks
    bmk_master_subq = db_session.query(BenchmarkIndices.BenchmarkIndices_Id,
                                       BenchmarkIndices.BenchmarkIndices_Name,
                                       BenchmarkIndices.Co_Code)\
                                        .filter(BenchmarkIndices.Co_Code != None, BenchmarkIndices.Is_Deleted != 1)\
                                        .subquery()
                                # .join(MFSecurity, MFSecurity.BenchmarkIndices_Id == BenchmarkIndices.BenchmarkIndices_Id)\
                                # MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1, 
                                

    if list_for == 'attribution':
        # get tri co codes for the benchmarks with MoM returns for last 24 months

        max_co_code_subq = db_session.query(TRIReturns.TRI_Co_Code.label('Co_Code'),
                                        func.max(TRIReturns.TRI_IndexDate).label('max_index_date'))\
                                            .filter(TRIReturns.Is_Deleted != 1, TRIReturns.TRI_IndexDate >= two_year_ago)\
                                            .group_by(TRIReturns.TRI_Co_Code, extract('year', TRIReturns.TRI_IndexDate), extract('month', TRIReturns.TRI_IndexDate))\
                                            .subquery()
        
        co_code_subq = db_session.query(max_co_code_subq.c.Co_Code.label('Co_Code'))\
                                            .group_by(max_co_code_subq.c.Co_Code)\
                                 .having(func.count(max_co_code_subq.c.max_index_date) >= 24)\
                                 .subquery()
        
    elif list_for == 'timeseries':
        # get co codes for the benchmarks with 5 yr NAV
        co_code_subq = db_session.query(BenchmarkIndices.Co_Code)\
                                 .join(NAV, NAV.Plan_Id == BenchmarkIndices.BenchmarkIndices_Id)\
                                 .filter(NAV.Is_Deleted != 1,
                                         BenchmarkIndices.Is_Deleted != 1,
                                         NAV.NAV_Date >= datetime.strftime(five_year_ago, r'%Y-%m-%d'),
                                         NAV.NAV_Type == 'I',
                                         BenchmarkIndices.TRI_Co_Code != None)\
                                 .group_by(BenchmarkIndices.Co_Code, NAV.NAV_Date)\
                                 .distinct()\
                                 .having(func.min(NAV.NAV_Date) <= datetime.strftime(five_year_ago, r'%Y-%m-%d'))\
                                 .subquery()
    elif list_for == 'overlap':
        max_date_index_wts = db_session.query(func.max(IndexWeightage.WDATE)).scalar()
        if max_date_index_wts:
            co_code_subq = db_session.query(IndexWeightage.Index_CO_CODE.label('Co_Code'))\
                                     .filter(IndexWeightage.WDATE == max_date_index_wts).subquery()
        else:
            raise BadRequest("No Index Weights Available.")

    # final query
    sql_benchmark = current_app.store.db.query(bmk_master_subq.c.BenchmarkIndices_Id,
                                               bmk_master_subq.c.BenchmarkIndices_Name)\
                                        .select_from(bmk_master_subq)\
                                        .join(co_code_subq, bmk_master_subq.c.Co_Code == co_code_subq.c.Co_Code)\
                                        .order_by(bmk_master_subq.c.BenchmarkIndices_Name)\
                                        .distinct()\
                                        .all()

    resp = list()
    for bm in sql_benchmark:
        data = dict()
        data["key"] = bm.BenchmarkIndices_Id
        data["label"] = bm.BenchmarkIndices_Name

        resp.append(data)

    return jsonify(resp)


@factsheet_bp.route("/api/v1/get_benchmark_returns", methods=['GET'])
def get_benchmark_returns():
    from_date = request.args.get("from_date", type=str)
    to_date = request.args.get("to_date", type=str)
    benchmark_id = request.args.get("benchmark_id", type=int)

    if not from_date or not to_date:
        raise BadRequest("Required parameter: from_date | to_date")

    if not benchmark_id:
        raise BadRequest("Required parameter: benchmark_id")

    from_date = parser.parse(from_date)
    to_date = parser.parse(to_date)

    if from_date > to_date:
        raise BadRequest("Validation error: from_date cannot be greater than to_date")

    db_session = current_app.store.db

    # get benchmark details
    benchmark_details = get_benchmarkdetails(db_session, benchmark_id)
    co_code = benchmark_details.Co_Code

    if co_code:
        cum_return = calculate_benchmark_tri_returns(db_session, co_code, from_date, to_date, typeof_return='absolute')
        resp = []
        if cum_return:
            data = dict()
            data["BenchmarkIndices_Id"] = benchmark_id
            data["Returns_Value"] = round(cum_return, 2)
            resp.append(data)
    else:
        raise BadRequest('Data Error: No benchmark found.')

    return jsonify(resp)


@factsheet_bp.route('/api/v1/get_growth_journey_by_amc', methods=['GET'])
def get_growth_journey_by_amc():
    sebi_nr = request.args.get("sebi_nr", type=str)

    db_session = get_sebi_database_scoped_session()
    growth = get_growth_journey_by_sebi_nr(db_session, sebi_nr, None)

    return jsonify(growth)


@factsheet_bp.route('/api/v1/get_flow_journey_by_amc', methods=['GET'])
def get_flow_journey_by_amc():
    sebi_nr = request.args.get("sebi_nr", type=str)

    db_session = get_sebi_database_scoped_session()
    flow = get_flow_journey_by_sebi_nr(db_session, sebi_nr, None)

    return jsonify(flow)

@factsheet_bp.route('/api/v1/get_dashboard_cash_level', methods=['GET'])
def get_dashboard_cash_level():
    classification_id = request.args.getlist("classification_id", type=str)
    products = request.args.getlist("product_id", type=str)

    today = dt.today()
    today = today.replace(day=1)
    two_month_ago = today - relativedelta(months=2)

    #pass this classification id from front end
    # classification_id = [50, 54, 95, 155, 153]
    resp = dict()

    sql_cashlevel_subquery = current_app.store.db.query(func.max(FactSheet.TransactionDate).label('TransactionDate'),
                                                        func.max(Plans.Plan_Id).label('Plan_Id'),
                                                        Fund.Fund_Id,
                                                        Product.Product_Name,
                                                        Classification.Classification_Id)\
                                                        .select_from(FactSheet)\
                                                        .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                                                        .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                                        .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                        .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                        .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id)\
                                                        .filter(FactSheet.Is_Deleted != 1, 
                                                                Plans.Is_Deleted != 1, 
                                                                FactSheet.NetAssets_Rs_Cr != None, 
                                                                FactSheet.Cash != None, 
                                                                PlanProductMapping.Is_Deleted != 1, 
                                                                MFSecurity.Status_Id == 1, 
                                                                PlanProductMapping.Product_Id.in_(products), 
                                                                Classification.Classification_Id.in_(classification_id),
                                                                FactSheet.TransactionDate >= two_month_ago)\
                                                        .group_by(Fund.Fund_Id, 
                                                                  Product.Product_Name, 
                                                                  Classification.Classification_Id)\
                                                        .subquery()
    
    sql_cashlevel = current_app.store.db.query(((func.sum((FactSheet.Cash * FactSheet.NetAssets_Rs_Cr)/100) * 100) / func.sum(FactSheet.NetAssets_Rs_Cr)).label('cash_weight'), 
                                                sql_cashlevel_subquery.c.Product_Name, 
                                                sql_cashlevel_subquery.c.Classification_Id)\
                                                .select_from(FactSheet)\
                                                .join(sql_cashlevel_subquery, and_(sql_cashlevel_subquery.c.Plan_Id == FactSheet.Plan_Id, 
                                                                                   sql_cashlevel_subquery.c.TransactionDate == FactSheet.TransactionDate))\
                                                .filter(FactSheet.Is_Deleted != 1)\
                                                .group_by(sql_cashlevel_subquery.c.Product_Name,
                                                          sql_cashlevel_subquery.c.Classification_Id)\
                                                .all()

    sql_classification = current_app.store.db.query(Classification.Classification_Name, Classification.Classification_Id)\
                                                    .filter(Classification.Classification_Id.in_(classification_id))\
                                                    .all()

    if sql_cashlevel and sql_classification:
        
        for classif_row in sql_classification:
            data = dict()
            data1 = dict()
            for row in sql_cashlevel:
                if classif_row.Classification_Id == row.Classification_Id:                    
                    data1[row.Product_Name] = row.cash_weight                                        

            resp[classif_row.Classification_Name] = data1

    return jsonify(resp)

@factsheet_bp.route('/api/v1/get_dashboard_top_movement', methods=['GET'])
def get_dashboard_top_movement():
    classification_id = request.args.getlist("classification_id", type=str)
    products = request.args.getlist("product_id", type=str)

    resp = dict()
    # classification_id = [50, 153, 155]
    # products = [1, 2, 4, 5]

    mining_query = get_latest_factsheet_query(current_app.store.db, products, [], [], [])
    mining_data = mining_query.filter(Plans.PlanType_Id==1).all()

    sql_classification = current_app.store.db.query(Classification.Classification_Name)\
                                                    .filter(Classification.Classification_Id.in_(classification_id)).all()
        
    sql_product = current_app.store.db.query(Product)\
                                            .filter(Product.Product_Id.in_(products),
                                                    Product.Is_Deleted != 1).all()

    if mining_data:
        df = pd.DataFrame(mining_data)
        #filtered debt funds and ETFs
        df = df[(df["AssetClass_Id"] != 3) & (df["Classification_Id"] != 147)]
        df['SCHEME_RETURNS_1YEAR'] = pd.to_numeric(df["SCHEME_RETURNS_1YEAR"])
        df['SCHEME_RETURNS_3YEAR'] = pd.to_numeric(df["SCHEME_RETURNS_3YEAR"])
        df['SCHEME_RETURNS_5YEAR'] = pd.to_numeric(df["SCHEME_RETURNS_5YEAR"])
        df = df.astype(object).replace(np.nan, None)

        for classif_row in sql_classification:
            classif_dict = dict()
            classif_dict = dashboard_top_movement(df, sql_product, classif_row.Classification_Name)
                
            resp[classif_row.Classification_Name] = classif_dict

        #for all
        resp['all'] = dashboard_top_movement(df, sql_product, '')
            
    return jsonify(resp)

def dashboard_top_movement(df, products, classification_name):    
    res_performance = dict()

    perf_period = [1, 3, 5]
    types = [
        {
            'type': 'performer',
            'ascending': False
        },
        {
            'type': 'Detractor',
            'ascending': True
        }
    ]
    for per_type in types:        
        classif_dict = dict()
        for period in perf_period:
            sort_column_name = F'SCHEME_RETURNS_{period}YEAR'
            period_dict = dict()

            for product in products:
                dt_list = list()
                
                df1 = df[((df["Classification_Name"] == classification_name) if classification_name != '' else 1 == 1 ) & (df["Product_Id"] == product.Product_Id) & (df['age_in_days'] >= (period * 365))]
                if not df1.empty:
                    df3 = df1.drop_duplicates()
                    df4 = df3[df3[sort_column_name].notnull()]
                    df5 = df4.sort_values(by=[sort_column_name], ascending=per_type['ascending'], inplace=False)                    
                    dt_list = df5.head(5).to_dict(orient="records")
                period_dict[product.Product_Name] = dt_list

            #TODO Reuse below code
            #all 
            dt_list = list()
            df1 = df[((df["Classification_Name"] == classification_name) if classification_name != '' else 1 == 1 ) & (df['age_in_days'] >= (period * 365))]
            if not df1.empty:
                df3 = df1.drop_duplicates()
                df4 = df3[df3[sort_column_name].notnull()]
                df5 = df4.sort_values(by=[sort_column_name], ascending=per_type['ascending'], inplace=False)                
                dt_list = df5.head(5).to_dict(orient="records")
            period_dict['all'] = dt_list
            
            classif_dict[period] = period_dict
        res_performance[per_type['type']] = classif_dict
    return res_performance

@factsheet_bp.route('/api/v1/get_dashboard_gainers_losers', methods=['GET'])
def get_dashboard_gainers_losers():
    resp = dict()

    # max_portfolio_date = current_app.store.db.query(func.max(FundStocks.Portfolio_Date)).scalar()

    # next_max_portfolio_date = shift_date(max_portfolio_date, 1,0)

    # get last two dates for which closing value is available
    # portfolio_closing_nav_date = current_app.store.db.query(func.max(ClosingValues.Date_).label('max_date'))\
    #                                                     .filter(ClosingValues.Is_Deleted != 1, 
    #                                                             ClosingValues.Date_ <= max_portfolio_date).scalar()

    
    # current_closing_nav_date = current_app.store.db.query(func.max(ClosingValues.Date_).label('max_date'))\
    #                                                     .filter(ClosingValues.Is_Deleted != 1, 
    #                                                             ClosingValues.ST_EXCHNG == 'NSE',
    #                                                             ClosingValues.Date_ <= next_max_portfolio_date).scalar()
    
    # get last two dates for which closing value is available
    max_closing_nav_dates = current_app.store.db.query((ClosingValues.Date_).label('max_date'))\
                                                        .filter(ClosingValues.Is_Deleted != 1, 
                                                                ClosingValues.ST_EXCHNG == 'NSE',
                                                                HoldingSecurity.Is_Deleted != 1
                                                                # ,ClosingValues.Date_ <= next_max_portfolio_date
                                                                ).distinct().order_by(desc(ClosingValues.Date_))\
                                                        .limit(2).all()
    
    if max_closing_nav_dates:
        cur_date = datetime.strftime(max_closing_nav_dates[0].max_date ,'%Y-%m-%d')
        prev_date = datetime.strftime(max_closing_nav_dates[1].max_date ,'%Y-%m-%d')
        product_ids = [1,4]

        df = get_fund_portfolio_movement_data_by_date(current_app.store.db, prev_date, cur_date, product_ids)

        if not df.empty:
            df1 = df.groupby(['Fund_Id', 'Fund_Name', 'Product_Name', 'Product_Id'], as_index=False).agg({'percentage_to_aum': "sum", 'value_in_inr': "sum", 'current_value_in_inr': "sum"})
            
            df1.rename(columns = {'value_in_inr':'prev_value_in_inr'}, inplace = True)
            df1.rename(columns = {'Fund_Name':'fund_name'}, inplace = True)
            df1.rename(columns = {'Product_Name':'product_name'}, inplace = True)
            df1.rename(columns = {'Product_Id':'product_id'}, inplace = True)

            df1 = df1.drop(df1[df1.current_value_in_inr == 0].index)
            df1 = df1.drop(df1[df1.percentage_to_aum <= 90].index)

            df1 = df1.astype(object).replace(np.nan, None)
            df1['today_return'] = ((df1['current_value_in_inr'].astype(float) * 100 / df1['prev_value_in_inr'].astype(float)) - 100)

            df_performers = df1.loc[df1['today_return']>0]
            df_detractors = df1.loc[df1['today_return']<0]

            df_performers.sort_values(by=['today_return'], ascending=False, inplace=True)
            df_detractors.sort_values(by=['today_return'], ascending=True, inplace=True)

            drop_cols = ['Fund_Id', 'prev_value_in_inr', 'current_value_in_inr', 'percentage_to_aum']
            df_performers = df_performers.drop(drop_cols, axis=1)
            df_detractors = df_detractors.drop(drop_cols, axis=1)

            product_type = dict()
            product_type['mutual_fund'] = pd.DataFrame(df_performers.loc[df_performers['product_id'] == 1]).to_dict('records')
            product_type['pms'] = pd.DataFrame(df_performers.loc[df_performers['product_id'] == 4]).to_dict('records')

            resp['gainers'] = product_type

            product_type = dict()
            product_type['mutual_fund'] = pd.DataFrame(df_detractors.loc[df_detractors['product_id'] == 1]).to_dict('records')
            product_type['pms'] = pd.DataFrame(df_detractors.loc[df_detractors['product_id'] == 4]).to_dict('records')
            
            resp['losers'] = product_type
            resp['date'] = datetime.strftime(max_closing_nav_dates[0].max_date ,'%d-%b-%Y')

    return jsonify(resp)

