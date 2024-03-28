import logging
import os
from typing import List
from sqlalchemy import and_, func
from sebi_lib.models import * 
from fin_models.masters_models import AMC as PMS_AMC
from datetime import date, datetime, timedelta
from sebi_lib.utils import get_sebi_database_scoped_session, get_last_day_for_prev_month
from utils.utils import print_query
import csv
from async_tasks.send_email import send_email_with_attachements
import json
from utils.finalyca_json_encoder import FinalycaJSONEncoder
from bizlogic.importer_helper import get_fund_manager_info_by_code, get_fundmanager_list, get_fund_manager_details

def get_amc(db_session, as_on_date, sebi_nr):
    obj = dict()

    new_date = as_on_date + timedelta(days=1)

    sql_amc = db_session.query( AMC.sebi_nr, AMC.name, AMC.register_date, AMCInfo.as_of, AMCInfo.address, AMCInfo.total_client, AMCInfo.total_aum_in_cr).join(AMCInfo, AMCInfo.sebi_nr==AMC.sebi_nr).filter(AMCInfo.as_of== as_on_date).filter(AMCInfo.is_discretionary_active == 1).filter(AMCInfo.sebi_nr==sebi_nr).filter(AMC.is_active==1).one_or_none()

    # Get other info if available from PMS_Base
    sql_pms_amc = db_session.query(PMS_AMC).filter(PMS_AMC.Product_Id==4).filter(PMS_AMC.SEBI_Registration_Number==sebi_nr).one_or_none()
    address = None
    if sql_pms_amc:
        address = sql_pms_amc.Address1 if sql_pms_amc.Address1 else ""
        address = "" if not address else address
        address += sql_pms_amc.Address2 if sql_pms_amc.Address2 else ""
    obj["address"] = address
    obj["logo"] = F"https://api.finalyca.com/{sql_pms_amc.AMC_Logo}" if sql_pms_amc else None
    obj["description"] = sql_pms_amc.AMC_Description if sql_pms_amc else None
    obj["email"] = sql_pms_amc.Email_Id if sql_pms_amc else None
    obj["mobile"] = sql_pms_amc.Contact_Numbers if sql_pms_amc else None
    obj["contact_name"] = sql_pms_amc.Contact_Person if sql_pms_amc else None
    obj["website"] = sql_pms_amc.Website_link if sql_pms_amc else None    
    sebi_nr = sql_amc.sebi_nr
    obj["as_of"] = sql_amc.as_of
    obj["sebi_nr"] = sebi_nr
    obj["name"] = sql_amc.name
    obj["url_name"] = str(sql_amc.name).replace(" ", "-") if sql_amc.name else None
    obj["register_date"] = sql_amc.register_date
    obj["address"] = sql_amc.address
    
    obj["facebook_url"] = sql_pms_amc.Facebook_url if sql_pms_amc else None 
    obj["linkedin_url"] = sql_pms_amc.Linkedin_url if sql_pms_amc else None
    obj["twitter_url"] = sql_pms_amc.Twitter_url if sql_pms_amc else None
    obj["youtube_url"] = sql_pms_amc.Youtube_url if sql_pms_amc else None
    
    obj["total_client"] = sql_amc.total_client if check_column_is_visible(db_session, sebi_nr, 'total_client') else 'NA'
    obj["total_aum_in_cr"] = sql_amc.total_aum_in_cr if check_column_is_visible(db_session, sebi_nr, 'aum') else 'NA'

    # Get growth journey for the AMC
    obj["growth"] = get_growth_journey_by_sebi_nr(db_session, sebi_nr, new_date)

    # Get flow journey for the AMC for DiscretionaryDetails
    obj["flow"] = get_flow_journey_by_sebi_nr(db_session, sebi_nr, new_date)

    # Get schemes for the AMC
    sql_schemes = db_session.query( Scheme.name, SchemePerformance.as_of, SchemePerformance.benchmark_name, SchemePerformance.aum, SchemePerformance.scheme_returns_1_month, SchemePerformance.scheme_returns_1_yr, SchemePerformance.benchmark_returns_1_month, SchemePerformance.benchmark_returns_1_yr).join(Scheme, SchemePerformance.scheme_id==Scheme.id).filter(SchemePerformance.as_of == as_on_date).filter(Scheme.sebi_nr == sebi_nr).order_by(SchemePerformance.as_of.desc()).all()

    schemes = list()
    for sql_scheme in sql_schemes:
        scheme = dict()
        scheme["as_on_date"] = sql_scheme.as_of
        scheme["aum"] = sql_scheme.aum if check_column_is_visible(db_session, sebi_nr, 'aum') else 'NA'
        scheme["name"] = sql_scheme.name
        scheme["product"] = "PMS"
        scheme["benchmark"] = sql_scheme.benchmark_name
        scheme["scheme_returns_1_month"] = sql_scheme.scheme_returns_1_month
        scheme["scheme_returns_1_yr"] = sql_scheme.scheme_returns_1_yr
        scheme["benchmark_returns_1_month"] = sql_scheme.benchmark_returns_1_month
        scheme["benchmark_returns_1_yr"] = sql_scheme.benchmark_returns_1_yr

        schemes.append(scheme)

    obj["schemes"] = schemes

    return obj
def get_flow_journey_by_sebi_nr(db_session, sebi_nr, new_date=None):
    sql_flows = db_session.query( DiscretionaryDetails.as_of, DiscretionaryDetails.inflow_month_in_cr, DiscretionaryDetails.outflow_month_in_cr)
    
    if new_date:
        sql_flows = sql_flows.filter(DiscretionaryDetails.as_of < new_date)
    
    sql_flows = sql_flows.filter(DiscretionaryDetails.sebi_nr == sebi_nr).order_by(DiscretionaryDetails.as_of.desc()).limit(12).all()

    flow = dict()
    flow["date"] = list()
    flow["monthly_inflow"] = list()
    flow["monthly_outflow"] = list()
    if check_column_is_visible(db_session, sebi_nr, 'monthly_flow'):
        for index in range(len(sql_flows)):
            sql_flow = sql_flows[-index-1]
            flow["date"].append(sql_flow.as_of.strftime('%b %y'))
            flow["monthly_inflow"].append(sql_flow.inflow_month_in_cr)
            flow["monthly_outflow"].append(sql_flow.outflow_month_in_cr)
    return flow

def get_growth_journey_by_sebi_nr(db_session, sebi_nr, new_date=None):    
    sql_growths = db_session.query(AMCInfo.as_of, AMCInfo.total_client, AMCInfo.total_aum_in_cr)

    if new_date:
        sql_growths = sql_growths.filter(AMCInfo.as_of < new_date)
    
    sql_growths = sql_growths.filter(AMCInfo.sebi_nr == sebi_nr).order_by(AMCInfo.as_of.desc()).limit(12).all()

    growth = dict()
    growth["date"] = list()
    growth["clients"] = list()
    growth["aum_in_cr"] = list()
    if check_column_is_visible(db_session, sebi_nr, 'aum') or check_column_is_visible(db_session, sebi_nr, 'total_client'):
        for index in range(len(sql_growths)):
            sql_growth = sql_growths[-index-1]
            growth["date"].append(sql_growth.as_of.strftime('%b %y'))
            growth["clients"].append(sql_growth.total_client  if check_column_is_visible(db_session, sebi_nr, 'total_client') else 'NA')
            growth["aum_in_cr"].append(sql_growth.total_aum_in_cr if check_column_is_visible(db_session, sebi_nr, 'aum') else 'NA')
    return growth

def check_column_is_visible(db_session, sebi_nr, column_name):
    sql_pms_amc = db_session.query(PMS_AMC).filter(PMS_AMC.Product_Id==4).filter(PMS_AMC.SEBI_Registration_Number==sebi_nr).one_or_none()
    if sql_pms_amc:
        if sql_pms_amc.hide_fields:
            hide_fields = str(sql_pms_amc.hide_fields).split(",")
            if column_name.lower() in hide_fields:
                return False
            
    return True
            
def get_sebi_website_export(db_session, as_on_date):
    total_data = dict()

    # sql_amcs = db_session.query(AMC.sebi_nr, AMC.name, AMC.register_date, AMCInfo.as_of, AMCInfo.address, AMCInfo.total_client, AMCInfo.total_aum_in_cr).join(AMCInfo, AMCInfo.sebi_nr==AMC.sebi_nr).filter(AMCInfo.as_of== as_on_date).filter(AMCInfo.is_discretionary_active == 1).filter(AMCInfo.total_client > 0).order_by(AMC.name).all()
    
    sql_amcs = db_session.query( AMCInfo.sebi_nr, AMC.name, func.max(AMCInfo.as_of).label('as_on_date')).join(AMC, AMC.sebi_nr==AMCInfo.sebi_nr).filter(AMC.is_active==1).filter(AMCInfo.is_discretionary_active == 1).filter(AMCInfo.total_client > 0).group_by(AMCInfo.sebi_nr, AMC.name).filter(AMCInfo.as_of<= as_on_date).order_by(AMC.name).all()

    
    for sql_amc in sql_amcs:
        sebi_nr = sql_amc.sebi_nr
        obj = get_amc(db_session, sql_amc.as_on_date, sebi_nr)
        total_data[obj["sebi_nr"]] = obj

    overview = list()
    for sebi_nr, item in total_data.items():
        obj = dict()
        obj["name"] = item["name"]
        obj["url_name"] = str(item["name"]).replace(" ", "-") if item["name"] else None
        obj["logo"] = item["logo"]
        obj["sebi_nr"] = item["sebi_nr"]
        overview.append(obj)
    
    logging.warning('Getting Fund Manager Export')
    #get Fund manager detail    
    fundmanager_detail = dict()
    fundmanager_overview = list()
    
    sql_fundmanagers = get_fundmanager_list(db_session, as_on_date, None, None, None)
    for fundmanagers in sql_fundmanagers:
        fm_detail = dict()
        fm_overview = dict()

        fm_data = get_fund_manager_info_by_code(db_session, fundmanagers.FundManager_Code, True, '')
        if fm_data:
            fm_overview['fund_manager_url_name'] = F'{str(fm_data["fund_manager_name"]).strip().replace(" ", "-")}-{str(fm_data["fund_manager_amc"]).strip().replace(" ", "-")}' if fm_data["fund_manager_name"] else None

            #TODO get below hardcoded api path from config
            fm_overview['fund_manager_image'] = F"https://api.finalyca.com/{fm_data['fund_manager_image']}" if fm_data["fund_manager_image"] else None
            fm_overview['fund_manager_name'] = fm_data["fund_manager_name"]
            fm_overview['fund_manager_code'] = fundmanagers.FundManager_Code
            fm_overview['product_name'] = fundmanagers.Product_Name

            fm_data.update(fm_overview)
            
            fm_details = get_fund_manager_details(db_session, fundmanagers.FundManager_Code, 1)
            fm_data['fund_manager_overview_data'] = fm_details

            fundmanager_detail[fundmanagers.FundManager_Code] = fm_data

            # fundmanager_detail.append(fm_detail)
            fundmanager_overview.append(fm_overview)

    
    return overview , total_data, fundmanager_detail, fundmanager_overview

def get_sebi_website_export_last_state(db_session):
    last_date = get_last_day_for_prev_month(now.month, now.year)

    total_data = dict()

    sql_amcs = db_session.query( AMCInfo.sebi_nr, AMC.name, func.max(AMCInfo.as_of).label('as_on_date') ).join(AMC, AMC.sebi_nr==AMCInfo.sebi_nr).filter(AMCInfo.is_discretionary_active == 1).filter(AMCInfo.total_client > 0).group_by(AMCInfo.sebi_nr, AMC.name).order_by(AMC.name).all()

    for sql_obj in sql_amcs:
        sebi_nr = sql_obj.sebi_nr
        as_on_date = sql_obj.as_on_date
        
        diff = last_date - as_on_date
        # if diff is more than 6 months (half of 365 days in a year)
        if diff.days > 183:
            continue

        obj = get_amc(db_session, as_on_date, sebi_nr)
        total_data[obj["sebi_nr"]] = obj

    overview = list()
    for sebi_nr, item in total_data.items():
        obj = dict()
        obj["name"] = item["name"]
        obj["logo"] = item["logo"]
        obj["sebi_nr"] = item["sebi_nr"]
        overview.append(obj)
    
    return overview , total_data



def prepare_export(export_dir_path, as_on_dates):
    db_session = get_sebi_database_scoped_session()

    # some = get_amc(db_session, date(2022, 6, 30), "INP000003674")

    overview_data_path = os.path.join(export_dir_path, 'sebi_overview.json')
    total_data_path = os.path.join(export_dir_path, 'sebi_detail.json')
    fund_manager_detail_data_path = os.path.join(export_dir_path, 'fund_manager_detail.json')
    fund_manager_overview_data_path = os.path.join(export_dir_path, 'fund_manager_overview.json')

    logging.warning('Getting AMC Export')

    if as_on_dates:
        overview, data, fundmanager_detail, fundmanager_overview = get_sebi_website_export(db_session, as_on_dates)
    else:
        overview, data = get_sebi_website_export_last_state(db_session)
        fundmanager_detail = []
        fundmanager_overview = []
        
    with open(overview_data_path, "w") as outfile:
        json.dump(overview, outfile, cls=FinalycaJSONEncoder)

    with open(total_data_path, "w") as outfile:
        json.dump(data, outfile, cls=FinalycaJSONEncoder)

    with open(fund_manager_detail_data_path, "w") as outfile:
        json.dump(fundmanager_detail, outfile, cls=FinalycaJSONEncoder)

    with open(fund_manager_overview_data_path, "w") as outfile:
        json.dump(fundmanager_overview, outfile, cls=FinalycaJSONEncoder)

    attachements = list()
    attachements.append(overview_data_path)
    attachements.append(total_data_path)
    attachements.append(fund_manager_detail_data_path)
    attachements.append(fund_manager_overview_data_path)

    return attachements

if __name__ == '__main__':
    now = date.today()
    as_on_date = get_last_day_for_prev_month(now.month, now.year)
    as_on_date = date(2023, 12, 31)
    
    target_emails = list()
    target_emails.append("vijay.shah@finalyca.com")
    target_emails.append("sachin.jaiswal@finalyca.com")

    export_path = "..\prelogin_next\data"
    # export_path = "../"

    attachements = prepare_export(export_path, as_on_date)
    # attachements = prepare_export(export_path, None)