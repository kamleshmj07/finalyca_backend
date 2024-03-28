from io import BytesIO
from operator import and_
from datetime import date, timedelta
from datetime import datetime as dt
import os
from utils.finalyca_store import *
from fin_models.masters_models import *
from fin_models.controller_master_models import *
from fin_models.controller_transaction_models import *
import pandas as pd
from sqlalchemy import distinct, func, and_, or_, case
from bizlogic.importer_helper import get_performance_and_nav_movement_mismatch
from jinja2 import Environment, FileSystemLoader, Template
from async_tasks.send_email import send_email_async
from async_tasks.send_sms import send_sms, SMSConfig
from pathlib import Path
from bizlogic.common_helper import get_navbydate
from bizlogic.importer_helper import get_plans_list_product_wise

def export_business_report(db_session, export_dir_path):
    today = dt.today()
    date45 = today + timedelta(days=45)
    date90 = today + timedelta(days=90)
    date6month = today - timedelta(days=180)

    sql_org_usage = db_session.query(Organization.Organization_Id, 
                                     Organization.Organization_Name, 
                                     Organization.License_Expiry_Date, 
                                     (Organization.No_Of_Lite_Licenses + Organization.No_Of_Pro_Licenses).label("Total Licences"),
                                     func.count(distinct(User.User_Id)).label('Total Users Created'), 
                                     func.count(distinct(UserLog.User_Id)).label('Total Users Logged in Once'), 
                                     Organization.Adminuser_Fullname, 
                                     Organization.Adminuser_Email, 
                                     Organization.Adminuser_Mobile, 
                                     Organization.is_api_enabled, 
                                     Organization.api_available_hits, 
                                     func.coalesce(UserType.usertype_name,'NA').label('User Type'))\
                                        .select_from(Organization)\
                                        .join(User, Organization.Organization_Id==User.Organization_Id)\
                                        .join(UserLog, User.User_Id==UserLog.User_Id)\
                                        .join(UserType, UserType.id == Organization.usertype_id, isouter=True)\
                                        .where(Organization.Is_Active == 1, Organization.License_Expiry_Date>date6month)\
                                        .group_by(
                                            Organization.Organization_Id, 
                                            Organization.Organization_Name, 
                                            Organization.License_Expiry_Date, 
                                            Organization.No_Of_Lite_Licenses + Organization.No_Of_Pro_Licenses,
                                            Organization.Adminuser_Fullname, 
                                            Organization.Adminuser_Email, 
                                            Organization.Adminuser_Mobile, 
                                            Organization.is_api_enabled, 
                                            Organization.api_available_hits, 
                                            UserType.usertype_name
                                        ).all()
    org_usage = pd.DataFrame(sql_org_usage)

    sql_org_usage6m = db_session.query(Organization.Organization_Id, 
                                     Organization.Organization_Name, 
                                     Organization.License_Expiry_Date, 
                                     (Organization.No_Of_Lite_Licenses + Organization.No_Of_Pro_Licenses).label("Total Licences"),
                                     func.count(distinct(User.User_Id)).label('Total Users Created'), 
                                     func.count(distinct(UserLog.User_Id)).label('Total Users Logged in Once'), 
                                     Organization.Adminuser_Fullname, 
                                     Organization.Adminuser_Email, 
                                     Organization.Adminuser_Mobile, 
                                     Organization.is_api_enabled, 
                                     Organization.api_available_hits, 
                                     func.coalesce(UserType.usertype_name,'NA').label('User Type'))\
                                        .select_from(Organization)\
                                        .join(User, Organization.Organization_Id==User.Organization_Id)\
                                        .join(UserLog, User.User_Id==UserLog.User_Id)\
                                        .join(UserType, UserType.id == Organization.usertype_id, isouter=True)\
                                        .where(Organization.Is_Active == 1, Organization.License_Expiry_Date<=date6month)\
                                        .group_by(
                                            Organization.Organization_Id, 
                                            Organization.Organization_Name, 
                                            Organization.License_Expiry_Date, 
                                            Organization.No_Of_Lite_Licenses + Organization.No_Of_Pro_Licenses,
                                            Organization.Adminuser_Fullname, 
                                            Organization.Adminuser_Email, 
                                            Organization.Adminuser_Mobile, 
                                            Organization.is_api_enabled, 
                                            Organization.api_available_hits, 
                                            UserType.usertype_name
                                        ).all()
    org_usage6m = pd.DataFrame(sql_org_usage6m)

    sql_user_usage = db_session.query(UserLog.User_Id, 
                                      User.Display_Name, 
                                      User.Access_Level,
                                      User.Contact_Number,
                                      User.Email_Address,
                                      Organization.Organization_Name, 
                                      func.count(UserLog.User_Id).label("Total Login"), 
                                      func.max(UserLog.login_timestamp).label("Last Login"), 
                                      case((Organization.License_Expiry_Date < today, 'Expired'), else_='valid').label('Subscription status'), 
                                      Organization.License_Expiry_Date, User.Is_Active, 
                                      func.coalesce(UserType.usertype_name,'NA').label('User Type'))\
                                        .select_from(UserLog).join(User, User.User_Id==UserLog.User_Id)\
                                        .join(Organization, Organization.Organization_Id==User.Organization_Id)\
                                        .join(UserType, UserType.id == Organization.usertype_id, isouter=True)\
                                        .group_by(UserLog.User_Id, 
                                                  User.Display_Name, 
                                                  User.Access_Level,
                                                  User.Contact_Number,
                                                  User.Email_Address,
                                                  Organization.Organization_Name, 
                                                  User.Is_Active, 
                                                  UserType.usertype_name,
                                                  Organization.License_Expiry_Date
                                        ).all()
    user_usage = pd.DataFrame(sql_user_usage)

    sql_api_usage = db_session.query(API.id, 
                                     API.name, 
                                     Organization.Organization_Name, 
                                     Organization.api_access_level, 
                                     Organization.api_available_hits, 
                                     func.count(APILog.id).label("api_hits_used"), 
                                     API.requested_at, 
                                     User.Display_Name, 
                                     User.Email_Address, 
                                     User.Contact_Number, 
                                     API.is_active)\
                                        .select_from(API)\
                                        .join(User, User.User_Id==API.requested_by)\
                                        .join(Organization, Organization.Organization_Id==API.org_id)\
                                        .outerjoin(APILog, and_(APILog.entity_id == API.id, APILog.entity_type=='api'))\
                                        .group_by(API.id, 
                                                  API.name, 
                                                  Organization.Organization_Name, 
                                                  Organization.api_access_level, 
                                                  Organization.api_available_hits, 
                                                  API.requested_at, 
                                                  User.Display_Name, 
                                                  User.Email_Address, 
                                                  User.Contact_Number, 
                                                  API.is_active
                                        ).all()
    api_usage = pd.DataFrame(sql_api_usage)

     #16 user expiry

    sql_user_expiry_date = db_session.query(Organization.Organization_Name, 
                                            User.Display_Name, 
                                            User.Email_Address, 
                                            User.Contact_Number, 
                                            Organization.License_Expiry_Date, 
                                            func.coalesce(UserType.usertype_name,'NA').label('User Type'))\
                                                .select_from(Organization)\
                                                .join(User, User.Organization_Id == Organization.Organization_Id)\
                                                .join(UserType, UserType.id == Organization.usertype_id, isouter=True)\
                                                .filter(Organization.License_Expiry_Date >= today)\
                                                .filter(Organization.License_Expiry_Date <= date45)\
                                                .all()

    user_expiry_date = pd.DataFrame(sql_user_expiry_date)

    #upcoming API expiry
    sql_api_expiry_date = db_session.query(Organization.Organization_Name, 
                                            User.Display_Name, 
                                            User.Email_Address, 
                                            User.Contact_Number, 
                                            Organization.License_Expiry_Date, 
                                            func.coalesce(UserType.usertype_name,'NA').label('User Type'))\
                                                .select_from(Organization)\
                                                .join(User, User.Organization_Id == Organization.Organization_Id)\
                                                .join(UserType, UserType.id == Organization.usertype_id, isouter=True)\
                                                .filter(Organization.License_Expiry_Date >= today)\
                                                .filter(Organization.License_Expiry_Date <= date90)\
                                                .filter(UserType.id == 3)\
                                                .all()

    api_expiry_date = pd.DataFrame(sql_api_expiry_date)


    #self subscribed
    sql_org_subscription = db_session.query(Organization.Organization_Id, 
                                            Organization.Organization_Name, 
                                            Organization.License_Expiry_Date,  
                                            Organization.No_Of_Lite_Licenses, 
                                            Organization.No_Of_Pro_Licenses, 
                                            Organization.Adminuser_Fullname, 
                                            Organization.Adminuser_Email, 
                                            Organization.Adminuser_Mobile, 
                                            case((Organization.is_self_subscribed == 1, 'Yes'), else_='No').label('Self subscribed'), 
                                            case((or_(Organization.is_payment_pending == None, Organization.is_payment_pending == 0), 'Yes'), else_='No').label('Payment pending'))\
                                                .select_from(Organization)\
                                                .join(User, Organization.Organization_Id==User.Organization_Id)\
                                                .where(Organization.Is_Active == 1)\
                                                .filter(Organization.is_self_subscribed == 1)\
                                                .all()
                    
    org_subscription = pd.DataFrame(sql_org_subscription)

    #subscriber list
    sql_subscribers = db_session.query(Organization.Organization_Name,
                                        User.Display_Name, 
                                        User.Email_Address,
                                        User.Access_Level,
                                        Organization.License_Expiry_Date,
                                        UserType.usertype_name,
                                        case((Organization.License_Expiry_Date < today, 'Expired'), else_='valid').label('Subscription status'), 
                                        User.Is_Active)\
                                            .select_from(Organization)\
                                            .join(User, User.Organization_Id == Organization.Organization_Id)\
                                            .outerjoin(UserType, or_(UserType.id == Organization.usertype_id, UserType.id == 1))\
                                            .filter(Organization.Is_Active == 1, User.Is_Active == 1, User.Email_Address != None, UserType.usertype_name != 'Trial')\
                                            .distinct()\
                                            .order_by(Organization.Organization_Name)\
                                            .all()
    
    org_subscribers = pd.DataFrame(sql_subscribers)


    #new AMC    
    last_30 = today - timedelta(days=30)
    sql_amc = db_session.query(AMC.AMC_Name, 
                                AMC.Created_Date, 
                                func.concat('https://api.finalyca.com/', AMC.AMC_Logo).label('AMC Logo'))\
                                .filter(AMC.Is_Deleted != 1, AMC.Created_Date >= last_30)\
                                .order_by(AMC.Created_Date)\
                                .all()
    
    onboarded_amc = pd.DataFrame(sql_amc)

    sql_trial = db_session.query(
                                      User.Display_Name, 
                                      Organization.Organization_Name, 
                                      case((Organization.License_Expiry_Date < today, 'Expired'), else_='valid').label('Subscription status'), 
                                      Organization.License_Expiry_Date, User.Is_Active, 
                                      func.coalesce(UserType.usertype_name,'NA').label('User Type'))\
                                        .select_from(User)\
                                        .join(Organization, Organization.Organization_Id==User.Organization_Id)\
                                        .join(UserType, UserType.id == Organization.usertype_id, isouter=True)\
                                        .group_by(User.Display_Name, 
                                                  Organization.Organization_Name, 
                                                  User.Is_Active, 
                                                  UserType.usertype_name,
                                                  Organization.License_Expiry_Date
                                        ).all()
    sql_trials = pd.DataFrame(sql_trial)

    sql_prelogin_trial_expired = db_session.query(
                                      User.Display_Name, 
                                      Organization.Organization_Name, 
                                      User.Contact_Number, 
                                      User.Email_Address, 
                                      case((Organization.License_Expiry_Date < today, 'Expired'), else_='valid').label('Subscription status'), 
                                      Organization.License_Expiry_Date, User.Is_Active, 
                                      func.coalesce(UserType.usertype_name,'NA').label('User Type'))\
                                        .select_from(User)\
                                        .join(Organization, Organization.Organization_Id==User.Organization_Id)\
                                        .join(UserType, UserType.id == Organization.usertype_id, isouter=True)\
                                        .filter(Organization.License_Expiry_Date < today,
                                                UserType.id == 10)\
                                        .group_by(User.Display_Name, 
                                                  Organization.Organization_Name, 
                                                  User.Contact_Number, 
                                                  User.Email_Address,
                                                  User.Is_Active, 
                                                  UserType.usertype_name,
                                                  Organization.License_Expiry_Date
                                        ).all()
    prelogin_trial_expired = pd.DataFrame(sql_prelogin_trial_expired)


    # TODO: make a standard directory
    attachements = list()
    output = F"business_report_{today.year}-{today.month}-{today.day}.xlsx"
    file_data_path = os.path.join(export_dir_path, output)
    attachements.append(file_data_path)

    with pd.ExcelWriter(file_data_path) as writer:
        org_usage.to_excel(writer, sheet_name="Organization Report", float_format="%.2f", index=False)
        org_usage6m.to_excel(writer, sheet_name="Expired Org > 6MNTH ", float_format="%.2f", index=False)
        user_usage.to_excel(writer, sheet_name="User Report", float_format="%.2f", index=False)
        api_usage.to_excel(writer, sheet_name="API Report", float_format="%.2f", index=False)        
        user_expiry_date.to_excel(writer, sheet_name="Upcoming user expiry", float_format="%.2f", index=False)
        org_subscription.to_excel(writer, sheet_name="Self subscription", float_format="%.2f", index=False)
        org_subscribers.to_excel(writer, sheet_name="Subscribers", float_format="%.2f", index=False)
        onboarded_amc.to_excel(writer, sheet_name="New AMC on-boarded", float_format="%.2f", index=False)
        sql_trials.to_excel(writer, sheet_name="User Status", float_format="%.2f", index=False)
        prelogin_trial_expired.to_excel(writer, sheet_name="Trial expired - Prelogin", float_format="%.2f", index=False)
        api_expiry_date.to_excel(writer, sheet_name="Upcoming API expiry", float_format="%.2f", index=False)

    return attachements
    

def export_data_report(db_session, export_dir_path):
    today = dt.today()
    first = today.replace(day=1)
    current_month = first - timedelta(days=1)

    first_current = current_month.replace(day=1)
    previous_month = first_current - timedelta(days=1)

    pms_performance_list = list()

    #1 - holding sum not 100%
    holding_sum_data = list()
    sql_holding_fund_wise = db_session.query(
                                            func.max(UnderlyingHoldings.Portfolio_Date), 
                                            Fund.Fund_Id, 
                                            Fund.Fund_Code, 
                                            Fund.Fund_Name, 
                                            Product.Product_Name, 
                                            Product.Product_Id)\
                                                .select_from(Fund)\
                                                .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                                .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                .join(Product,Product.Product_Id == PlanProductMapping.Product_Id)\
                                                .join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id)\
                                                .filter(MFSecurity.Status_Id == 1)\
                                                .filter(PlanProductMapping.Is_Deleted != 1)\
                                                .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                                .group_by(Fund.Fund_Id, 
                                                          Fund.Fund_Code, 
                                                          Fund.Fund_Name, 
                                                          Product.Product_Name, 
                                                          Product.Product_Id)

    if sql_holding_fund_wise:

        for holding in sql_holding_fund_wise:
            sql_holding_sum = db_session.query(func.sum(UnderlyingHoldings.Percentage_to_AUM))\
                                                    .filter(UnderlyingHoldings.Portfolio_Date == holding[0])\
                                                    .filter(UnderlyingHoldings.Fund_Id == holding.Fund_Id)\
                                                    .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                                    .scalar()

            if sql_holding_sum:
                if sql_holding_sum > 101 or sql_holding_sum < 99:
                    data = dict()
                    data["Fund Id"] = holding.Fund_Id
                    data["Fund Code"] = holding.Fund_Code
                    data["Fund Name"] = holding.Fund_Name
                    data["Product Id"] = holding.Product_Id
                    data["Product Name"] = holding.Product_Name
                    data["Portfolio Date"] = holding[0]
                    data["Total holding Percent"] = sql_holding_sum

                    holding_sum_data.append(data)
                    
    
    holding_sum = pd.DataFrame(holding_sum_data)                  


    #2 Factsheet count
    api_factsheet_count = db_session.query(Plans.Plan_Id, 
                                           Plans.Plan_Name, 
                                           Plans.Plan_Code, 
                                           Product.Product_Name, 
                                           MFSecurity.MF_Security_OpenDate, 
                                           func.min(FactSheet.TransactionDate).label('First Factsheet as on'), 
                                           func.max(FactSheet.TransactionDate).label('Last Factsheet as on'), 
                                           func.count(Plans.Plan_Id).label('factsheet Count'), 
                                           case((or_(Product.Product_Id == 4,Product.Product_Id == 5), func.datediff(text('Month'), func.min(FactSheet.TransactionDate), func.max(FactSheet.TransactionDate)  ) + 1  ), else_=(func.datediff(text('Day'), func.min(FactSheet.TransactionDate), func.max(FactSheet.TransactionDate)) - (func.datediff(text('Week'), func.min(FactSheet.TransactionDate), func.max(FactSheet.TransactionDate)) * 2) )).label('Expected factsheet Count'))\
                                                .select_from(Plans)\
                                                .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                                .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                .outerjoin(FactSheet, FactSheet.Plan_Id == Plans.Plan_Id)\
                                                .filter(MFSecurity.Status_Id == 1)\
                                                .filter(Plans.Is_Deleted != 1)\
                                                .filter(PlanProductMapping.Is_Deleted != 1)\
                                                .filter(FactSheet.Is_Deleted != 1)\
                                                .filter(FactSheet.TransactionDate <= current_month)\
                                                .group_by(Plans.Plan_Id, 
                                                          Plans.Plan_Code, 
                                                          Plans.Plan_Name, 
                                                          Product.Product_Name, 
                                                          MFSecurity.MF_Security_OpenDate, 
                                                          Product.Product_Id)\
                                                .all()

    factsheet_count = pd.DataFrame(api_factsheet_count)

    #3 Fund NAV count
    api_fund_nav_count = db_session.query(Plans.Plan_Id, 
                                          Plans.Plan_Name, 
                                          Plans.Plan_Code, 
                                          Product.Product_Name, 
                                          MFSecurity.MF_Security_OpenDate, 
                                          func.min(NAV.NAV_Date).label('First NAV as on'), 
                                          func.max(NAV.NAV_Date).label('Last NAV as on'), 
                                          func.count(Plans.Plan_Id).label('NAV Count'), 
                                          (func.datediff(text('Day'), func.min(NAV.NAV_Date), func.max(NAV.NAV_Date)) + 1).label('Expected NAV Count'))\
                                                .select_from(Plans)\
                                                .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                                .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                .outerjoin(NAV, NAV.Plan_Id == Plans.Plan_Id)\
                                                .filter(NAV.NAV_Type == 'P')\
                                                .filter(NAV.Is_Deleted != 1)\
                                                .filter(MFSecurity.Status_Id == 1)\
                                                .filter(Plans.Is_Deleted != 1)\
                                                .filter(PlanProductMapping.Is_Deleted != 1)\
                                                .filter(NAV.NAV_Date <= current_month)\
                                                .group_by(Plans.Plan_Id, 
                                                          Plans.Plan_Code, 
                                                          Plans.Plan_Name, 
                                                          Product.Product_Name, 
                                                          MFSecurity.MF_Security_OpenDate)\
                                                .all()

    fund_nav_count = pd.DataFrame(api_fund_nav_count)


    #4 Fund manager not available
    api_fundmanager_na = db_session.query(Fund.Fund_Id, 
                                          Fund.Fund_Code, 
                                          Fund.Fund_Name, 
                                          Product.Product_Name, 
                                          func.min(MFSecurity.MF_Security_OpenDate).label('Inception date'))\
                                            .select_from(Fund)\
                                            .outerjoin(FundManager, FundManager.Fund_Id == Fund.Fund_Id)\
                                            .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                            .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                            .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                            .filter(FundManager.FundManager_Id == None)\
                                            .filter(MFSecurity.Status_Id == 1)\
                                            .filter(Plans.Is_Deleted != 1)\
                                            .filter(PlanProductMapping.Is_Deleted != 1)\
                                            .group_by(Fund.Fund_Id, 
                                                      Fund.Fund_Name, 
                                                      Fund.Fund_Code, 
                                                      Product.Product_Name)\
                                            .all()

    fundmanager_missing = pd.DataFrame(api_fundmanager_na)

        #5 Benchmark not available
    api_benchmark_na = db_session.query(Plans.Plan_Id, 
                                        Plans.Plan_Code, 
                                        Plans.Plan_Name, 
                                        Product.Product_Name)\
                                            .select_from(Plans)\
                                            .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                            .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                            .outerjoin(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id)\
                                            .filter(MFSecurity.Status_Id == 1)\
                                            .filter(Plans.Is_Deleted != 1)\
                                            .filter(PlanProductMapping.Is_Deleted != 1)\
                                            .filter(BenchmarkIndices.BenchmarkIndices_Id == None)\
                                            .all()

    benchmark_not_available = pd.DataFrame(api_benchmark_na)


    #6 Market cap is blank

    api_marketcap = db_session.query(HoldingSecurity.HoldingSecurity_Id, 
                                     HoldingSecurity.HoldingSecurity_Name, 
                                     HoldingSecurity.ISIN_Code, 
                                     HoldingSecurity.MarketCap)\
                                        .select_from(Plans)\
                                        .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                        .join(AssetClass, AssetClass.AssetClass_Id == MFSecurity.AssetClass_Id)\
                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                        .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                        .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                        .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                                        .join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id)\
                                        .join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id)\
                                        .filter(Fund.Is_Deleted != 1)\
                                        .filter(MFSecurity.Status_Id == 1)\
                                        .filter(AMC.Is_Deleted != 1)\
                                        .filter(HoldingSecurity.active == 1)\
                                        .filter(or_(func.coalesce(HoldingSecurity.MarketCap, '') == '',func.coalesce(HoldingSecurity.MarketCap,'') == '-'))\
                                        .filter(func.coalesce(UnderlyingHoldings.Asset_Class, case((HoldingSecurity.HoldingSecurity_Type == 'Equity', HoldingSecurity.HoldingSecurity_Type), else_=None)) == 'Equity')\
                                        .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                        .distinct()\
                                        .order_by(HoldingSecurity.HoldingSecurity_Name)\
                                        .all()

    marketcap_not_available = pd.DataFrame(api_marketcap)


    #7 Sector id is blank
    api_sector_id = db_session.query(HoldingSecurity.HoldingSecurity_Id, 
                                     HoldingSecurity.HoldingSecurity_Name, 
                                     HoldingSecurity.ISIN_Code, 
                                     HoldingSecurity.Sector_Id)\
                                        .select_from(Plans)\
                                        .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                        .join(AssetClass, AssetClass.AssetClass_Id == MFSecurity.AssetClass_Id)\
                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                        .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                        .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                        .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                                        .join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id)\
                                        .join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id)\
                                        .filter(Fund.Is_Deleted != 1)\
                                        .filter(MFSecurity.Status_Id == 1)\
                                        .filter(AMC.Is_Deleted != 1)\
                                        .filter(HoldingSecurity.active == 1)\
                                        .filter(or_(func.coalesce(HoldingSecurity.Sector_Id, '') == '',
                                                    func.coalesce(HoldingSecurity.Sector_Id,'') == '-'))\
                                        .filter(func.coalesce(UnderlyingHoldings.Asset_Class, case((HoldingSecurity.HoldingSecurity_Type == 'Equity', HoldingSecurity.HoldingSecurity_Type), else_=None)) == 'Equity')\
                                        .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                        .distinct()\
                                        .order_by(HoldingSecurity.HoldingSecurity_Name)\
                                        .all()

    sectorid_not_available = pd.DataFrame(api_sector_id)

    #TODO change below logic optimise it with 10% movement  
    #8 PMS portfolio returns for last 2 months
    
    sql_pms_plans = db_session.query(Plans.Plan_Id, 
                                     Plans.Plan_Name, 
                                     Plans.Plan_Code, 
                                     MFSecurity.Fund_Id, 
                                     Fund.HideAttribution, 
                                     BenchmarkIndices.Attribution_Flag)\
                                        .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                        .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                                        .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                        .join(Classification, MFSecurity.Classification_Id == Classification.Classification_Id)\
                                        .join(NAV, NAV.Plan_Id == Plans.Plan_Id)\
                                        .join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id)\
                                        .filter(MFSecurity.Status_Id == 1)\
                                        .filter(NAV.NAV_Type == 'P')\
                                        .filter(PlanProductMapping.Is_Deleted != 1)\
                                        .filter(PlanProductMapping.Product_Id == 4)\
                                        .filter(Plans.Is_Deleted != 1)\
                                        .distinct()\
                                        .all()
    
    if sql_pms_plans:        
        for pms_plans in sql_pms_plans:
            perf_data = dict()
            
            end_nav = get_navbydate(db_session, pms_plans.Plan_Id, current_month)   
            start_nav = get_navbydate(db_session, pms_plans.Plan_Id, previous_month)
            if start_nav and end_nav:
                perf_data['Plan Id'] = pms_plans.Plan_Id
                perf_data['Plan Name'] = pms_plans.Plan_Name
                perf_data['Plan Code'] = pms_plans.Plan_Code
                perf_data['Porfolio Returns'] = ((end_nav - start_nav) / start_nav * 100)
                perf_data['Current Date'] = current_month
                perf_data['Previous Date'] = previous_month
                perf_data['Previous NAV'] = start_nav
                perf_data['Current NAV'] = end_nav

                pms_performance_list.append(perf_data)

    pms_performance_list = sorted(pms_performance_list, key=lambda d: d['Porfolio Returns'], reverse=True)

    pms_performance_data = pd.DataFrame(pms_performance_list)


    #9 List of plans where nav change is more than 3% for last month - daily
    plans_daily_movement = list()
    daily_percent = 3

    sql_plans_movement = db_session.query(Plans.Plan_Id, 
                                          Plans.Plan_Name, 
                                          NAV.NAV_Date.label('Date'), 
                                          NAV.NAV, 
                                          func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date).label('prev_nav'),
                                          (((((NAV.NAV - func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date)) / func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date))) * 100)).label('Portfolio_Returns'), 
                                          Product.Product_Name)\
                                                .select_from(Plans)\
                                                .join(NAV,NAV.Plan_Id == Plans.Plan_Id)\
                                                .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                .filter(NAV.NAV_Type=='P')\
                                                .filter(Plans.Is_Deleted != 1,NAV.Is_Deleted != 1)\
                                                .filter(NAV.NAV_Date >= previous_month)\
                                                .filter(NAV.NAV_Date <= current_month)\
                                                .filter(PlanProductMapping.Is_Deleted != 1)\
                                                .all()

    if sql_plans_movement:
        for plans_movement in sql_plans_movement:
            if plans_movement[5]:
                if plans_movement[5] >= daily_percent or plans_movement[5] <= -daily_percent:
                    data = dict()
                    data["Plan id"] = plans_movement.Plan_Id
                    data["Plan Name"] = plans_movement.Plan_Name
                    data["Nav Date"] = plans_movement[2]
                    data["Current NAV"] = plans_movement.NAV
                    data["Previous NAV"] = plans_movement[4]
                    data["Increase in Percent"] = plans_movement[5]
                    data["Product"] = plans_movement.Product_Name

                    plans_daily_movement.append(data)

    plans_daily_movement = sorted(plans_daily_movement, key=lambda d: d['Increase in Percent'], reverse=True)
    daily_navmovement_data = pd.DataFrame(plans_daily_movement)


    #10 List of plans where nav change is more than 10% for last month - monthly
    plans_monthly_movement = list()
    monthly_percent = 10
    all_ends = pd.date_range(start='1990-01-31', end=dt.today(), freq='M')
    
    all_dates=list()
    for dt1 in all_ends:
       all_dates.append(dt1.to_pydatetime().date())

    sql_plans_monthly_movement = db_session.query(Plans.Plan_Id, 
                                                  Plans.Plan_Name, 
                                                  NAV.NAV_Date.label('Date'), 
                                                  NAV.NAV, 
                                                  func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date).label('prev_nav'),
                                                  (((((NAV.NAV - func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date)) / func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date))) * 100)).label('Portfolio_Returns'),
                                                  Product.Product_Name)\
                                                        .select_from(Plans)\
                                                        .join(MFSecurity, MFSecurity.MF_Security_Id==Plans.MF_Security_Id)\
                                                        .join(NAV, and_(NAV.Plan_Id==Plans.Plan_Id, NAV.NAV_Type=='P'))\
                                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                        .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                        .filter(NAV.NAV_Type=='P')\
                                                        .filter(Plans.Is_Deleted != 1, NAV.Is_Deleted != 1, PlanProductMapping.Is_Deleted != 1)\
                                                        .filter(MFSecurity.Status_Id==1)\
                                                        .filter(NAV.NAV_Date.in_(all_dates))\
                                                        .all()
 
    if sql_plans_monthly_movement:
        for plans_movement in sql_plans_monthly_movement:
            if plans_movement[5]:
                if plans_movement[5] >= monthly_percent or plans_movement[5] <= -monthly_percent:
                    data = dict()
                    data["Plan id"] = plans_movement.Plan_Id
                    data["Plan Name"] = plans_movement.Plan_Name
                    data["Nav Date"] = plans_movement[2]
                    data["Current NAV"] = plans_movement.NAV
                    data["Previous NAV"] = plans_movement[4]
                    data["Increase in Percent"] = plans_movement[5]
                    data["Product"] = plans_movement.Product_Name

                    plans_monthly_movement.append(data)

    plans_monthly_movement = sorted(plans_monthly_movement, key=lambda d: d['Increase in Percent'], reverse=True)
    monthly_navmovement_data = pd.DataFrame(plans_monthly_movement)

    #performance and NAV movement mismatch
    plans_nav_performance_mismatch = list()
    
    plans_nav_performance_mismatch = get_performance_and_nav_movement_mismatch(db_session, previous_month.date(), current_month.date())

    plans_nav_performance_mismatch = sorted(plans_nav_performance_mismatch, key=lambda d: d['Difference'], reverse=True)
    plans_nav_performance_mismatch_data = pd.DataFrame(plans_nav_performance_mismatch)

    #TODO optimize below code 
    #11 Sector list where change is more thn 10%
    sector_movement_list = list()
    if sql_pms_plans:        
        for pms_plans in sql_pms_plans:
            perf_data = dict()
            previous_portfolio_date = None
            last_portfolio_date = None

            last_portfolio_date = db_session.query(func.max(UnderlyingHoldings.Portfolio_Date))\
                                                        .join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                                        .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                        .filter(MFSecurity.Status_Id == 1)\
                                                        .filter(Plans.Is_Deleted != 1)\
                                                        .filter(MFSecurity.Is_Deleted != 1)\
                                                        .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                                        .filter(Plans.Plan_Id == pms_plans.Plan_Id)\
                                                        .scalar()

            if last_portfolio_date != None and last_portfolio_date != 'null':
                previous_portfolio_date = db_session.query(func.max(UnderlyingHoldings.Portfolio_Date))\
                                                                    .join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                                    .filter(MFSecurity.Status_Id == 1)\
                                                                    .filter(Plans.Is_Deleted != 1)\
                                                                    .filter(MFSecurity.Is_Deleted != 1)\
                                                                    .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                                                    .filter(Plans.Plan_Id == pms_plans.Plan_Id)\
                                                                    .filter(UnderlyingHoldings.Portfolio_Date != last_portfolio_date)\
                                                                    .scalar()
            
            if previous_portfolio_date and last_portfolio_date:
                sql_sectors_data = db_session.query(PortfolioSectors.Sector_Code, 
                                                    PortfolioSectors.Sector_Name, 
                                                    PortfolioSectors.Plan_Id, 
                                                    Plans.Plan_Name, 
                                                    func.sum(PortfolioSectors.Percentage_To_AUM).label('current_perc'), 
                                                    func.lag(func.sum(PortfolioSectors.Percentage_To_AUM)).over(partition_by=(PortfolioSectors.Plan_Id, PortfolioSectors.Sector_Name),order_by=PortfolioSectors.Portfolio_Date).label('prev_perc'), 
                                                    (func.sum(PortfolioSectors.Percentage_To_AUM) - func.lag(func.sum(PortfolioSectors.Percentage_To_AUM)).over(partition_by=(PortfolioSectors.Plan_Id, PortfolioSectors.Sector_Name),order_by=PortfolioSectors.Portfolio_Date)).label('diff'), 
                                                    PortfolioSectors.Portfolio_Date)\
                                                        .select_from(PortfolioSectors)\
                                                        .join(Sector, Sector.Sector_Code==PortfolioSectors.Sector_Code)\
                                                        .join(Plans, Plans.Plan_Id == PortfolioSectors.Plan_Id)\
                                                        .filter(PortfolioSectors.Is_Deleted != 1)\
                                                        .filter(PortfolioSectors.Plan_Id == pms_plans.Plan_Id)\
                                                        .filter(or_(PortfolioSectors.Portfolio_Date == last_portfolio_date, 
                                                                    PortfolioSectors.Portfolio_Date == previous_portfolio_date))\
                                                        .group_by(PortfolioSectors.Sector_Code, 
                                                                  PortfolioSectors.Sector_Name, 
                                                                  PortfolioSectors.Portfolio_Date, 
                                                                  PortfolioSectors.Plan_Id, 
                                                                  Plans.Plan_Name)\
                                                        .all()

                if sql_sectors_data:
                    for sector_data in sql_sectors_data:
                        if sector_data[6]:
                            if sector_data[6] > 10 or  sector_data[6] < -10:
                                data = dict()
                                data["Plan Id"] = sector_data.Plan_Id
                                data["Plan Name"] = sector_data.Plan_Name
                                data["Sector Code"] = sector_data.Sector_Code
                                data["Sector Name"] = sector_data.Sector_Name
                                data["Portfolio Date"] = sector_data.Portfolio_Date
                                data["Current Weightage"] = sector_data[4]
                                data["Pervious Weightage"] = sector_data[5]
                                data["Difference"] = sector_data[6]

                                sector_movement_list.append(data)
    
    sector_movement_list = sorted(sector_movement_list, key=lambda d: d['Difference'], reverse=True)
    sector_movement_data = pd.DataFrame(sector_movement_list)

    #12 attribution disabled
    # attribution_disabled_list = list()
    # if sql_pms_plans:        
    #     for pms_plans in sql_pms_plans:
            
    #         perf_data = dict()

    #         attribution_fromdate = None
    #         attribution_todate = None
    #         factsheet_fromdate = None
    #         factsheet_todate = None
    #         attribution_flag = None
    #         hide_attribution = None
    #         attribution_visible = True
    #         remark = None
            
    #         attribution_todate = db_session.query(func.max(UnderlyingHoldings.Portfolio_Date)).join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(MFSecurity.Status_Id == 1).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(UnderlyingHoldings.Is_Deleted != 1).filter(Plans.Plan_Id == pms_plans.Plan_Id).filter(UnderlyingHoldings.Portfolio_Date <= current_month).scalar()
            
    #         attribution_fromdate = db_session.query(func.max(UnderlyingHoldings.Portfolio_Date)).join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(MFSecurity.Status_Id == 1).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(UnderlyingHoldings.Is_Deleted != 1).filter(Plans.Plan_Id == pms_plans.Plan_Id).filter(UnderlyingHoldings.Portfolio_Date <= previous_month).scalar()

    #         if pms_plans.Attribution_Flag == False:
    #             remark='Attribution is disabled for this benchmark.'
    #             attribution_visible = False

    #         if pms_plans.HideAttribution == True:
    #             remark='Attribution is disabled for this fund.'
    #             attribution_visible = False


    #         if attribution_todate==None and remark == None:
    #             remark = 'Holdings not available.'
    #             attribution_visible = False
            
    #         if attribution_fromdate==None and remark==None:
    #             remark = 'Holdings not avaiable for previous month - ' + previous_month.strftime("%B %d, %Y")
    #             attribution_visible = False

    #         factsheet_fromdate = db_session.query(FactSheet.TransactionDate).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.Plan_Id == pms_plans.Plan_Id).filter(FactSheet.TransactionDate == attribution_fromdate).scalar()

    #         factsheet_todate = db_session.query(FactSheet.TransactionDate).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.Plan_Id == pms_plans.Plan_Id).filter(FactSheet.TransactionDate == attribution_todate).scalar()

    #         if factsheet_fromdate==None and remark==None:
    #             remark = 'Factsheet not avaiable for previous month - '+ attribution_fromdate.strftime("%B %d, %Y")
    #             attribution_visible = False
            

    #         if remark==None:
    #             if pms_plans.Attribution_Flag:
    #                 attribution_flag = pms_plans.Attribution_Flag

    #             if pms_plans.HideAttribution:
    #                 hide_attribution = pms_plans.HideAttribution
                
    #             if attribution_flag != False and attribution_fromdate>=factsheet_fromdate and attribution_todate>=factsheet_todate and hide_attribution != True:
    #                 attribution_visible = True
            
    #         if attribution_visible == False:
    #             data = dict()
    #             data["Plan Id"] = pms_plans.Plan_Id
    #             data["Plan Name"] = pms_plans.Plan_Name
    #             data["Attribution Visible"] = 'No'
    #             data["Reason"] = remark

    #             attribution_disabled_list.append(data)
    
    # benchmark_not_available_data = pd.DataFrame(attribution_disabled_list)
    
    #13 co_code duplicate in holdingsecurity
    sql_co_code_duplicate = db_session.query(HoldingSecurity.Co_Code, 
                                             func.count(HoldingSecurity.Co_Code).label('Count'))\
                                                .filter(HoldingSecurity.Is_Deleted != 1, 
                                                        HoldingSecurity.active != 0)\
                                                .group_by(HoldingSecurity.Co_Code)\
                                                .having(func.count(HoldingSecurity.Co_Code) > 1)\
                                                .all()

    co_code_duplicate_data = pd.DataFrame(sql_co_code_duplicate)

    #14 isin_code duplicate in holdingsecurity
    sql_isin_code_duplicate = db_session.query(HoldingSecurity.ISIN_Code, 
                                               func.count(HoldingSecurity.ISIN_Code).label('Count'))\
                                                    .filter(HoldingSecurity.Is_Deleted != 1)\
                                                    .group_by(HoldingSecurity.ISIN_Code)\
                                                    .having(func.count(HoldingSecurity.ISIN_Code) > 1)\
                                                    .all()

    isin_code_duplicate_data = pd.DataFrame(sql_isin_code_duplicate)


    #15 Asset Class mismatch
    sql_assetclass_mismatch = db_session.query(HoldingSecurity.HoldingSecurity_Name, 
                                               HoldingSecurity.ISIN_Code, 
                                               HoldingSecurity.Asset_Class, 
                                               HoldingSecurity.Instrument_Type, 
                                               HoldingSecurity.HoldingSecurity_Type, 
                                               HoldingSecurity.Issuer_Name, 
                                               HoldingSecurity.Co_Code)\
                                                    .filter(HoldingSecurity.Is_Deleted != 1)\
                                                    .filter(HoldingSecurity.ISIN_Code.like('INF%'))\
                                                    .filter(HoldingSecurity.Instrument_Type != 'Mutual Funds')\
                                                    .filter(HoldingSecurity.Asset_Class != 'Mutual Fund')\
                                                    .all()

    assetclass_mismatch_data = pd.DataFrame(sql_assetclass_mismatch)

    #Sector assigned as others
    sql_sector_others = db_session.query(HoldingSecurity.HoldingSecurity_Name, 
                                         HoldingSecurity.ISIN_Code, 
                                         HoldingSecurity.Asset_Class, 
                                         Sector.Sector_Name, 
                                         HoldingSecurity.Instrument_Type, 
                                         HoldingSecurity.HoldingSecurity_Type, 
                                         HoldingSecurity.Issuer_Name, 
                                         HoldingSecurity.Co_Code)\
                                            .join(Sector, HoldingSecurity.Sector_Id == Sector.Sector_Id, isouter=True)\
                                            .filter(HoldingSecurity.Is_Deleted != 1)\
                                            .filter(Sector.Sector_Name == 'Others')\
                                            .filter(HoldingSecurity.Asset_Class == 'Equity')\
                                            .all()

    sector_others_data = pd.DataFrame(sql_sector_others)

    #Expense ratio blank
    mf_scheme_data = get_plans_list_product_wise(db_session, 1, current_month, None)
    df_scheme_data = pd.DataFrame(mf_scheme_data)
    df_scheme_data['ExpenseRatio'] = df_scheme_data['ExpenseRatio'].apply(pd.to_numeric, downcast='float')
    df_exp_missing = df_scheme_data.loc[(df_scheme_data["ExpenseRatio"] == 0) & (df_scheme_data["FundType_Name"] == 'Open Ended')]

    #Holding security type null
    sql_null_security_type = db_session.query(HoldingSecurity.HoldingSecurity_Name, 
                                              HoldingSecurity.ISIN_Code, 
                                              HoldingSecurity.Asset_Class, 
                                              Sector.Sector_Name, 
                                              HoldingSecurity.Instrument_Type, 
                                              HoldingSecurity.HoldingSecurity_Type, 
                                              HoldingSecurity.Issuer_Name, 
                                              HoldingSecurity.Co_Code)\
                                                .select_from(HoldingSecurity)\
                                                .join(Sector, HoldingSecurity.Sector_Id == Sector.Sector_Id)\
                                                .filter(HoldingSecurity.Is_Deleted != 1)\
                                                .filter(HoldingSecurity.Asset_Class == 'Equity')\
                                                .filter(HoldingSecurity.HoldingSecurity_Type == None)\
                                                .all()

    null_security_type_data = pd.DataFrame(sql_null_security_type)    

    #check AUM movement
    sql_aum_data = db_session.query(Plans.Plan_Name, 
                                    Plans.Plan_Id, 
                                    Product.Product_Code, 
                                    FactSheet.TransactionDate, 
                                    FactSheet.NetAssets_Rs_Cr, 
                                    func.lag(FactSheet.NetAssets_Rs_Cr).over(partition_by=(Plans.Plan_Name,  Plans.Plan_Id), order_by=FactSheet.TransactionDate).label('prev_aum'), 
                                    ((FactSheet.NetAssets_Rs_Cr * 100/func.lag(FactSheet.NetAssets_Rs_Cr).over(partition_by=(Plans.Plan_Name,  Plans.Plan_Id), order_by=FactSheet.TransactionDate))-100).label('aum_perc_movement'))\
                                        .select_from(Fund)\
                                        .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                        .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                        .join(PlanProductMapping, Plans.Plan_Id == PlanProductMapping.Plan_Id)\
                                        .join(FactSheet, Plans.Plan_Id == FactSheet.Plan_Id)\
                                        .join(Product, PlanProductMapping.Product_Id == Product.Product_Id)\
                                        .filter(PlanProductMapping.Product_Id.in_([4,5]), 
                                                PlanProductMapping.Is_Deleted != 1, 
                                                FactSheet.Is_Deleted != 1, 
                                                Fund.Is_Deleted != 1, 
                                                Plans.Is_Deleted != 1, 
                                                FactSheet.Is_Deleted != 1, 
                                                MFSecurity.Is_Deleted != 1, 
                                                MFSecurity.Status_Id == 1, 
                                                FactSheet.NetAssets_Rs_Cr != None, 
                                                FactSheet.TransactionDate.in_([previous_month, current_month]))\
                                        .all()

    # if sql_aum_data:
    aum_movement_list = list()
    if sql_aum_data:
        for aum_movement in sql_aum_data:
            if aum_movement[6]:
                if aum_movement[6] >= 10 or aum_movement[6] <= -10:
                    data = dict()
                    data["Plan Id"] = aum_movement.Plan_Id
                    data["Plan Name"] = aum_movement.Plan_Name
                    data["Product Name"] = aum_movement.Product_Code
                    data["Current Date"] = current_month
                    data["Previous Date"] = previous_month
                    data["Current AUM"] = aum_movement.NetAssets_Rs_Cr
                    data["Previous AUM"] = aum_movement[5]                    
                    data["Difference in %"] = aum_movement[6]

                    aum_movement_list.append(data)

    aum_movement_list = sorted(aum_movement_list, key=lambda d: d['Difference in %'], reverse=True)
    aum_movement_data = pd.DataFrame(aum_movement_list)

    # TODO: make a standard directory
    attachements = list()
    output = F"healthcheck_report_{today.year}-{today.month}-{today.day}.xlsx"
    file_data_path = os.path.join(export_dir_path, output)
    attachements.append(file_data_path)

    with pd.ExcelWriter(file_data_path) as writer:        
        holding_sum.to_excel(writer, sheet_name="Holding not equal to 100", float_format="%.2f", index=False)
        factsheet_count.to_excel(writer, sheet_name="Factsheet Count", float_format="%.2f", index=False)
        fund_nav_count.to_excel(writer, sheet_name="Fund NAV Count", float_format="%.2f", index=False)
        fundmanager_missing.to_excel(writer, sheet_name="Fund Manager missing", float_format="%.2f", index=False)
        benchmark_not_available.to_excel(writer, sheet_name="Benchmark not available", float_format="%.2f", index=False)
        marketcap_not_available.to_excel(writer, sheet_name="MarketCap not available", float_format="%.2f", index=False)
        sectorid_not_available.to_excel(writer, sheet_name="Sector not available", float_format="%.2f", index=False)
        pms_performance_data.to_excel(writer, sheet_name="Portfolio Returns by NAV - PMS", float_format="%.2f", index=False)
        daily_navmovement_data.to_excel(writer, sheet_name="NAV movement > 3% Daily", float_format="%.2f", index=False)
        monthly_navmovement_data.to_excel(writer, sheet_name="NAV movement > 10% Monthly", float_format="%.2f", index=False)
        plans_nav_performance_mismatch_data.to_excel(writer, sheet_name="Performance and NAV Mismatch", float_format="%.2f", index=False)
        sector_movement_data.to_excel(writer, sheet_name="Sector Movement > 10%", float_format="%.2f", index=False)
        # benchmark_not_available_data.to_excel(writer, sheet_name="Benchmark disabled", float_format="%.2f", index=False)        
        co_code_duplicate_data.to_excel(writer, sheet_name="Duplicate Co_Code", float_format="%.2f", index=False)
        isin_code_duplicate_data.to_excel(writer, sheet_name="Duplicate ISIN Code", float_format="%.2f", index=False)
        assetclass_mismatch_data.to_excel(writer, sheet_name="Asset Class mismatch", float_format="%.2f", index=False)
        sector_others_data.to_excel(writer, sheet_name="Security marked Others", float_format="%.2f", index=False)
        df_exp_missing.to_excel(writer, sheet_name="Expense ratio not available", float_format="%.2f", index=False)
        null_security_type_data.to_excel(writer, sheet_name="Holding Security Type null", float_format="%.2f", index=False)
        aum_movement_data.to_excel(writer, sheet_name="AUM Movement", float_format="%.2f", index=False)

    return attachements

def check_trial_user_usage(db_session):
    today = dt.today()
    yday = today - timedelta(days=1)

    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )

    config = get_config(config_file_path)
    path = F"{str(Path(__file__).parent.parent)}/src/templates/"
        
    sql_user_usage = db_session.query(Organization.Organization_Name, 
                                      User.Email_Address, 
                                      User.Contact_Number, 
                                      User.Display_Name, 
                                      User.Created_Date_Time, 
                                      func.count(UserLog.User_Id).label("Total Login"), 
                                      func.max(UserLog.login_timestamp).label("Last Login"), 
                                      User.Is_Active, 
                                      Organization.License_Expiry_Date,
                                      UserType.usertype_name,
                                      func.datediff(text('hour'), User.Created_Date_Time, today).label('diff'))\
                                            .select_from(User)\
                                            .join(Organization, Organization.Organization_Id==User.Organization_Id)\
                                            .join(UserType, UserType.id == Organization.usertype_id, isouter=True)\
                                            .join(UserLog, User.User_Id==UserLog.User_Id, isouter=True)\
                                            .filter(Organization.usertype_id.in_([4, 9, 10]), 
                                                    Organization.Is_Active ==1, 
                                                    Organization.License_Expiry_Date >= yday.strftime('%Y-%m-%d'))\
                                            .group_by(Organization.Organization_Name, 
                                                      User.Email_Address, 
                                                      User.Contact_Number, 
                                                      User.Display_Name, 
                                                      User.Created_Date_Time, 
                                                      User.Is_Active, 
                                                      User.Created_Date_Time, 
                                                      Organization.License_Expiry_Date,
                                                      UserType.usertype_name)\
                                            .order_by(Organization.License_Expiry_Date)\
                                            .all()
    user_usage = pd.DataFrame(sql_user_usage)
    
    for ind in user_usage.index:
        template_name = ''
        subject = ''
        sms_text = ''
        
        organization_name = user_usage['Organization_Name'][ind]
        email_address = user_usage['Email_Address'][ind]
        contact_number = user_usage['Contact_Number'][ind]
        display_name = user_usage['Display_Name'][ind]
        created_date_time = user_usage['Created_Date_Time'][ind]
        license_expiry_date = user_usage['License_Expiry_Date'][ind]
        total_login = user_usage['Total Login'][ind]
        created_before_hrs = user_usage['diff'][ind]
        usertype_name = user_usage['usertype_name'][ind]

        if license_expiry_date < datetime.date(today):#expired
            delta = datetime.date(today) - license_expiry_date
            if delta.days == 1:
                msg = 'Trial - expired.'
                template_name = 'trial_user_subscribe.html'
                subject = 'Finalyca - Subscription'

                if total_login == 0:#send subscription mail to user                    
                    msg = 'Trial - expired with no usage.'
                    
                #send email to business team
                with open(F'{path}new_trial_user_details_to_business_team.html') as file_:
                    template = Template(file_.read())

                html_msg = template.render(name=display_name, email_id=email_address, mobile=contact_number, end_date=license_expiry_date, msg=msg, total_login=total_login, user_type=usertype_name)
                
                trial_user_mail_to = list(str(config["TRIAL_USER_MAIL_TO"]).split(";"))
                for e in trial_user_mail_to:
                    send_email_async(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], e, "Finalyca - Trial | Expired", html_msg)
            

        elif created_before_hrs>=24:
            if total_login == 0:#never logged in after one day email and SMS
                template_name = 'trial_user_no_login.html'
                subject = 'Finalyca - Trial'
                sms_text = F"Welcome {display_name}! \n Explore, learn, and thrive with Finalyca's 2-day Trial. \n Enjoy the journey by clicking on https://portal.finalyca.com/  \n If you need assistance, feel free to reach out. Dimple Turakhia: 9840775773.  \n FNLYCA"
                
            else:#logged in atleast once email and SMS
                template_name = 'trial_user_logged_in_atleast_once.html'
                subject = 'Finalyca - trial'
                sms_text = F"Hello {display_name},\n Check out our 2-day Trial's brilliance! Dive into knowledge with us at https://portal.finalyca.com/  \n For guidance, reach out to Dimple Turakhia (9840775773). \n FNLYCA"
                
        if email_address and template_name:
            template = None

            #send email to user            
            with open(F'{path}{template_name}', encoding='utf8') as file_:
                template = Template(file_.read())

            html_msg = template.render(name=display_name, email_id=email_address, mobile=contact_number, end_date=license_expiry_date)

            send_email_async(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], email_address, subject, html_msg)
        
        if contact_number and sms_text:
            sms_config = SMSConfig()
            sms_config.url = config["SMS_URL"]
            sms_config.sender_id = config["SMS_SENDER_ID"]
            sms_config.is_unicode = config["SMS_IS_UNICODE"]
            sms_config.is_flash = config["SMS_IS_FLASH"]
            sms_config.api_key = config["SMS_API_KEY"]
            sms_config.client_id = config["SMS_CLIENT_ID"]
            
            send_sms(sms_config, contact_number, sms_text)


def main():
    db_session = get_finalyca_scoped_session(True)
    export_dir_path = "E:\\Finalyca\\api\\Reports\\"
    file_name = export_data_report(db_session, export_dir_path)
    # check_trial_user_usage(db_session)
    export_business_report(db_session, export_dir_path)
    print(F"Saved in {file_name}")

    
    
# main()
