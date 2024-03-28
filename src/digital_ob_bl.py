from datetime import datetime
import hashlib
from typing import Dict
from fin_models.controller_master_models import Organization, User, DOFacilitator
from fin_models.controller_transaction_models import OrgFundSettings
from sqlalchemy import and_, distinct, func
from fin_models.masters_models import AMC, Fund, MFSecurity
import urllib
import requests
import json

class MissingConfigurationException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class UpstreamServerErrorException(Exception):
    def __init__(self, info: Dict):
        self.info = info

    def __str__(self):
        return json.dumps(self.info)

def can_user_transact(db_session, user_id, user_org_id, fund_code):
    result = False

    # Get the fund code and check if the org is allowed to make the transaction
    sql_obj = db_session.query(OrgFundSettings).filter(and_(OrgFundSettings.fund_code==fund_code, OrgFundSettings.org_id==user_org_id, OrgFundSettings.can_buy == 1)).one_or_none()

    # TODO: check if individual user can be allowed to make the transaction
    
    if sql_obj:
        result = True
    
    return result

def get_fund_invest_url(db_session, user_id, fund_code):
    sql_user = db_session.query(User).filter(User.User_Id == user_id).one_or_none()
    user_name = sql_user.Display_Name
    user_email = sql_user.Email_Address
    user_mobile = sql_user.Contact_Number
    user_org_id = sql_user.Organization_Id

    sql_obj = db_session.query(OrgFundSettings).filter(and_(OrgFundSettings.fund_code==fund_code, OrgFundSettings.org_id==user_org_id, OrgFundSettings.can_buy == 1)).one_or_none()
    facilitator_id = sql_obj.facilitator_id
    distributor_org_id = sql_obj.distributor_org_id
    # distributor_sso_token = sql_obj.distributor_token

    if not facilitator_id:
        raise MissingConfigurationException("Please add Facilitator id in the Org Admin portal")

    if not distributor_org_id:
        raise MissingConfigurationException("Please add Distributor org id in the Org Admin portal")

    # if not distributor_sso_token:
    #     raise MissingConfigurationException("Please add Distributor SSO token in the Org Admin portal")

    sql_facilitator = db_session.query(DOFacilitator).filter(DOFacilitator.id == facilitator_id).one_or_none()   
    gateway_url = sql_facilitator.facilitator_url
    
    # TODO: there could be different SSO for different facilitator. Currently it is developed for 1SilverBullet platform.
    # url = create_sso_url(gateway_url, distributor_org_id, user_email, user_name, user_mobile, distributor_sso_token)
    url = create_sso_url(gateway_url, distributor_org_id, user_email, user_name, user_mobile, None)

    return url

# def create_sso_url(gateway_url, distributor_org_id, user_email, user_name, user_mobile, distributor_sso_token):
#     # Based on 1SB SSO document, organization can have 2 types. M for AMC (manufacturer) and C for Distributor (consumer).
#     distributor_org_type = 'C'
#     sso_str = F"{distributor_org_id}|{distributor_org_type}|{user_email}|{user_name}|{user_mobile}|{distributor_sso_token}"

#     token = hashlib.sha256(sso_str.encode('utf-8')).hexdigest()

#     query = dict()
#     query["orgId"] = distributor_org_id
#     query["orgType"] = distributor_org_type
#     query["userId"] = user_email
#     query["userName"] = urllib.parse.quote_plus(user_name)
#     query["userMobileNumber"] = user_mobile
#     query["token"] = token

#     query_str = '&'.join(["{}={}".format(k, v) for k, v in query.items()])

#     url = F"{gateway_url}?{query_str}"
#     # url = 'https://www.1silverbullet.tech/'
#     return url

def create_sso_url(gateway_url, distributor_org_id, user_email, user_name, user_mobile, distributor_sso_token):
    # Based on discussion with Brajendra.
    # org_id = 'LbfRDmbDFp7Q'
    safe_user_name = urllib.parse.quote_plus(user_name)
    org_type = 'D'
    # url = F"http://3.6.207.156:8081/ssoLoginUrl?orgId={distributor_org_id}&orgType={org_type}&userEmail={user_email}&userName={safe_user_name}&userMobileNumber={user_mobile}"

    # Updated as per recommendation from Akash Rajwar
    url = F'http://13.126.143.172:8081/ssoLoginUrl?orgId={distributor_org_id}&orgType={org_type}&userEmail={user_email}&userName={safe_user_name}&userMobileNumber={user_mobile}'

    valid_sso_url_found = False
    error_msg = dict()

    resp = requests.post(url)
    if resp.status_code == 200:
        data = resp.json()
        if "status" in data and "errorCode" in data["status"]:
            if data["status"]["errorCode"] == '0':
                valid_sso_url_found = True
                sso_url = data["ssoUrl"]
            else:
                error_msg = data["status"]["errorMessage"]
    
    if not valid_sso_url_found:
        raise UpstreamServerErrorException(error_msg)

    return sso_url

def get_investment_settings(db_session, org_id):
    result = list()

    sql_funds = db_session.query(distinct(OrgFundSettings.fund_code)).filter(OrgFundSettings.org_id == org_id).filter(OrgFundSettings.can_buy==True).all()
    fund_codes = list()
    for sql_f in sql_funds:
        fund_codes.append(sql_f[0])

    sql_fund_amcs = db_session.query(Fund, AMC).join(MFSecurity, and_(MFSecurity.Fund_Id==Fund.Fund_Id, MFSecurity.Is_Deleted != 1)).join(AMC, MFSecurity.AMC_Id==AMC.AMC_Id).filter(Fund.Fund_Code.in_(fund_codes)).all()
    fund_amcs = dict()
    for sql_fu in sql_fund_amcs:
        fund_amcs[sql_fu[0].Fund_Code] = {'fund': sql_fu[0].Fund_Name, "amc_id": sql_fu[1].AMC_Id, 'amc': sql_fu[1].AMC_Name }


    # Check if we have an entry in the table for same combination. if yes, edit it else create it.
    sql_settings = db_session.query(OrgFundSettings, DOFacilitator).join(DOFacilitator, DOFacilitator.id==OrgFundSettings.facilitator_id).filter(OrgFundSettings.org_id == org_id).filter(OrgFundSettings.can_buy==True).all()
    for sql_obj in sql_settings:
        fund_code = sql_obj[0].fund_code
        amc_id = sql_obj[0].amc_id
        fund_lookup = fund_amcs[fund_code]

        obj = dict()
        obj["amc"] = fund_lookup["amc"]
        obj["amc_id"] = fund_lookup["amc_id"]
        obj["fund"] = fund_lookup["fund"]
        obj["fund_code"] = sql_obj[0].fund_code
        obj["facilitator"] = sql_obj[1].name
        obj["facilitator_id"] = sql_obj[1].id
        # obj["distributor_pan_no"] = sql_obj[0].distributor_pan_no
        obj["distributor_org_id"] = sql_obj[0].distributor_org_id
        obj["distributor_token"] = sql_obj[0].distributor_token
        result.append(obj)

    return result

def save_investment_setting(db_session, org_id, amc_id, fund_code, facilitator_id, distributor_org_id, distributor_token, user_id):
    ts = datetime.now()

    # Check if we have an entry in the table for same combination. if yes, edit it else create it.
    sql_settings = db_session.query(OrgFundSettings).filter(and_(OrgFundSettings.org_id == org_id, OrgFundSettings.amc_id == amc_id, OrgFundSettings.fund_code == fund_code)).one_or_none()
    if not sql_settings:
        sql_settings = OrgFundSettings()
        sql_settings.org_id = org_id
        sql_settings.amc_id = amc_id
        sql_settings.fund_code = fund_code
        sql_settings.created_by = user_id
        sql_settings.created_at = ts
    
    sql_settings.can_buy = True
    sql_settings.facilitator_id = facilitator_id
    # sql_settings.distributor_pan_no = distributor_pan_no
    sql_settings.distributor_org_id = distributor_org_id
    sql_settings.distributor_token = distributor_token

    if sql_settings.id:
        sql_settings.modified_by = user_id
        sql_settings.modified_at = ts
    else:
        db_session.add(sql_settings)

    db_session.commit()

def remove_investment_setting(db_session, org_id, amc_id, fund_code, user_id):
    ts = datetime.now()

    sql_settings = db_session.query(OrgFundSettings).filter(and_(OrgFundSettings.org_id == org_id, OrgFundSettings.amc_id == amc_id, OrgFundSettings.fund_code == fund_code)).one_or_none()
    if sql_settings:    
        sql_settings.can_buy = False
        sql_settings.facilitator_id = None
        sql_settings.distributor_org_id = None
        sql_settings.distributor_token = None
        sql_settings.modified_by = user_id
        sql_settings.modified_at = ts
        db_session.commit()