import base64
from datetime import datetime
import uuid
from fin_models.controller_master_models import API
from flask import current_app, Blueprint, jsonify, request, g

from src.utils import get_user_info
from .access_bl import *
from .aa import check_transaction_status, initiate_consent, initiate_fetch, get_report, process_accounts
from fin_models.controller_transaction_models import AccountAggregatorAPIStatus

account_aggregator_bp = Blueprint("account_aggregator_bp", __name__)

@account_aggregator_bp.route('/account_aggregator/perfios_consent_initiate', methods=['POST'])
def consent_initiate():
    phone_number = request.form.get("mobile_number", type=int)
    start_date_str = request.form.get("start_date")
    end_date_str = request.form.get("end_date")

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    # TODO: Check in the database if the TXN_ID is valid or not
    TXN_ID = uuid.uuid4().hex
    user_id = F"{phone_number}@anumati"
    USER_ID = base64.b64encode(user_id.encode("ascii")).decode("ascii")

    BASE_URL = current_app.config["PERFIOS_BASE_URL"]
    API_KEY = current_app.config["PERFIOS_FINALYCA_API_KEY"]
    ORG_CODE = current_app.config["PERFIOS_FINALYCA_ORG_CODE"]
    PROFILE_ID = current_app.config["PERFIOS_FINALYCA_PROFILE_ID"]

    status, redirect_url, txn_id, error_code = initiate_consent(BASE_URL, API_KEY, ORG_CODE, PROFILE_ID, USER_ID, TXN_ID, start_date, end_date)
    
    resp = dict()
    resp["status"]= status
    resp["redirect_url"]= redirect_url
    resp["error_code"]= error_code

    sql_obj = AccountAggregatorAPIStatus()
    sql_obj.user_id = USER_ID
    sql_obj.txn_id = TXN_ID
    sql_obj.data_start_date = start_date
    sql_obj.data_end_date = end_date
    sql_obj.consent_init_req_time = datetime.now()
    current_app.store.db.add(sql_obj)
    current_app.store.db.commit()

    return jsonify(resp)

@account_aggregator_bp.route('/account_aggregator/perfios_consent_callback', methods=['POST'])
def api_consent_callback():
    txn_id = request.json.get("txnId")
    status = request.json.get("consentStatus")
    accounts = request.json.get("accounts")
    
    sql_obj = current_app.store.db.query(AccountAggregatorAPIStatus).filter(AccountAggregatorAPIStatus.txn_id==txn_id).one_or_none()

    if not sql_obj:
        raise Exception(F"{txn_id} was not found in the database.")
   
    # Initiate fetch
    BASE_URL = current_app.config["PERFIOS_BASE_URL"]
    API_KEY = current_app.config["PERFIOS_FINALYCA_API_KEY"]
    ORG_CODE = current_app.config["PERFIOS_FINALYCA_ORG_CODE"]
    PROFILE_ID = current_app.config["PERFIOS_FINALYCA_PROFILE_ID"]
    user_id = sql_obj.user_id
    start_date = sql_obj.data_start_date
    end_date = sql_obj.data_end_date

    start_date_str = start_date.date().isoformat()
    end_date_str = end_date.date().isoformat()

    resp = initiate_fetch(BASE_URL, API_KEY, ORG_CODE, PROFILE_ID, user_id, txn_id, start_date_str, end_date_str, accounts)
    
    sql_obj.consent_status_time = datetime.now()
    sql_obj.consent_status = status
    sql_obj.consent_accounts = accounts
    sql_obj.fetch_init_req_time = datetime.now()
    sql_obj.fetch_init_resp = resp.json()
    current_app.store.db.commit()

    return jsonify({"status": "Ok"})

@account_aggregator_bp.route('/account_aggregator/perfiod_status_check', methods=['POST'])
def get_transaction_status():
    txn_id = request.form.get("txn_id")

    BASE_URL = current_app.config["PERFIOS_BASE_URL"]
    API_KEY = current_app.config["PERFIOS_FINALYCA_API_KEY"]
    ORG_CODE = current_app.config["PERFIOS_FINALYCA_ORG_CODE"]
    PROFILE_ID = current_app.config["PERFIOS_FINALYCA_PROFILE_ID"]

    is_sucess, status, error_code, accounts = check_transaction_status(BASE_URL, API_KEY, ORG_CODE, txn_id)
    resp = dict()
    resp['is_sucess'] = is_sucess
    resp['status'] = status
    resp['error_code'] = error_code
    resp['accounts'] = accounts

    results = list()
    if accounts:
        for account in accounts:
            obj = dict()
            fip_id = account["fipId"]
            linkRefNumber = account["linkRefNumber"]
            status = account["status"]
            if status == "READY":
                resp = get_report(BASE_URL, API_KEY, ORG_CODE, txn_id, fip_id, linkRefNumber)
                d = resp.json()["data"]
                obj["data"] = d

            obj["fipId"] = fip_id
            obj["linkRefNumber"] = linkRefNumber
            obj["status"] = status

        results.append(obj)
    
    resp['report'] = results
    
    return jsonify({"status": "Ok", "result": resp})

@account_aggregator_bp.route('/account_aggregator/perfios_fetch_callback', methods=['POST'])
def api_fetch_callback():
    txn_id = request.json.get("txnId")
    status = request.json.get("consentStatus")
    accounts = request.json.get("accounts")

# fiType – string, e.g. DEPOSIT, RECURRING_DEPOSIT etc.
# fipId – string,fip id
# maskedAccNumber – string,masked account number
# accType – string,type of account e.g. SAVINGS,CURRENT
# linkRefNumber – string,link ref number
# status – string,gives fetch status e.g. READY, DENIED, PENDING, DELIVERED, TIMEOUT, FAILED
    BASE_URL = current_app.config["PERFIOS_BASE_URL"]
    API_KEY = current_app.config["PERFIOS_FINALYCA_API_KEY"]
    ORG_CODE = current_app.config["PERFIOS_FINALYCA_ORG_CODE"]
    PROFILE_ID = current_app.config["PERFIOS_FINALYCA_PROFILE_ID"]

    sql_obj = current_app.store.db.query(AccountAggregatorAPIStatus).filter(AccountAggregatorAPIStatus.txn_id==txn_id).one_or_none()
    if not sql_obj:
        raise Exception(F"{txn_id} was not found in the database.")

    sql_obj.fetch_callback_req_time = datetime.now()

    results = list()
    for account in accounts:
        obj = dict()
        fip_id = account["fipId"]
        linkRefNumber = account["linkRefNumber"]
        status = account["status"]
        if status == "READY":
            resp = get_report(BASE_URL, API_KEY, ORG_CODE, txn_id, fip_id, linkRefNumber)
            d = resp.json()["data"]
            obj["data"] = d

        obj["fipId"] = fip_id
        obj["linkRefNumber"] = linkRefNumber
        obj["status"] = status

        results.append(obj)

    sql_obj.report_fetch_resp = results
    current_app.store.db.commit()

    statements = list()
    json_data = sql_obj.report_fetch_resp
    for data_obj in json_data:
        fip_id = data_obj["fipId"]
        link_ref_number = data_obj["linkRefNumber"]
        status = data_obj["status"]
        d = data_obj["data"]
        xml_str = base64.b64decode(d).decode('utf-8')
        statements.append(process_accounts(xml_str, current_app.store.db))
        
    return jsonify({"status": "Ok"})