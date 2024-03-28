from datetime import datetime
import requests
import base64
import pandas as pd
import uuid
import xml.etree.ElementTree as ET 
from compass.portfolio_crud import get_all_investors, get_investor_dashboard_info, find_or_create_investor, find_or_create_investor_account, find_or_create_dummy_investor_account, find_or_create_investor_holding, update_investor, update_investor_account, delete_investor, find_or_create_investor_transactions, get_investor_transactions, validate_investor_transactions
from bizlogic.holding_interface import HoldingType

def check_transaction_status(BASE_URL, api_key, org_code, transaction_id):
    status = None
    is_sucess = False
    error_code = None
    accounts = None

    headers = dict()
    headers["api_key"] = api_key
    headers["org_id"] = org_code

    body = {
        "txnId": transaction_id
    }
    resp = requests.post(F"{BASE_URL}/process/transactionStatus", headers=headers, json=body)
    if resp.status_code == 200:
        obj = resp.json()
        status = obj["status"]
        if status == "COMPLETED":
            if obj["success"]:
                is_sucess = True
                accounts = obj["accounts"]
            else:
                error_code = obj["errorCode"]
    else:
        obj = resp.json()
        error_code = obj["errorCode"]
    
    return is_sucess, status, error_code, accounts

def initiate_consent(BASE_URL, api_key, org_code, profile_id, user_id, input_txn_id, start_date, end_date):
    consent_start_date = datetime.strftime(start_date, '%Y-%m-%d')
    consent_end_date = datetime.strftime(end_date, '%Y-%m-%d')
    status = None
    redirect_url = None
    txn_id = None
    error_code = None

    headers = dict()
    headers["api_key"] = api_key
    headers["org_id"] = org_code

    body = {
        "profileId": profile_id,
        "userId": user_id,
        "fipIds": [
            # "HDFC-FIP",
            # "ICICI-FIP",
            # 'Kotak Mahindra Bank',
            "ACME-FIP"
            # "Central Depository Services Limited",
            # "NSDL",
            # 'National Securities Depository Limited',
        ],
        "txnId": input_txn_id,
        # "fiTypes": [ 
        #     "EQUITIES", "MUTUAL_FUNDS", "sasr"
        # ],
        "fiTypes": [
            # 'DEPOSIT', 'TERM_DEPOSIT', 'RECURRING_DEPOSIT', 
            'SIP', 'CP', 'GOVT_SECURITIES', 'EQUITIES', 'BONDS', 'DEBENTURES', 'MUTUAL_FUNDS', 'ETF', 'IDR', 'CIS', 'AIF', 'INVIT', 'REIT', 'OTHER',
            # 'NPS', 
            # 'LIFE_INSURANCE', 'GENERAL_INSURANCE', 'INSURANCE_POLICIES', 'ULIP',
            # 'GSTN', 'GSTR1_3B', 'GSTN_GSTR1', 'GSTN_GSTR2A', 'GSTN_GSTR3B', 'GSTR'
        ],
        # "fiTypes" : ["DEPOSIT"],
        "startDate": consent_start_date,
        "endDate": consent_end_date,
        "returnUrl":"http://www.finalyca.com",
        "customerUniqueId":"wer2312434",
        # "optionalParams":{
        #     "consentNotification":"https://webhook.site/consent",
        #     "dataNotification":"https://webhook.site/data",
        #     "reportNotification":"https://webhook.site/report"
        # },
    }
    resp = requests.post(F"{BASE_URL}/process/initiateConsent", headers=headers, json=body)
    status = resp.status_code
    if resp.status_code == 200:
        obj = resp.json()
        consent_handle = obj["consentHandle"]
        redirect_url = obj["redirectURL"]
        txn_id = obj["txnId"]

    else:
        obj = resp.json()
        error_code = obj["errorCode"]

    return status, redirect_url, txn_id, error_code

def initiate_fetch(BASE_URL, api_key, org_code, profile_id, user_id, txn_id, start_date, end_date, accounts):
    headers = dict()
    headers["api_key"] = api_key
    headers["org_id"] = org_code

    body = {
        "profileId": profile_id,
        "userId": user_id,
        "txnId": txn_id,
        "startDate": start_date,
        "endDate": end_date,
        "accounts": accounts
    }

    resp = requests.post(F"{BASE_URL}/process/initiateFetch", headers=headers, json=body)
    return resp

def get_report(BASE_URL, api_key, org_code, txn_id, fip_id, link_ref_number):
    headers = dict()
    headers["api_key"] = api_key
    headers["org_id"] = org_code

    body = {
        "txnId": txn_id,
        "fipId": fip_id,
        "linkRefNumber": link_ref_number,
        # "format": report_format
    }

    resp = requests.post(F"{BASE_URL}/process/rawReport", headers=headers, json=body)
    return resp


def parse_equity_profile_xml_element(ele):
    holders = list()

    for holders_node in ele:
        if holders_node.tag == '{http://api.rebit.org.in/FISchema/equities}Holders':
            for node in holders_node:
                if node.tag == '{http://api.rebit.org.in/FISchema/equities}Holder':
                    obj = dict()
                    obj["name"] = node.attrib["name"]
                    obj["pan_no"] = node.attrib["pan"]
                    # As of now it is not clear how this will work for multiple owners and multiple accounts
                    obj["dematId"] = node.attrib["dematId"]
                    obj["kycCompliance"] = node.attrib["kycCompliance"] if "kycCompliance" in node.attrib else None
                    holders.append(obj)

    return holders

def parse_mf_profile_xml_element(ele):
    holders = list()
    
    for holders_node in ele:
        if holders_node.tag == '{http://api.rebit.org.in/FISchema/mutual_funds}Holders':        
            for node in holders_node:
                if node.tag == '{http://api.rebit.org.in/FISchema/mutual_funds}Holder':
                    obj = dict()
                    obj["name"] = node.attrib["name"]
                    obj["pan_no"] = node.attrib["pan"]
                    obj["dematId"] = node.attrib["dematId"] if "dematId" in node.attrib else None
                    obj["folioNo"] = node.attrib["folioNo"]
                    obj["kycCompliance"] = node.attrib["kycCompliance"] if "kycCompliance" in node.attrib else None
                    holders.append(obj)

    return holders
    
def parse_mf_holding_xml_element(ele):
    holdings = list()

    cost_value = ele.attrib['costValue']
    current_value = ele.attrib['currentValue']

    for investment_node in ele:
        if investment_node.tag == '{http://api.rebit.org.in/FISchema/mutual_funds}Investment':
            for holdings_node in investment_node:
                if holdings_node.tag == '{http://api.rebit.org.in/FISchema/mutual_funds}Holdings':
                    for node in holdings_node:
                        holdings.append(node.attrib)

    return cost_value, current_value, holdings

def parse_mf_transaction_xml_element(ele):
    start_date = None
    end_date = None

    start_date = ele.attrib['startDate']
    end_date = ele.attrib['endDate']
    transactions = list()
    for child in ele:
        transactions.append(child.attrib)

    return start_date, end_date, transactions
    
def parse_equity_holding_xml_element(ele):
    holdings = list()

    cost_value = ele.attrib['costValue'] if 'costValue' in ele.attrib else None
    current_value = ele.attrib['currentValue'] if 'currentValue' in ele.attrib else None

    for investment_node in ele:
        if investment_node.tag == '{http://api.rebit.org.in/FISchema/equities}Investment':
            for holdings_node in investment_node:
                if holdings_node.tag == '{http://api.rebit.org.in/FISchema/equities}Holdings':
                    for node in holdings_node:
                        holdings.append(node.attrib)

    return cost_value, current_value, holdings

def parse_equity_transaction_xml_element(ele):
    start_date = None
    end_date = None

    start_date = ele.attrib['startDate']
    end_date = ele.attrib['endDate']
    transactions = list()
    for child in ele:
        transactions.append(child.attrib)

    return start_date, end_date, transactions

def process_accounts(xml_str, db_session):
    account_type = None
    # TODO: Check the RM so we can map it properly
    rm_id = 10201

    res = dict()

    now = datetime.today()
    as_on_date = datetime.combine(now, datetime.min.time())
    
    tree = ET.fromstring(xml_str) 

    # Account Details
    for key, value in tree.attrib.items():
        if key == 'maskedAccNumber':
            res["account_no"] = value
        elif key == 'linkedAccRef':
            res["linkedAccRef"] = value
        elif key == 'type':
            account_type = res["account_type"] = value
        elif key == 'version':
            res["version"] = value

    account_id = None

    if account_type == 'equities':
        for child in tree:
            if child.tag == '{http://api.rebit.org.in/FISchema/equities}Profile':
                owners = parse_equity_profile_xml_element(child)
                for owner in owners:
                    primary_investor_id = find_or_create_investor(db_session, owner["name"], owner["pan_no"], owner["name"], rm_id)
                    owner_info = [{"PAN": owner["pan_no"], "name": owner["name"]}]
                    demat_id = owner["dematId"]
                    account_id = find_or_create_investor_account(db_session, primary_investor_id, owner_info, "demat", None, None, demat_id, None, None, rm_id)

            elif child.tag == '{http://api.rebit.org.in/FISchema/equities}Summary':
                cost_value, current_value, holdings = parse_equity_holding_xml_element(child)
                for holding in holdings:
                    units = float(holding["units"])
                    unit_value = float(holding["lastTradedPrice"])
                    total_value = units*unit_value
                    holding_id = find_or_create_investor_holding(db_session, account_id, as_on_date, holding["isin"], holding["isinDescription"], HoldingType.equity.value, None, None, units, unit_value, total_value, rm_id)
                
            elif child.tag == '{http://api.rebit.org.in/FISchema/equities}Transactions':
                res["start_date"], res["end_date"], res["transactions"] = parse_equity_transaction_xml_element(child)

    elif account_type == 'mutualfunds':
        for child in tree:
            if child.tag == '{http://api.rebit.org.in/FISchema/mutual_funds}Profile':
                owners = parse_mf_profile_xml_element(child)
                for owner in owners:
                    primary_investor_id = find_or_create_investor(db_session, owner["name"], owner["pan_no"], owner["name"], rm_id)
                    owner_info = [{"PAN": owner["pan_no"], "name": owner["name"]}]
                    # folio_no = owner["folioNo"]
                    # account_id = find_or_create_investor_account(db_session, primary_investor_id, owner_info, "folio", None, None, folio_no, None, None, rm_id)
            elif child.tag == '{http://api.rebit.org.in/FISchema/mutual_funds}Summary':
                cost_value, current_value, holdings = parse_mf_holding_xml_element(child)
                for holding in holdings:
                    folio_no = holding["folioNo"]
                    account_id = find_or_create_investor_account(db_session, primary_investor_id, owner_info, "folio", None, None, folio_no, None, None, rm_id)
                    units = float(holding["closingUnits"])
                    unit_value = float(holding["nav"])
                    total_value = units*unit_value
                    holding_id = find_or_create_investor_holding(db_session, account_id, as_on_date, holding["isin"], holding["isinDescription"], HoldingType.mutual_funds.value, None, None, units, unit_value, total_value, rm_id)

            elif child.tag == '{http://api.rebit.org.in/FISchema/mutual_funds}Transactions':
                res["start_date"], res["end_date"], res["transactions"] = parse_mf_transaction_xml_element(child)

    return res