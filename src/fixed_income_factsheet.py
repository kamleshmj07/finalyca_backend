from flask import Blueprint, current_app, request, jsonify
from werkzeug.exceptions import BadRequest
from itsdangerous import json
from datetime import datetime as dtt
import pandas as pd
import numpy as np

from bizlogic.fixed_income_db_helper import get_fi_detailed_security_info, get_fi_call_put_for_id, get_fi_redemption_for_id,\
                                            get_fi_securities_for_name, get_raw_data_for_securities_exposure
from bizlogic.fixed_income_analysis import get_fi_issuer_characteristic, get_fi_cashflows, get_fi_coupon_and_pay_dates,\
                                           get_fi_yield_to_maturity, get_fi_macaulay_duration
from fin_models.masters_models import DebtSecurity, DebtCreditRating
from utils.utils import remove_stop_words

fixed_income_factsheet_bp = Blueprint("fixed_income_factsheet_bp", __name__)

# factsheet overview
@fixed_income_factsheet_bp.route('/api/v1/fi_security', methods=['GET'])
def get_fi_security_overview():
    security_id = request.args.get('security_id')
    isin = request.args.get('isin')

    if not security_id and not isin:
        raise BadRequest('Parameter Required: <security_id> or <isin>')

    filters = []
    if security_id:
        filters.append({'column' : 'DebtSecurity_Id', 'value' : security_id})

    if isin:
        filters.append({'column' : 'ISIN', 'value' : isin})

    list_fi_sec = get_fi_detailed_security_info(current_app.store.db, filters)
    bilav_internal_issuer_id = int(list_fi_sec[0]['Bilav_Internal_Issuer_Id'])

    filters = [
        {
            'column' : 'Bilav_Internal_Issuer_Id',
            'value' : [bilav_internal_issuer_id]
        }
    ]

    # get issuer information
    df_issuer_details = get_fi_issuer_characteristic(current_app.store.db, filters=filters)
    list_fi_sec[0]['Total_Issue_Size'] = df_issuer_details['Total_Issue_Size'].iloc[0]

    return jsonify(list_fi_sec)


@fixed_income_factsheet_bp.route('/api/v1/call_put', methods=['GET'])
def get_fi_security_call_put():
    security_id = request.args.get('security_id')
    bond_option = request.args.get('bond_option')

    if not security_id:
        raise BadRequest('security id parameter is required')
    
    debt_call_put_json = get_fi_call_put_for_id(current_app.store.db, security_id, bond_option)
    return jsonify(debt_call_put_json)


@fixed_income_factsheet_bp.route('/api/v1/fi_redemption', methods=['GET'])
def get_fi_redemption():
    security_id = request.args.get('security_id')

    if not security_id:
        raise BadRequest('security_id parameter is required')
   
    debt_json = get_fi_redemption_for_id(current_app.store.db, security_id)
    return jsonify(debt_json)


# modal search logic to get debt security name
@fixed_income_factsheet_bp.route('/api/v1/search_fi_securities', methods=['GET'])
def search_fi_securities():
    fi_security_name = request.args.get('fi_security_name', type=str, default=None)
    resp = list()

    if not fi_security_name:
        raise BadRequest('Security Name is required.')
    
    resp = get_fi_securities_for_name(current_app.store.db, fi_security_name)
    return jsonify(resp)


# dropdown listing logic
@fixed_income_factsheet_bp.route('/api/v1/bond_type', methods=['GET'])
def get_bond_types():
    db_session = current_app.store.db
    resp = list()

    q = db_session.query(DebtSecurity.Bond_Type_Code,
                         DebtSecurity.Bond_Type).distinct().all()

    for r in q:
        data = dict()
        data["key"] = r.Bond_Type_Code
        data["label"] = r.Bond_Type
        resp.append(data)

    return jsonify(resp)


@fixed_income_factsheet_bp.route('/api/v1/fi_sectors', methods=['GET'])
def get_fi_sectors():
    db_session = current_app.store.db
    resp = list()

    q = db_session.query(DebtSecurity.Ind_Code,
                         DebtSecurity.Industry).distinct().all()

    for r in q:
        data = dict()
        data["key"] = r.Ind_Code
        data["label"] = r.Industry
        resp.append(data)

    return jsonify(resp)


@fixed_income_factsheet_bp.route('/api/v1/fi_exchanges', methods=['GET'])
def get_fi_exchanges():
    db_session = current_app.store.db
    resp = list()

    q1 = db_session.query(DebtSecurity.Exchange_1.label('Exchange_Code'),
                          DebtSecurity.Exchange_1_Local_Code.label('Exchange_Name'))\
                    .filter(DebtSecurity.Exchange_1 != None).distinct()
    q2 = db_session.query(DebtSecurity.Exchange_2.label('Exchange_Code'),
                          DebtSecurity.Exchange_2_Local_Code.label('Exchange_Name'))\
                    .filter(DebtSecurity.Exchange_2 != None).distinct()
    q3 = db_session.query(DebtSecurity.Exchange_3.label('Exchange_Code'),
                          DebtSecurity.Exchange_3_Local_Code.label('Exchange_Name'))\
                    .filter(DebtSecurity.Exchange_3 != None).distinct()

    q = q1.union(q2,q3)

    for r in q:
        data = dict()
        data["key"] = r.Exchange_Code
        data["label"] = r.Exchange_Name
        resp.append(data)

    return jsonify(resp)


@fixed_income_factsheet_bp.route('/api/v1/fi_sec_cashflows', methods=['GET'])
def get_fi_characteristics():
    # TODO Move the following to separate function, here only function call based on input should take place
    security_id = request.args.get('security_id', type=int)
    price = request.args.get('price', type=np.float64, default=None)
    db_session = current_app.store.db

    if not security_id:
        raise BadRequest('Parameter Required: <security_id>')

    # prepare the dataset for input to the cashflow calculation engine
    list_fi_sec = get_fi_detailed_security_info(db_session, filters=[{'column' : 'DebtSecurity_Id', 'value' : security_id}])

    df_sec = pd.DataFrame(list_fi_sec)
    list_cols_reqd = ['Face_Value', 'Interest_Rate', 'Maturity_Date', 'Interest_Pay_Date_1', 'Interest_Pay_Date_2',
                      'Interest_Pay_Date_3', 'Interest_Pay_Date_4', 'Interest_Pay_Date_5', 'Interest_Pay_Date_6',
                      'Interest_Pay_Date_7', 'Interest_Pay_Date_8', 'Interest_Pay_Date_9', 'Interest_Pay_Date_10',
                      'Interest_Pay_Date_11', 'Interest_Pay_Date_12', 'Coupon_Type_Code', 'Coupon_Type',
                      'Interest_Payment_Frequency_Code', 'Interest_Payment_Frequency']
    list_drop_cols = [x for x in list(df_sec.columns) if x not in list_cols_reqd]
    df_sec.drop(list_drop_cols, axis=1, inplace=True)

    # freq, maturity date and coupon rate, face value
    coupon_frequency = df_sec['Interest_Payment_Frequency'][0]
    maturity_date = dtt.strptime(df_sec['Maturity_Date'][0], r"%d %b %Y")
    coupon_rate = df_sec['Interest_Rate'][0]
    face_value = df_sec['Face_Value'][0]
    lst_dates = df_sec[['Interest_Pay_Date_1', 'Interest_Pay_Date_2', 'Interest_Pay_Date_3', 'Interest_Pay_Date_4',
                        'Interest_Pay_Date_5', 'Interest_Pay_Date_6', 'Interest_Pay_Date_7', 'Interest_Pay_Date_8',
                        'Interest_Pay_Date_9', 'Interest_Pay_Date_10', 'Interest_Pay_Date_11', 'Interest_Pay_Date_12']].values.flatten().tolist()

    payout_dates, effective_coupon_rate, compounding_periods = get_fi_coupon_and_pay_dates(lst_payout_dates=lst_dates,
                                                                                           maturity_date=maturity_date,
                                                                                           coupon_rate=coupon_rate,
                                                                                           coupon_frequency=coupon_frequency)

    # in between redemption values to be added nearest to payout dates
    lst_redmp_records = get_fi_redemption_for_id(db_session, security_id)
    df_cf = get_fi_cashflows(face_value, effective_coupon_rate, maturity_date, payout_dates, lst_redmp_records)

    total_interest_earned = df_cf['Coupon_Value'].sum()
    single_coupon_value=coupon_rate*face_value/100
    bond_yield = (single_coupon_value / face_value) * 100
    total_compounding_periods = len(payout_dates)
    yield_to_maturity = macaulay_duration = modifield_duration = None
    if price:
        yield_to_maturity = get_fi_yield_to_maturity(face_value=face_value,
                                                     single_coupon_value=single_coupon_value,
                                                     coupon_frequency=coupon_frequency,
                                                     total_compounding_periods=total_compounding_periods,
                                                     curr_mkt_price_of_bond=price)

        df_cf['Total_Cashflow'] = np.where(df_cf['Is_Redemption'], df_cf['Redemption_Price'] + df_cf['Coupon_Value'], df_cf['Coupon_Value'])
        macaulay_duration = get_fi_macaulay_duration(df_cf['Total_Cashflow'], effective_coupon_rate)
        modifield_duration = macaulay_duration / (1 + (yield_to_maturity / compounding_periods))

        yield_to_maturity = round(yield_to_maturity,6)*100 if yield_to_maturity else None
        bond_yield = round(bond_yield, 6)
        macaulay_duration = round(macaulay_duration,2)
        modifield_duration = round(modifield_duration,2)

    # preparing the response
    resp = {}
    cf_json = df_cf.to_json(orient='records')
    cf_parsed = json.loads(cf_json)
    resp['Cashflows'] = cf_parsed
    resp['Total_Interest'] = total_interest_earned
    resp['Yield'] = bond_yield
    resp['Yield_To_Maturity'] = yield_to_maturity
    resp['Macaulay_Duration_In_Yrs'] = macaulay_duration
    resp['Modified_Duration_In_Yrs'] = modifield_duration

    return jsonify(resp)


@fixed_income_factsheet_bp.route('/api/v1/fi_ratings_track', methods=['GET'])
def get_fi_ratings_track():
    security_id = request.args.get('security_id', type=int)
    db_session = current_app.store.db

    qry = db_session.query(DebtCreditRating.DebtSecurity_Id,
                           DebtCreditRating.Rating_Agency,
                           DebtCreditRating.Rating_Symbol,
                           DebtCreditRating.Rating_Direction,
                           DebtCreditRating.Rating_Outlook_Description,
                           DebtCreditRating.Rating_Prefix,
                           DebtCreditRating.Rating_Suffix,
                           DebtCreditRating.Prefix_Description,
                           DebtCreditRating.Suffix_Description,
                           DebtCreditRating.Watch_Flag,
                           DebtCreditRating.Watch_Flag_Reason,
                           DebtCreditRating.Rating_Date)\
                    .filter(DebtCreditRating.DebtSecurity_Id == security_id,
                            DebtCreditRating.Is_Deleted != 1)

    df = pd.DataFrame(qry)
    df.sort_values(by='Rating_Date', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['Rating_Date'] = pd.to_datetime(df['Rating_Date'], format=r'%Y-%m-%d')
    df['Rating_Date'] = df['Rating_Date'].dt.strftime(r'%d %b %Y')

    # cleaning data for the Rating_Agency information
    df["Rating_Agency"] = df["Rating_Agency"].str.upper()
    df["Rating_Agency"] = df["Rating_Agency"].str.replace('[^a-zA-Z0-9\s]', '')

    stop_words  = ['RATINGS', 'LTD']
    if not df.empty:
        df["Rating_Agency"] = df.apply(lambda x: remove_stop_words(x["Rating_Agency"], stop_words), axis=1)
        df["Rating_Agency"] = df["Rating_Agency"].str.strip()

    result = df.to_json(orient='records')
    parsed = json.loads(result)

    return jsonify(parsed)


@fixed_income_factsheet_bp.route('/api/v1/fi_security_exposure', methods=['GET'])
def get_fi_security_exposure():
    h_security_id = request.args.get('security_id')
    db_session = current_app.store.db
    page = request.args.get('page', type=int, default=0)
    limit = request.args.get('limit', type=int, default=None)

    result, total_records = get_raw_data_for_securities_exposure(db_session, [h_security_id])
    df = pd.DataFrame(result)

    resp = {}
    if df.empty:
        resp["result"] = []
        resp["total_records"] = 0
        resp["total_exposure_in_cr"] = 0
        return jsonify(resp)

    df.columns = map(str.lower, df.columns)
    df = df.groupby('fund_id').first().reset_index()
    # df = df.drop_duplicates()
    df['portfolio_date'] = df['portfolio_date'].dt.strftime(r'%d %b %y')
    df['purchase_date'] = df['purchase_date'].dt.strftime(r'%d %b %y')
    df['value_in_inr'] = round(pd.to_numeric(df['value_in_inr'])/10000000.00, 2)
    df.sort_values(by=['value_in_inr'], ascending=0, inplace=True)
    total_exposure_in_cr = df['value_in_inr'].sum()

    resp["result"] = df.to_dict(orient='records')
    resp["total_records"] = total_records
    resp["total_exposure_in_cr"] = total_exposure_in_cr

    return jsonify(resp)


