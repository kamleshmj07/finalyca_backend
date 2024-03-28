from itsdangerous import json
import numpy as np
import pandas as pd
from datetime import date as dt
from flask import Blueprint, current_app, request, jsonify
from werkzeug.exceptions import BadRequest
from sqlalchemy import func

from bizlogic.fixed_income_db_helper import get_fi_securities_summary, get_credit_ratings_for_security, get_raw_data_for_securities_exposure
from bizlogic.fixed_income_analysis import get_issuers_detail
from fin_models.masters_models import DebtSecurity, HoldingSecurity
fixed_income_issuers_bp = Blueprint("fixed_income_issuers_bp", __name__)


@fixed_income_issuers_bp.route('/api/v1/issuers', methods=['POST'])
def get_issuers():
    criterias = request.json
    filter = criterias.get('filter', None) if criterias else None
    sorting = criterias.get('sort', None) if criterias else None
    page = request.args.get('page', type=int, default=0)
    limit = request.args.get('limit', type=int, default=None)

    resp = {}
    result, total_records = get_issuers_detail(current_app.store.db, page, limit, filter, sorting)

    if not result:
        resp["result"] = []
        resp["total_records"] = 0
        return jsonify(resp)

    resp["result"] = result
    resp["total_records"] = total_records

    return jsonify(resp)


@fixed_income_issuers_bp.route('/api/v1/fi_securities_summary', methods=['POST'])
def get_fi_security_summary():
    bilav_internal_issuer_id = request.args.get('issuer_id', type=int)
    criterias = request.json
    filters = criterias.get('filter', None) if criterias else None
    sorting = criterias.get('sort', None) if criterias else None
    page = request.args.get('page', type=int, default=0)
    limit = request.args.get('limit', type=int, default=50)

    if bilav_internal_issuer_id:
        filters = [] if not filters else filters
        filters.append({"column" : "Bilav_Internal_Issuer_Id", "value" : bilav_internal_issuer_id})

    db_session = current_app.store.db
    result, total_records  = get_fi_securities_summary(db_session, page, limit, filters, sorting)

    resp = {}
    if not result:
        resp["result"] = []
        resp["total_records"] = 0
        return jsonify(resp)

    df = pd.DataFrame(result)
    df['FI_Credit_Ratings'] = np.empty((len(df), 0)).tolist()

    list_security_ids = list(df["DebtSecurity_Id"])
    lst_index_to_drop = []
    filters=[{'column' : 'Rating_Symbol', 'value' : f.get('value')} for f in filters if 'Rating_Symbol' == f.get('column')] if filters else []
    # TODO: Refactor this logic here
    for security_id in list_security_ids:
        security_index = df.loc[df["DebtSecurity_Id"] == security_id].index.values[0]
        issuer_type = df.loc[security_index, "Issuer_Type"]
        if issuer_type not in ["Government", "State Government"]:
            new_filter = filters + [{'column' : 'DebtSecurity_Id', 'value' : security_id}]
            list_credit_ratings = get_credit_ratings_for_security(db_session, filters=new_filter)
            if len(list_credit_ratings) > 0:
                df.at[security_index, "FI_Credit_Ratings"].extend(list_credit_ratings)
    #         else:
    #             lst_index_to_drop.append(security_index)

    # df.drop(index=lst_index_to_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df.loc[:, ~df.columns.duplicated()]

    resp["result"] = df.to_dict(orient='records')
    resp["total_records"] = total_records

    return jsonify(resp)


@fixed_income_issuers_bp.route('/api/v1/fi_issuer_exposure', methods=['GET'])
def get_fi_issuer_exposure():
    bilav_issuer_id = request.args.get('issuer_id')
    db_session = current_app.store.db
    page = request.args.get('page', type=int, default=0)
    limit = request.args.get('limit', type=int, default=None)

    sql_hsec = db_session.query(HoldingSecurity.HoldingSecurity_Id)\
                         .join(DebtSecurity, HoldingSecurity.Co_Code == func.concat('BLV_', DebtSecurity.Bilav_Code))\
                         .filter(DebtSecurity.Is_Deleted != 1,
                                 DebtSecurity.Bilav_Internal_Issuer_Id == bilav_issuer_id,
                                 HoldingSecurity.Maturity_Date > dt.today()).all()

    lst_hsecurity_ids = [x[0] for x in sql_hsec]

    result, total_records = get_raw_data_for_securities_exposure(db_session, lst_hsecurity_ids)
    df = pd.DataFrame(result)

    resp = {}
    if df.empty:
        resp["result"] = []
        resp["total_records"] = 0
        resp["total_exposure_in_cr"] = 0
        return jsonify(resp)

    df.columns = map(str.lower, df.columns)
    columns_to_display = ["plan_id", "fund_name", "product_name", "value_in_inr", "portfolio_date"]
    df = df[columns_to_display].drop_duplicates()
    df = df.groupby(by=["fund_name", "product_name", "portfolio_date"]) .agg({
        'value_in_inr' : 'sum',
        'plan_id' : 'max',
        'fund_name' : 'max',
        'product_name' : 'max',
        'portfolio_date' : 'max',
    })
    df.reset_index(drop=True, inplace=True)
    df['portfolio_date'] = df['portfolio_date'].dt.strftime(r'%d %b %y')
    df['value_in_inr'] = round(pd.to_numeric(df['value_in_inr'])/10000000.00, 2)
    df.sort_values(by=['value_in_inr'], ascending=0, inplace=True)
    total_exposure_in_cr = df['value_in_inr'].sum()

    resp["result"] = df.to_dict(orient='records')
    resp["total_records"] = total_records
    resp["total_exposure_in_cr"] = total_exposure_in_cr

    return jsonify(resp)


