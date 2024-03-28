import pandas as pd
from itsdangerous import json
from datetime import date as dt, timedelta
from flask import Blueprint, current_app, request, jsonify
from sqlalchemy import func, desc

from fin_models.masters_models import DebtSecurity, HoldingSecurity
from bizlogic.fixed_income_analysis import get_fi_issuer_characteristic
from bizlogic.fixed_income_db_helper import get_latest_available_price_of_securities

fixed_income_dashboard_bp = Blueprint('fixed_income_dashboard_bp', __name__)


@fixed_income_dashboard_bp.route('/api/v1/fi_issuer_coverage', methods=['GET'])
def get_fi_issuer_coverage():

    db_session = current_app.store.db
    sql_obj = db_session.query(DebtSecurity.Issuer_Type,
                               func.max(DebtSecurity.Issuer_Type_Code).label('Issuer_Type_Code'),
                               func.count(DebtSecurity.Bilav_Internal_Issuer_Id).label('Total_Count'))\
                        .join(HoldingSecurity, HoldingSecurity.Co_Code == func.concat('BLV_', DebtSecurity.Bilav_Code))\
                        .filter(HoldingSecurity.Maturity_Date >= dt.today())\
                        .group_by(DebtSecurity.Issuer_Type)

    df = pd.DataFrame(sql_obj)

    result = df.to_json(orient='records')
    parsed = json.loads(result)

    return jsonify(parsed)


@fixed_income_dashboard_bp.route('/api/v1/fi_sec_by_size', methods=['GET'])
def get_fi_securities_by_size():
    fallback_days = request.args.get('fallback_days', type=int, default=30) # number of days to fallback for latest issuance search
    limit = request.args.get('limit', type=int, default=None)
    page = request.args.get('page', type=int, default=0)

    today = dt.today()
    compare_date = today - timedelta(days=fallback_days)

    db_session = current_app.store.db
    sql_obj = db_session.query(DebtSecurity.Security_Name,
                               DebtSecurity.DebtSecurity_Id,
                               DebtSecurity.Issue_Size)\
                        .join(HoldingSecurity, HoldingSecurity.Co_Code == func.concat('BLV_', DebtSecurity.Bilav_Code))\
                        .filter(HoldingSecurity.Maturity_Date >= dt.today(),
                                DebtSecurity.Issue_Date >= compare_date,
                                HoldingSecurity.Is_Deleted != 1,
                                HoldingSecurity.active == 1)\
                        .order_by(desc(DebtSecurity.Issue_Size))

    total_records = sql_obj.count()

    if page >= 0 and limit:
        offset = page*limit
        sql_obj = sql_obj.offset(offset)
        sql_obj = sql_obj.limit(limit)

    df = pd.DataFrame(sql_obj)

    resp = {}
    result = df.to_json(orient='records')
    parsed = json.loads(result)
    resp["result"] = parsed
    resp["total_records"] = total_records
    return jsonify(resp)


@fixed_income_dashboard_bp.route('/api/v1/fi_issuers_by_size', methods=['GET'])
def get_fi_issuers_by_size():
    # TODO Pagination logic to be implemented for the function get_fi_issuer_characteristic(..) 
    # and subsequent changes need to be done on other references
    fallback_days = request.args.get('fallback_days', type=int, default=30) # number of days to fallback for latest issuance search
    limit = request.args.get('limit', type=int, default=5)
    asc = request.args.get('asc', type=int, default=0)

    today = dt.today()
    compare_date = today - timedelta(days=fallback_days)

    filters = [
        {
            'column' : 'Issue_Date',
            'value' : compare_date
        }
    ]

    db_session = current_app.store.db
    df = get_fi_issuer_characteristic(db_session, filters)
    
    result = '[]'
    if not df.empty:
        df.sort_values('Total_Issue_Size', inplace=True, ascending=asc)
        result = df[['Bilav_Internal_Issuer_Id','Issuer', 'Total_Issue_Size']].head(limit).to_json(orient='records')

    parsed = json.loads(result)

    return jsonify(parsed)


@fixed_income_dashboard_bp.route('/api/v1/fi_newly_listed_securities', methods=['GET'])
def get_fi_newly_listed_securities():
    fallback_days = request.args.get('fallback_days', type=int, default=30) # number of days to fallback for latest issuance search
    limit = request.args.get('limit', type=int, default=None)
    page = request.args.get('page', type=int, default=0)

    today = dt.today()
    compare_date = today - timedelta(days=fallback_days)

    db_session = current_app.store.db
    total_records = 0
    while not total_records:
        sql_obj = db_session.query(DebtSecurity.Security_Name,
                                   DebtSecurity.Issue_Date,
                                   DebtSecurity.Issue_Price,
                                   DebtSecurity.DebtSecurity_Id)\
                            .filter(DebtSecurity.Issue_Date >= compare_date)\
                            .order_by(desc(DebtSecurity.Issue_Date))

        compare_date -= timedelta(days=fallback_days)
        total_records = sql_obj.count()

    if page >= 0 and limit:
        offset = page*limit
        sql_obj = sql_obj.offset(offset)
        sql_obj = sql_obj.limit(limit)

    df = pd.DataFrame(sql_obj)
    resp = {}
    resp["result"] = df.to_dict(orient="records")
    resp["total_records"] = total_records

    return jsonify(resp)

