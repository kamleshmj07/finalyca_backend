from datetime import datetime
import imp
from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import desc, func, or_
from utils.utils import get_unsafe_db_engine
from werkzeug.exceptions import BadRequest
from .data_access import *
from .utils import required_user_access_pro

exposure_sp_bp = Blueprint("exposure_sp_bp", __name__)

def temp_fund_stock_tp_json(sql_obj):
    obj = dict()
    obj["AMC_Id"] = sql_obj.AMC_Id
    obj["AMC_Name"] = sql_obj.AMC_Name
    obj["AMC_Logo"] = sql_obj.AMC_Logo
    obj["Fund_Id"] = sql_obj.Fund_Id
    obj["Plan_Id"] = sql_obj.Plan_Id
    obj["Plan_Name"] = sql_obj.Plan_Name
    obj["Product_Id"] = sql_obj.Product_Id
    obj["Product_Name"] = sql_obj.Product_Name
    obj["Classification_Id"] = sql_obj.Classification_Id
    obj["Classification_Name"] = sql_obj.Classification_Name
    obj["Percentage_to_AUM"] = sql_obj.Percentage_to_AUM
    obj["Value_In_Cr"] = sql_obj.Value_In_Cr

    return obj

@exposure_sp_bp.route('/api/v1/exposure/security')
@required_user_access_pro
def get_security_exposure():
    security_id = request.args.get("id", type=int, default=None)
    if not security_id:
        raise BadRequest(description="No security id was provided")

    Classification_Id = request.args.get("Classification_Id", type=int, default=None)
    Product_Id = request.args.get("Product_Id", type=int, default=None)
    AMC_Id = request.args.get("AMC_Id", type=int, default=None)
    Bucket_Id = request.args.get("Bucket_Id", type=int, default=None)

    storedProc = "Exec [PMS_Base].[Logics].[Get_SecurityHoldingDetails] @HoldingSecurity_Id = ?, @Sector_Id = ?, @Issuer_Id = ?, @AMC_Id = ?, @Product_Id = ?, @Classification_Id = ?, @Bucket_Id = ?"
    params = ( security_id, None, None, AMC_Id, Product_Id, Classification_Id, Bucket_Id)

    results = list()
    # db_session = current_app.store.db()
    # engine = db_session.bind.engine
    engine = get_unsafe_db_engine(current_app.config)
    connection = engine.raw_connection()
    more_results = list()
    try:
        cursor_obj = connection.cursor()
        # If there is a stored procedure that inserts data into table (let it be a temp table) and then returns some data back, make sure 'SET NOCOUNT ON' line is added after BEGIN statement else adapter will not consider it as a proper SQL and will keep on giving exception.
        cursor_obj.execute(storedProc, params)

        col_names = [i[0] for i in cursor_obj.description]
        results = [dict(zip(col_names, row)) for row in cursor_obj]

        while cursor_obj.nextset():
            columns = [column[0] for column in cursor_obj.description]
            rrr = [dict(zip(columns, row)) for row in cursor_obj]
            more_results.append(rrr)

        cursor_obj.close()
        connection.commit()
    finally:
        connection.close()

    resp = dict()
    resp["total_funds"] = len(results)
    resp["data"] = results

    if len(more_results) > 2:
        resp["total_funds"] = more_results[0][0]["NoOfSchemes"]
        resp["highest_weight"] = more_results[0][0]
        resp["highest_value"] = more_results[1][0]

    return jsonify(resp)
    
@exposure_sp_bp.route('/api/v1/exposure/issuer')
@required_user_access_pro
def get_issuer_exposure():
    issuer_id = request.args.get("id", type=int, default=None)
    if not issuer_id:
        raise BadRequest(description="No issuer id was provided")

    Classification_Id = request.args.get("Classification_Id", type=int, default=None)
    Product_Id = request.args.get("Product_Id", type=int, default=None)
    AMC_Id = request.args.get("AMC_Id", type=int, default=None)
    Bucket_Id = request.args.get("Bucket_Id", type=int, default=None)

    storedProc = "Exec [PMS_Base].[Logics].[Get_SecurityHoldingDetails] @HoldingSecurity_Id = ?, @Sector_Id = ?, @Issuer_Id = ?, @AMC_Id = ?, @Product_Id = ?, @Classification_Id = ?, @Bucket_Id = ?"
    params = ( None, None, issuer_id, AMC_Id, Product_Id, Classification_Id, Bucket_Id)

    results = list()
    # db_session = current_app.store.db()
    # engine = db_session.bind.engine
    engine = get_unsafe_db_engine(current_app.config)
    connection = engine.raw_connection()
    more_results = list()
    try:
        cursor_obj = connection.cursor()
        # If there is a stored procedure that inserts data into table (let it be a temp table) and then returns some data back, make sure 'SET NOCOUNT ON' line is added after BEGIN statement else adapter will not consider it as a proper SQL and will keep on giving exception.
        cursor_obj.execute(storedProc, params)

        col_names = [i[0] for i in cursor_obj.description]
        results = [dict(zip(col_names, row)) for row in cursor_obj]

        while cursor_obj.nextset():
            columns = [column[0] for column in cursor_obj.description]
            rrr = [dict(zip(columns, row)) for row in cursor_obj]
            more_results.append(rrr)

        cursor_obj.close()
        connection.commit()
    finally:
        connection.close()

    resp = dict()
    # resp["total_funds"] = len(results)
    resp["data"] = results

    if len(more_results) > 2:
        resp["total_funds"] = more_results[0][0]["NoOfSchemes"]
        resp["highest_weight"] = more_results[0][0]
        resp["highest_value"] = more_results[1][0]

    return jsonify(resp)

@exposure_sp_bp.route('/api/v1/exposure/sector')
@required_user_access_pro
def get_sector_exposure():
    sector_id = request.args.get("id", type=int, default=None)
    if not sector_id:
        raise BadRequest(description="No sector id was provided")

    Classification_Id = request.args.get("Classification_Id", type=int, default=None)
    Product_Id = request.args.get("Product_Id", type=int, default=None)
    AMC_Id = request.args.get("AMC_Id", type=int, default=None)
    Bucket_Id = request.args.get("Bucket_Id", type=int, default=None)

    storedProc = "Exec [PMS_Base].[Logics].[Get_SecurityHoldingDetails] @HoldingSecurity_Id = ?, @Sector_Id = ?, @Issuer_Id = ?, @AMC_Id = ?, @Product_Id = ?, @Classification_Id = ?, @Bucket_Id = ?"
    params = ( None, sector_id, None, AMC_Id, Product_Id, Classification_Id, Bucket_Id)

    results = list()
    # db_session = current_app.store.db()
    # engine = db_session.bind.engine
    engine = get_unsafe_db_engine(current_app.config)
    connection = engine.raw_connection()
    more_results = list()
    try:
        cursor_obj = connection.cursor()
        # If there is a stored procedure that inserts data into table (let it be a temp table) and then returns some data back, make sure 'SET NOCOUNT ON' line is added after BEGIN statement else adapter will not consider it as a proper SQL and will keep on giving exception.
        cursor_obj.execute(storedProc, params)

        col_names = [i[0] for i in cursor_obj.description]
        results = [dict(zip(col_names, row)) for row in cursor_obj]

        while cursor_obj.nextset():
            columns = [column[0] for column in cursor_obj.description]
            rrr = [dict(zip(columns, row)) for row in cursor_obj]
            more_results.append(rrr)

        cursor_obj.close()
        connection.commit()
    finally:
        connection.close()

    resp = dict()
    resp["data"] = results
    resp["total_funds"] = None
    resp["highest_weight"] = None
    resp["highest_value"] = None

    if len(more_results) > 2 and len(more_results[0]) > 0 and len(more_results[1]) > 0:
        resp["total_funds"] = more_results[0][0]["NoOfSchemes"]
        resp["highest_weight"] = more_results[0][0]
        resp["highest_value"] = more_results[1][0]

    return jsonify(resp)
