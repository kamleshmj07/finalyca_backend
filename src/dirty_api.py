from flask import Blueprint, current_app, jsonify, request
from utils.utils import get_unsafe_db_engine

dirty_api_bp = Blueprint("dirty_api_bp", __name__)

@dirty_api_bp.route("/api/v1/client/fund_mining", methods=['POST'])
def get_fund_mining():
    f = request.json
    req = f["RequestObject"]

    plan_id = 0

    storedProc = "Exec [PMS_Base].[Logics].[Get_FundMining] @Product = ?, @AssetClass=?, @Classification=?, @AMC=?, @Filters=?, @Bucket_Id=?, @Sort_By=?, @Sort_Type=?, @Plan_Id=?"
    params = ( req["Product"], req["AssetClass"], req["Classification"], req["AMC"], req["QueryFilters"], req["Bucket_Id"], req["Sort_By"], req["Sort_Type"],  plan_id)

    results = list()

    # db_session = current_app.store.db()
    # engine = db_session.bind.engine
    engine = get_unsafe_db_engine(current_app.config)
    connection = engine.raw_connection()
    try:
        cursor_obj = connection.cursor()
        # If there is a stored procedure that inserts data into table (let it be a temp table) and then returns some data back, make sure 'SET NOCOUNT ON' line is added after BEGIN statement else adapter will not consider it as a proper SQL and will keep on giving exception.
        cursor_obj.execute(storedProc, params)

        col_names = [i[0] for i in cursor_obj.description]
        results = [dict(zip(col_names, row)) for row in cursor_obj]

        cursor_obj.close()
        connection.commit()
    finally:
        connection.close()

    return jsonify(results)

@dirty_api_bp.route("/api/v1/client/fund_compare", methods=['POST'])
def get_fund_compare():
    f = request.json
    req = f["RequestObject"]

    storedProc = "Exec [PMS_Base].[Logics].[Get_FundCompare] @Plans = ?"
    params = ( req["Plans"])

    results = list()
    # db_session = current_app.store.db()
    # engine = db_session.bind.engine
    engine = get_unsafe_db_engine(current_app.config)
    connection = engine.raw_connection()
    try:
        cursor_obj = connection.cursor()
        # If there is a stored procedure that inserts data into table (let it be a temp table) and then returns some data back, make sure 'SET NOCOUNT ON' line is added after BEGIN statement else adapter will not consider it as a proper SQL and will keep on giving exception.
        cursor_obj.execute(storedProc, params)

        col_names = [i[0] for i in cursor_obj.description]
        results = [dict(zip(col_names, row)) for row in cursor_obj]

        cursor_obj.close()
        connection.commit()
    finally:
        connection.close()

    return jsonify(results)

@dirty_api_bp.route("/client/token", methods=['GET'])
def get_token():
    ip_address = request.access_route

    return jsonify({"address": ip_address})

