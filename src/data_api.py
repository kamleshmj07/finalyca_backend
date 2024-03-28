from datetime import datetime
from flask import Blueprint, current_app, jsonify, request, send_file
from sqlalchemy import desc, or_, and_
from werkzeug.exceptions import BadRequest
from utils import pretty_float
from bizlogic.core_helper import get_security_info, get_mf_breakup

data_api_bp = Blueprint("data_api_bp", __name__)

@data_api_bp.route('/api/v1/securities', methods=["GET"])
def get_security_info_api():
    isin_list = request.args.getlist('isin')
    if not isin_list:
        raise BadRequest("Please provide at least one ISIN")

    data = get_security_info(current_app.store.db, isin_list)
    resp = list(data.values())
    return jsonify(resp)

@data_api_bp.route('/api/v1/mf_holdings', methods=["GET"])
def get_mf_breakup_api():
    isin_list = request.args.getlist('isin')
    portfolio_month = request.args.get('month', type=int)
    portfolio_year = request.args.get('year', type=int)
    if not isin_list:
        raise BadRequest("Please provide at least one ISIN")

    if not portfolio_month or not portfolio_year:
        raise BadRequest("Please provide the month and year for the breakup")


    data = get_mf_breakup(current_app.store.db, isin_list, portfolio_month, portfolio_year)
    resp = list(data.values())
    return jsonify(resp)
