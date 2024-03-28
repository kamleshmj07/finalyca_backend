from flask import jsonify, request, Blueprint, current_app, jsonify, request, send_file
from werkzeug.exceptions import BadRequest
import csv
import datetime
import logging

from src.utils import get_user_info
from compass.model_portfolio import *

portfolio_model_bp = Blueprint("portfolio_model_bp", __name__)

@portfolio_model_bp.route('/portfolio_model/sample_holdings_file', methods=["GET"])
def api_get_holding_sample():
    sample_file = "../sample/model_portfolio_holdings.csv"
    return send_file(sample_file)

@portfolio_model_bp.route('/portfolio_model/sample_returns_file', methods=["GET"])
def api_get_returns_sample():
    sample_file = "../sample/model_portfolio_returns.csv"
    return send_file(sample_file)

@portfolio_model_bp.route('/portfolio_model', methods=["POST"])
def api_save_model_portfolio():
    try:
        res = dict()
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        user_id = user_info["id"]
        org_id = user_info["org_id"]

        incoming_data = request.form

        name = incoming_data["name"]
        description = incoming_data.get("description")
        portfolio_date_str = incoming_data.get("as_on_date")
        portfolio_date = datetime.datetime.fromisoformat(portfolio_date_str)

        holdings_file = request.files["holdings"]
        returns_file = request.files["returns"]

        holdings = list()
        holdings_header = ["isin", "name", "weight"]

        returns = list()
        returns_header = ["date", "monthly_returns"]
        
        # portfolio holdings
        try:
            holding_csv = holdings_file.read().decode('utf-8')
            csvreader = csv.reader(holding_csv.splitlines())
            is_first_line = True
            for row in csvreader:
                # continue if empty row
                if not ''.join(row).strip():
                    continue

                if is_first_line:
                    is_first_line = False
                    continue
                else:
                    row1 = { holdings_header[idx]: cell.strip(' ') for idx, cell in enumerate(row) }
                    holdings.append(row1)

            # portfolio returns -> Could be empty
            return_csv = returns_file.read().decode('utf-8')
            csvreader = csv.reader(return_csv.splitlines())
            is_first_line = True
            for row in csvreader:
                if is_first_line:
                    is_first_line = False
                    continue
                else:
                    row1 = { returns_header[idx]: cell.strip(' ') for idx, cell in enumerate(row) }
                    returns.append(row1)
        except IndexError:
            raise BadRequest("Please ensure the file you are uploading is following the samples provided")

        try:
            portfolio_id = save_model_portfolio(current_app.store.db, name, description, portfolio_date, holdings, returns, user_id)
        except DataFormatException as exe:
            raise BadRequest("Please ensure the file you are uploading is following the samples provided")
    except Exception as err:
        logging.exception(err)


    return jsonify({"id": portfolio_id})

@portfolio_model_bp.route('/portfolio_model', methods=["GET"])
def api_get_all_model_portfolio():
    # with user id and org id
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    resp = get_model_portfolios(current_app.store.db, user_id)
    return jsonify(resp)

@portfolio_model_bp.route('/portfolio_model/<int:id>', methods=["GET"])
def api_get_model_portfolio(id):
    # with user id and org id and available dates for the holdings
    resp = get_one_model_portfolios(current_app.store.db, id)
    return jsonify(resp)

@portfolio_model_bp.route('/portfolio_model/<int:id>', methods=["PUT"])
def api_edit_model_portfolio(id):
    # with user id and org id and available dates for the holdings
    res = dict()
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    incoming_data = request.form

    description = incoming_data.get("description")
    portfolio_date = incoming_data.get("as_on_date")

    holdings_file = request.files["holdings"]
    returns_file = request.files["returns"]

    holdings = list()
    holdings_header = ["isin", "name", "weight"]

    returns = list()
    returns_header = ["date", "monthly_returns"]
    
    # portfolio holdings
    holding_csv = holdings_file.read().decode('utf-8')
    # csvreader = csv.DictReader(holding_csv.splitlines())
    csvreader = csv.reader(holding_csv.splitlines())
    is_first_line = True
    for row in csvreader:
        if is_first_line:
            is_first_line = False
            continue
        else:
            # row1 = { k.strip(' '): v.strip(' ')  for k, v in row.items()}
            row1 = { holdings_header[idx]: cell.strip(' ') for idx, cell in enumerate(row) }
            holdings.append(row1)

    # portfolio returns -> Could be empty
    return_csv = returns_file.read().decode('utf-8')
    # csvreader = csv.DictReader(return_csv.splitlines())
    csvreader = csv.reader(return_csv.splitlines())
    is_first_line = True
    for row in csvreader:
        if is_first_line:
            is_first_line = False
            continue
        else:
            # row1 = { k.strip(' '): v.strip(' ')  for k, v in row.items()}
            row1 = { returns_header[idx]: cell.strip(' ') for idx, cell in enumerate(row) }
            returns.append(row1)

    portfolio_id = edit_model_portfolio(current_app.store.db, id, description, portfolio_date, holdings, returns, user_id)

    return jsonify({"id": portfolio_id})

@portfolio_model_bp.route('/portfolio_model/<int:id>', methods=["DELETE"])
def api_delete_model_portfolio(id):
    # with user id and org id and available dates for the holdings
    resp = delete_model_portfolio(current_app.store.db, id)

    return jsonify({"id": resp})
