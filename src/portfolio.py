from io import BytesIO
from flask import jsonify, request, Blueprint, current_app, jsonify, request, send_file
import json
import datetime
from datetime import datetime, date 
import pandas as pd
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest
import logging 
from jinja2 import Environment, FileSystemLoader, Template
import xml.etree.ElementTree as ET
from pathlib import Path

from cas_parser import NSDLParser, CDSLParser
from pdfminer.pdfdocument import PDFPasswordIncorrect

from compass.portfolio_helper import *
from compass.portfolio_db import *
from compass.portfolio_analysis import *
from compass.portfolio_crud import *
from compass.investor_portfolio import *
from compass.model_portfolio import get_one_model_portfolios

from src.utils import get_user_info
from utils.utils import validate_jwt_token
from fin_models.masters_models import BenchmarkIndices

from bizlogic.core_helper import prepare_index_holdings_from_db
from bizlogic.importer_helper import prepare_plan_holdings, find_portfolio_overlap_v2, get_organizationid_by_userid, get_organization_whitelabel, get_performancetrend_data
from bizlogic.common_helper import get_benchmark_trailing_returns_for_all_period, get_benchmarkdetails
from compass.portfolio_pdf_helper import get_investor_portfolio_history, get_portfoliooverlappdf


from fin_models.servicemanager_models import DeliveryRequest
from bizlogic.common_helper import object_to_xml, get_plan_meta_info

portfolio_bp = Blueprint("portfolio_bp", __name__)


@portfolio_bp.route('/portfolio/parse_ecas', methods=["POST"])
def process_ecas():
    logging.warning(F"parse_ecas start" )
    is_test = request.args.get("test", type=int)

    if is_test:
        # Read file
        file_path = "sample/CDSL.json"
        content = dict()
        with open(file_path) as json_file:
            content = json.load(json_file)
    else:
        logging.warning(F"parse_ecas start else" )
        pdf_file = request.files.get("file")
        pdf_type = request.form.get("file_type")
        pdf_pwd = request.form.get("password")

        if pdf_type == "NSDL":
            parser = NSDLParser()
        elif pdf_type == "CDSL":
            logging.warning(F"parse_ecas before cdslparser" )
            parser = CDSLParser()
            logging.warning(F"parse_ecas after cdslparser" )

        try:
            owners = parser.parse(pdf_file, password=pdf_pwd)
            content = owners.export_to_json(True)
        except PDFPasswordIncorrect:
            raise BadRequest("Password provided for the PDF is Incorrect.")

    return jsonify(content)


@portfolio_bp.route('/portfolio/excel_portfolio', methods=["POST"])
def save_excel_portfolio():
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    # Get excel statement and the date
    investor_name = request.form.get("investor_name")
    investor_pan = request.form.get("investor_pan")
    portfolio_date = request.form.get("portfolio_date")
    portfolio = request.files.get("portfolio")
    label = request.files.get("label", default=investor_name)
    linked_pms_code = request.files.get("linked_pms_code")

    primary_investor_id = find_or_create_investor(current_app.store.db, investor_name, investor_pan, investor_name, user_id)

    owners = [{"PAN": investor_pan, "name": investor_name}]

    account_id = find_or_create_dummy_investor_account(current_app.store.db, primary_investor_id, owners, label, linked_pms_code, user_id)

    # read Excel file and store the holdings
    cols = ["S.No", "ISIN Code", "Security Name", "Units", "Total Price (INR)", "Holding Type", "Sector", "Sub-sector", "Derivative", "Equity Market Cap", "Debt Credit Rating"]
    df = pd.read_excel(portfolio, sheet_name='Holdings')

    #Process Holdings
    df = df[cols]
    df = df[df['Security Name'].notna()]

    # Clean the duplicates for ISIN
    demats = df[df['ISIN Code'].notna()]
    non_demats = df[df['ISIN Code'].isna()]

    df1 = demats.groupby(
        ['ISIN Code'], dropna=False, as_index=False
        ).agg(
        **{
            'ISIN Code' : ("ISIN Code", "first"),
            'Security Name' : ("Security Name", "first"), 
            'Units' : ("Units", "sum"),
            'Total Price (INR)' : ("Total Price (INR)", "sum"),
            'Holding Type' : ("Holding Type", "first"),
            'Sector' : ("Sector", "first"),
            'Sub-sector' : ("Sub-sector", "first"),
            'Derivative' : ("Derivative", "first"),
            'Equity Market Cap' : ("Equity Market Cap", "first"),
            'Debt Credit Rating' : ("Debt Credit Rating", "first"),
        }
    )

    df = pd.concat([df1, non_demats],ignore_index=True)

    df = df.fillna(0)
    holdings = df.to_dict(orient="records")

    for holding in holdings:
        isin = holding["ISIN Code"] if holding["ISIN Code"] else None
        name = holding["Security Name"]
        holding_type = holding["Holding Type"]
        units = holding["Units"]
        total_price = holding["Total Price (INR)"]
        unit_price = total_price/units
        find_or_create_investor_holding(current_app.store.db, account_id, portfolio_date, isin, name, holding_type, None, None, units, unit_price, total_price, user_id)

    #Process Transactions
    cols = ["ISIN Code", "Security Name", "Transaction Type", "Transaction Date (dd-mm-yyyy)", "Price Per Unit (INR)", "Units", "Total Price (INR)", "Holding Type"]
    tran_df = pd.read_excel(portfolio, sheet_name='Transactions')
    if not tran_df.empty:
        tran_df = tran_df[cols]
        tran_df = tran_df[tran_df['ISIN Code'].notna()] #Consider only demats 

        tran_df = tran_df.fillna(0)
        tranactions = tran_df.to_dict(orient="records")

        for transaction in tranactions:
            isin = transaction["ISIN Code"] if transaction["ISIN Code"] else None
            name = transaction["Security Name"]
            transaction_type = 'S' if transaction["Transaction Type"] == 'Sell' else 'B'
            holding_type = transaction["Holding Type"]
            tran_date = transaction["Transaction Date (dd-mm-yyyy)"]
            total_price = transaction["Total Price (INR)"]
            units = transaction["Units"]
            unit_price = transaction["Price Per Unit (INR)"] if transaction["Price Per Unit (INR)"] else total_price/units        

            find_or_create_investor_transactions(current_app.store.db, account_id, tran_date, isin, name, holding_type, transaction_type, units, unit_price, total_price, user_id)
        
        #Validations
        sql_transactions = get_investor_transactions(current_app.store.db, [account_id])

        tran_df_val = pd.DataFrame(sql_transactions)
        validate_investor_transactions(current_app.store.db, tran_df_val)
        
    resp = {'msg': F"Excel statement of {investor_name} for {portfolio_date} has been uploaded successfully."}
    return jsonify(resp)
    

@portfolio_bp.route('/portfolio/portfolio', methods=["POST"])
def save_portfolio():
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    statement = request.json

    # Check first holder of first account. This will be the primary investor.
    primary_investor_id = None
    primary_investor_name = None
    as_on_date = None
    for obj in statement:
        if not as_on_date:
            as_on_date = obj["as_on_date"]

        investors = obj["investors"]
        for investor in investors:
            name = investor["name"]
            pan_no = investor["PAN"]
            if not primary_investor_id:
                primary_investor_id = find_or_create_investor(current_app.store.db, name, pan_no, name, user_id)
                primary_investor_name = name

        demats = obj["demat_accounts"]
        for acc in demats:
            # TODO: add user id in created and edited by
            account_id = find_or_create_investor_account(current_app.store.db, primary_investor_id, investors, "demat", acc["depository"], acc["dp_name"], acc["client_id"], acc["label"], acc["linked_pms_code"], user_id)

            holdings = acc["holdings"]
            for holding in holdings:
                holding_id = find_or_create_investor_holding(current_app.store.db, account_id, as_on_date, holding["isin"],  holding["name"], holding["type"], holding["coupon_rate"], holding["maturity_date"], holding["units"], holding["unit_value"], holding["total_value"], user_id)

        folios = obj["folio_accounts"]
        for acc in folios:
            account_id = find_or_create_investor_account(current_app.store.db, primary_investor_id, investors, "folio", None, None, acc["folio_no"], acc["label"], None, user_id)
            
            holdings = acc["holdings"]
            for holding in holdings:
                holding_id = find_or_create_investor_holding(current_app.store.db, account_id, as_on_date, holding["isin"],  holding["name"], holding["type"], holding["coupon_rate"], holding["maturity_date"], holding["units"], holding["unit_value"], holding["total_value"], user_id)

    msg = F"eCAS statement of {primary_investor_name} for {as_on_date} has been uploaded successfully."
    resp = {"msg": msg, "investor_id": primary_investor_id}

    return jsonify(resp)

@portfolio_bp.route('/portfolio/investors', methods=["GET"])
def api_get_investors():
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]
    resp = get_all_investors(current_app.store.db, user_id)

    return jsonify(resp)


@portfolio_bp.route('/portfolio/investors/<int:investor_id>', methods=["GET"])
def get_investor_by_id(investor_id):
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    investor = get_investor_dashboard_info(current_app.store.db, investor_id, user_id)
    return jsonify(investor)


@portfolio_bp.route('/portfolio/investors/<int:investor_id>', methods=['DELETE'])
def delete_investor_by_id(investor_id):
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    id = delete_investor(current_app.store.db, investor_id, user_id)
    resp = dict()
    resp["msg"] = "Successfully deleted investor" if id else "Investor not deleted"
    return jsonify(resp)


@portfolio_bp.route('/portfolio/investors/<int:investor_id>', methods=['PUT'])
def update_investor_by_id(investor_id):
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    investor_data = request.json if request.json else request.form
    name = investor_data['owners'][0]['name']
    id = update_investor(current_app.store.db, investor_id, user_id, name, investor_data["label"])
    resp = dict()
    resp["msg"] = "Successfully updated investor" if id else "Investor not updated"
    return jsonify(resp)


@portfolio_bp.route('/portfolio/investoraccount/<int:investor_account_id>', methods=["PUT"])
def update_investor_account_by_investor_account_id(investor_account_id):
    db_session = current_app.store.db
    user_info = get_user_info(request, db_session, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]

    investor_data = request.json
    id = update_investor_account(db_session, investor_account_id, investor_data, user_id)

    resp = dict()
    resp["msg"] = "Successfully Updated Investor Details." if id else "Investor Details Not Updated."

    return jsonify(resp)


@portfolio_bp.route('/portfolio/investor/holdings_analysis', methods=["GET"])
def api_get_investor_holding_analysis():
    # list of demat ids
    account_ids = request.args.getlist('account_id', type=int)

    # date
    date_str = request.args.get('date')

    portfolio_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    detailed_analysis = request.args.get("is_detailed", type=int)
    export_csv = request.args.get('export_csv', type=int)
    # pdf_export = request.args.get('export_pdf', type=int)
    send_email = request.args.get('is_email', type=int, default=0)
    is_historic = request.args.get('is_historic', type=int)
    investor_id = get_investorid_by_account_id(current_app.store.db, account_ids[0])
    #send x_user_id in header
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    # user_id = 20237

    holdings = prepare_raw_holdings_from_db(current_app.store.db, account_ids, portfolio_date, detailed_analysis)

    if export_csv:
        response = create_portfolio_report_only(holdings, False)

        output = BytesIO()
        with pd.ExcelWriter(output) as writer:
            for key, exposure in response.items():
                exposure.to_excel(writer, sheet_name=key, float_format="%.2f", index=False)
        output.seek(0)
        return send_file(output, attachment_filename="testing.xlsx", as_attachment=True)

    # elif pdf_export:
    #     results = create_portfolio_report_only(holdings, False)

    #     response = dict()
    #     for key, exposure in results.items():
    #         response[key] = exposure.to_dict(orient="records")

    #     response['portfolio_history'] = get_investor_portfolio_history(current_app.store.db, account_ids, None, 0)
    #     response['investor_details'] = get_investor_dashboard_info(current_app.store.db, investor_id, user_id)
    #     response['is_historic'] = is_historic
    #     response['portfolio_date'] = datetime.datetime.strftime(portfolio_date,'%d %b %Y')
        
    #     image_path = current_app.config['IMAGE_PATH']
    #     whitelabel_dir = current_app.config['WHITELABEL_DIR']
    #     generatereport_dir = current_app.config['REPORT_GENERATION_PATH']
    #     organization_id = get_organizationid_by_userid(current_app.store.db, user_id)

    #     file_name = get_portfoliopdf(current_app.store.db, organization_id, image_path, whitelabel_dir, generatereport_dir, response)

    #     if send_email:
    #         email_id = request.args['email_id']
    #         template_vars = dict()
    #         organization_whitelabel_data = get_organization_whitelabel(current_app.store.db, organization_id)
    #         template_vars["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{organization_whitelabel_data['logo']}"        
    #         template_vars["customer_name"] = user_info["user_name"]
    #         template_vars["organization_name"] = user_info["org_name"]        
    #         template_vars["report_name"] = "Portfolio Report"

    #         environment = Environment(loader=FileSystemLoader('./src/templates'), keep_trailing_newline=False, trim_blocks=True, lstrip_blocks=True)

    #         template = environment.get_template('pdf_fundcompare_email_template.html')
    #         html_body = template.render(template_vars)
    #         subject = F"Portfolio Report"

    #         attachements = list()
    #         attachements.append(file_name)
    #         schedule_email_activity(current_app.store.db, email_id, '', '', subject, html_body, attachements)

    #         resp = dict()
    #         resp["msg"] = 'Report shared successfully.'
    #         return resp
                
        return send_file(file_name, attachment_filename="portfolio.pdf")

    else:
        results = create_portfolio_report_only(holdings,
                                               only_important_field=False,
                                               drop_unimp_cols=False)
        results['accounts'] = get_account_aggregation_report(holdings)

        response = dict()
        for key, exposure in results.items():
            response[key] = exposure.to_dict(orient="records") if key not in ['accounts', 'sectors'] else exposure

        return jsonify(response)


@portfolio_bp.route('/portfolio/investor/holdings_movement', methods=["GET"])
def api_get_investor_holdings_movement():
    # list of demat ids
    account_ids = request.args.getlist('account_id', type=int)

    # date
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
   
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')

    detailed_analysis = request.args.get("is_detailed", type=int)
    to_export = request.args.get('export', type=int)
    
    portfolio_movement_df = get_portfolio_movement(current_app.store.db, account_ids, start_date, end_date, detailed_analysis)
    res = portfolio_movement_df.to_dict(orient="records")
    return jsonify(res)


@portfolio_bp.route('/portfolio/investor/performance_analysis', methods=["GET"])
def api_get_investor_performance_analysis():
    # list of demat ids
    account_ids = request.args.getlist('account_id', type=int)
    benchmark_id = request.args.get('benchmark_id', type=int)

    # date
    date_str = request.args.get('date')
    portfolio_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    start_date = end_date = None

    start_date_str = request.args.get('start_date')
    if start_date_str:
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
    else:
        start_date = portfolio_date - relativedelta(years=1)

    end_date_str = request.args.get('end_date')
    if end_date_str:
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
    else:
        end_date = portfolio_date

    response = dict()
    db_session = current_app.store.db
    holdings_df = get_normalized_portfolio_holdings(db_session, account_ids, portfolio_date, False)
    transactions = get_investor_transactions(current_app.store.db, account_ids)

    response = get_portfolio_performance_analysis_by_date(db_session, holdings_df, start_date, end_date, transactions)


    # TODO Make this live on Philip's confirmation
    # response['benchmark_pe'] = get_benchmark_pe_ratio_for_a_date(db_session, benchmark_id, end_date)

    return jsonify(response)


@portfolio_bp.route('/portfolio/investor/portfolio_history', methods=["GET"])
def api_get_investor_portfolio_history():
    # list of demat ids
    account_ids = request.args.getlist('account_id', type=int)

    # date
    detailed_analysis = request.args.get("is_detailed", type=int)
    
    response = dict()
    response = get_investor_portfolio_history(current_app.store.db, account_ids, None, detailed_analysis)
        
    return jsonify(response)


@portfolio_bp.route('/portfolio/investor/portfolio_overlap', methods=["GET"])
def api_get_investor_portfolio_overlap():
    # list of demat ids
    account_ids = request.args.getlist('account_id', type=int)
    # plan_id = request.args.get('plan_id', type=int)
    plan_id_list =  request.args.getlist("plan_id", type=int) # list of plan ids
    detailed_analysis = request.args.get("is_detailed", type=int)
    # date
    date_str = request.args.get('date')
    portfolio_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    export_report = request.args.get('export', type=int)   
    benchmark_id = request.args.get('benchmark_id', type=int)

    db_session = current_app.store.db
    #send x_user_id in header
    user_info = get_user_info(request, db_session, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    # user_id = 20237
    fund_portfolio_date = None
    securities_df = get_normalized_portfolio_holdings(db_session, account_ids, portfolio_date, detailed_analysis)
    cols = [ "total_price", "units", "instrument_type", "asset_class", "issuer", "sub_sector", "market_cap", "equity_style", "risk_category", "account_alias", "unit_price", "coupon_rate", "maturity", "portfolio_date" ]
    securities_light = securities_df.drop(cols, axis=1)

    all_plans_df = prepare_plan_holdings(db_session, plan_id_list)
    plan_df = all_plans_df[plan_id_list[0]]
    if not plan_df.empty:
        fund_portfolio_date = datetime.datetime.strftime(plan_df.iloc[0]['portfolio_date'], '%d %b %Y')
    # response = find_portfolio_overlap(securities_light, plan_df, True)
    # response = find_portfolio_overlap_v2(securities_light, plan_df, True, True, )

    if len(plan_id_list) > 1:
        total_pfs = list(all_plans_df.keys())
        overlap = dict()
        for plan in total_pfs:
            df_a = all_plans_df[plan]
            overlap[plan] = find_portfolio_overlap_v2(securities_light, df_a, True, True)

        return overlap
    else:
        response = find_portfolio_overlap_v2(securities_light, plan_df, True, True)
    
    trailing_return = list()
    plan_id = plan_id_list[0]
    # get trailing returns (%)
    # TODO Need to confirm w/ CMOTS if the data available for the transaction date
    # on a month end is based on which portfolio date.
    benchmark_name = db_session.query(BenchmarkIndices.BenchmarkIndices_Name).filter(BenchmarkIndices.BenchmarkIndices_Id == benchmark_id).scalar()
    res_factsheet = get_performancetrend_data(db_session, plan_id, portfolio_date)
    res = dict()

    if res_factsheet:
        res["plan_id"] = plan_id
        res["plan_name"] = res_factsheet["plan_name"]
        res["inception_date"] = res_factsheet["inception_date"]
        res["benchmark_name"] = benchmark_name

        # TODO: Remove the following as not required
        res_fact = dict()
        res_fact["bm_ret_1m"] = res_factsheet["bm_ret_1m"]
        res_fact["bm_ret_3m"] = res_factsheet["bm_ret_3m"]
        res_fact["bm_ret_6m"] = res_factsheet["bm_ret_6m"]
        res_fact["bm_ret_1y"] = res_factsheet["bm_ret_1y"]
        res_fact["bm_ret_2y"] = res_factsheet["bm_ret_2y"]
        res_fact["bm_ret_3y"] = res_factsheet["bm_ret_3y"]
        res_fact["bm_ret_5y"] = res_factsheet["bm_ret_5y"]
        res_fact["bm_ret_ince"] = res_factsheet["bm_ret_ince"]

        res_fact["scheme_ret_1m"] = res_factsheet["scheme_ret_1m"]
        res_fact["scheme_ret_3m"] = res_factsheet["scheme_ret_3m"]
        res_fact["scheme_ret_6m"] = res_factsheet["scheme_ret_6m"]
        res_fact["scheme_ret_1y"] = res_factsheet["scheme_ret_1y"]
        res_fact["scheme_ret_2y"] = res_factsheet["scheme_ret_2y"]
        res_fact["scheme_ret_3y"] = res_factsheet["scheme_ret_3y"]
        res_fact["scheme_ret_5y"] = res_factsheet["scheme_ret_5y"]
        res_fact["scheme_ret_ince"] = res_factsheet["scheme_ret_ince"]

        res["returns"] = res_fact

    trailing_return.append(res)

    res_fact = {'inception_date': '', 'plan_name': benchmark_name, 'plan_id': benchmark_id, 'returns': dict()}
    periods = ['1m', '3m', '6m', '1y', '3y', '5y']

    holdings_df = get_normalized_portfolio_holdings(db_session, account_ids, portfolio_date, False)
    if holdings_df.empty:
        raise BadRequest(F"No Portfolio found for {portfolio_date}")

    fund_trailing_returns = get_portfolio_performance_analysis_for_all_period(db_session, holdings_df, portfolio_date, periods)

    benchmark_details = get_benchmarkdetails(db_session, benchmark_id)
    co_code = benchmark_details.Co_Code
    if not co_code:
        raise BadRequest('Data Error: No benchmark found.')

    benchmark_trailing_returns = get_benchmark_trailing_returns_for_all_period(db_session, benchmark_id, co_code, portfolio_date)

    for period in periods:
        res_fact['returns'][F"scheme_ret_{period}"] = fund_trailing_returns[period]['equity_performance'] if fund_trailing_returns[period] else None

        res_fact['returns'][F"bm_ret_{period}"] = benchmark_trailing_returns[period]['Returns_Value'] if benchmark_trailing_returns[period] else None

    trailing_return.append(res_fact)

    response["trailing_returns"] = trailing_return


    if export_report:
        portfolio_dict = dict()
        portfolio_dict["trailing_returns"] = fund_trailing_returns
        portfolio_dict["benchmark_returns"] = benchmark_trailing_returns
        portfolio_dict["benchmark_name"] = benchmark_name

        response["portfolio_trailing_returns"] = portfolio_dict

        investor_id = get_investorid_by_account_id(db_session, account_ids[0])        
        organization_id = get_organizationid_by_userid(db_session, user_id)
        image_path = current_app.config['IMAGE_PATH']
        whitelabel_dir = current_app.config['WHITELABEL_DIR']
        generatereport_dir = current_app.config['REPORT_GENERATION_PATH']

        response['investor_details'] = get_investor_dashboard_info(db_session, investor_id, user_id)        
        fund_info = get_plan_meta_info(db_session,[plan_id])
        response['fund_info'] = fund_info[str(plan_id)]
        response['portfolio_date'] = datetime.datetime.strftime(portfolio_date,'%d %b %Y')
        response['fund_portfolio_date'] = fund_portfolio_date
        response['organization_whitelabel_data'] = get_organization_whitelabel(db_session, organization_id)

        if response['organization_whitelabel_data']:
            response["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{response['organization_whitelabel_data']['logo']}"
            response["disclaimer_text"] = response['organization_whitelabel_data']['disclaimer']


        file_name = get_portfoliooverlappdf(db_session, generatereport_dir, response)

        return send_file(file_name, attachment_filename="portfolio_overlap.pdf")

    return jsonify(response)


@portfolio_bp.route('/portfolio/investor/portfolio_overlap_with_model', methods=["GET"])
def api_get_investor_portfolio_overlap_with_model():
    # list of demat ids
    account_ids = request.args.getlist('account_id', type=int)
    model_portfolio_id = request.args.get('model_portfolio_id', type=int)
    detailed_analysis = request.args.get("is_detailed", type=int)
    # date
    date_str = request.args.get('date')
    portfolio_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    export_report = request.args.get('export', type=int)    
    investor_id = get_investorid_by_account_id(current_app.store.db, account_ids[0])

    #send x_user_id in header
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    # user_id = 20237
    model_portfolio_date = None
    securities_df = get_normalized_portfolio_holdings(current_app.store.db, account_ids, portfolio_date, detailed_analysis)
    cols = [ "total_price", "units", "instrument_type", "asset_class", "issuer", "sub_sector", "market_cap", "equity_style", "risk_category", "account_alias", "unit_price", "coupon_rate", "maturity", "portfolio_date" ]
    securities_light = securities_df.drop(cols, axis=1)

    model_portfolio = get_one_model_portfolios(current_app.store.db, model_portfolio_id)
    if model_portfolio:
        if model_portfolio['holdings']:
            model_portfolio_date = datetime.datetime.strftime(model_portfolio['holdings'][0]['as_of_date'],'%d %b %Y')
    holdings_df = pd.DataFrame(model_portfolio["holdings"])
    holdings_df["sector"] = "Unknown"
    holdings_df = holdings_df.drop(["as_of_date"], axis=1)

    # response = find_portfolio_overlap(securities_light, holdings_df, True)
    # response = find_portfolio_overlap_with_isin(securities_light, holdings_df)
    response = find_portfolio_overlap_v2(securities_light, holdings_df, True, True)

    if export_report:
        organization_id = get_organizationid_by_userid(current_app.store.db, user_id)
        image_path = current_app.config['IMAGE_PATH']
        whitelabel_dir = current_app.config['WHITELABEL_DIR']
        generatereport_dir = current_app.config['REPORT_GENERATION_PATH']

        response['investor_details'] = get_investor_dashboard_info(current_app.store.db, investor_id, user_id)
        response['model_portfolio_name'] = model_portfolio['name']
        response['portfolio_date'] = datetime.datetime.strftime(portfolio_date,'%d %b %Y')
        response['fund_portfolio_date'] = model_portfolio_date

        response['organization_whitelabel_data'] = get_organization_whitelabel(current_app.store.db, organization_id)

        if response['organization_whitelabel_data']:
            response["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{response['organization_whitelabel_data']['logo']}"
            response["disclaimer_text"] = response['organization_whitelabel_data']['disclaimer']
       
        file_name = get_portfoliooverlappdf(current_app.store.db, generatereport_dir, response)

        return send_file(file_name, attachment_filename="portfolio_overlap.pdf")

        
    return jsonify(response)


@portfolio_bp.route('/portfolio/investor/portfolio_overlap_with_indices', methods=["GET"])
def api_get_investor_portfolio_overlap_with_indices():
    # list of demat ids
    account_ids = request.args.getlist('account_id', type=int)
    benchmark_id = request.args.get('benchmark_id', type=int, default=127)
    detailed_analysis = request.args.get("is_detailed", type=int)

    date_str = request.args.get('date')     # date
    portfolio_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    db_session = current_app.store.db
    investor_id = get_investorid_by_account_id(db_session, account_ids[0])

    user_info = get_user_info(request, db_session, current_app.config['SECRET_KEY'])     #send x_user_id in header
    user_id = user_info["id"]
    # user_id = 20237

    securities_df = get_normalized_portfolio_holdings(db_session, account_ids, portfolio_date, detailed_analysis)
    cols = [ "total_price", "units", "instrument_type", "asset_class", "issuer", "sub_sector", "market_cap", "equity_style", "risk_category", "account_alias", "unit_price", "coupon_rate", "maturity", "portfolio_date" ]
    securities_light = securities_df.drop(cols, axis=1)


    df_index_holdings = prepare_index_holdings_from_db(db_session, benchmark_id, portfolio_date)

    if df_index_holdings.shape[0] == 0:
        raise BadRequest(F"No Index found for {portfolio_date}")

    response = find_portfolio_overlap_v2(securities_light, df_index_holdings, True, True)

    return jsonify(response)

#TODO change name for below API
@portfolio_bp.route('/portfolio/investor/generate_report_1', methods=["POST"])
def generate_report_1():
    account_ids = request.args.getlist('account_id', type=int)
    is_detailed = request.args.get('is_detailed', type=int)
    plan_id = request.args.get('plan_id', type=int)
    email = request.args.get('email_id', type=str)
    benchmark_id = request.args.get('benchmark_id', type=int)
    portfolio_date = datetime.datetime.strptime(request.args.get('date'), '%Y-%m-%d')
    from_date = datetime.datetime.strptime(request.args.get('from_date'), '%Y-%m-%d')
    to_date = datetime.datetime.strptime(request.args.get('to_date'), '%Y-%m-%d')
    time_period_type = request.args.get('time_period_type', type=str)

    f = request.json
    if 'selection_data' in f:
        selection_data = f["selection_data"]

    db_session = current_app.store.db
    # investor_id = get_investorid_by_account_id(db_session, account_ids[0])

    user_info = get_user_info(request, db_session, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    # user_id = 20237

    # http://localhost:5100/portfolio/investor/generate_report_1?is_detailed=0&date=2022-03-31&account_id=107&account_id=105&account_id=104&account_id=103&account_id=106&account_id=108&account_id=109&&benchmark_id=154&time_period_type=POP&plan_id=1495&from_date=2021-03-31&to_date=2022-03-31
    # 20237 - sachin
    
    
    portfolio_dict = dict()

    # portfolio_dict['investor_details'] = get_investor_dashboard_info(db_session, investor_id, user_id)  
    portfolio_dict['user_id'] = user_id
    portfolio_dict['plan_id'] = plan_id
    portfolio_dict['benchmark_id'] = benchmark_id
    portfolio_dict['account_ids'] = account_ids
    # fund_info = get_plan_meta_info(db_session,[plan_id])
    # portfolio_dict['fund_info'] = fund_info[str(plan_id)]
    portfolio_dict['portfolio_date'] = portfolio_date
    # portfolio_dict['portfolio_date_label'] = datetime.datetime.strftime(portfolio_date,'%d %b %Y')
    portfolio_dict['from_date'] = from_date
    # portfolio_dict['from_date_label'] = datetime.datetime.strftime(from_date,'%d %b %Y')
    portfolio_dict['to_date'] = to_date
    # portfolio_dict['to_date_label'] = datetime.datetime.strftime(to_date,'%d %b %Y')
    portfolio_dict['report_date'] = datetime.datetime.strftime( date.today(),'%d %b %Y')

    portfolio_dict['image_path'] = current_app.config['IMAGE_PATH']
    portfolio_dict['whitelabel_dir'] = current_app.config['WHITELABEL_DIR']
    portfolio_dict['generatereport_dir'] = current_app.config['REPORT_GENERATION_PATH']

    # portfolio_dict['organization_id'] = get_organizationid_by_userid(db_session, user_id)
    portfolio_dict['time_period_type'] = time_period_type
    portfolio_dict['show_trend_analysis'] = selection_data['trend_analysis']
    portfolio_dict['show_performance'] = selection_data['performance']
    portfolio_dict['show_stock_details'] = selection_data['stock_details']
    portfolio_dict['page_break_required'] = selection_data['page_break']
    portfolio_dict['is_detailed'] = is_detailed
    
    json_req = object_to_xml(portfolio_dict, 'portfolio_parameters')

    dict_tree = ET.fromstring(json_req)
    # xmlstr = ET.tostring(tree, encoding='utf8')

    sql_request = DeliveryRequest()
    # TODO: remove the hard coded part from the below list
    sql_request.Channel_Id = 2
    sql_request.Type = "X-Ray PDF"
    sql_request.Recipients = email if email else ''
    sql_request.Body = ''
    sql_request.Request_Time = datetime.datetime.now()
    sql_request.Template_Id = 0
    sql_request.RecipientsCC = ''
    sql_request.RecipientsBCC = ''
    sql_request.Subject = ''
    sql_request.IsBodyHTML = 1
    sql_request.Attachments = None
    sql_request.Parameters = ET.tostring(dict_tree).decode()
    sql_request.Resources = None
    sql_request.Status = 0
    sql_request.Status_Message = "Pending"
    sql_request.Created_By = user_id
    sql_request.Is_Deleted = 0
    
    db_session.add(sql_request)
    db_session.commit()
    
    return jsonify({"msg": 'Success'})


@portfolio_bp.route('/get_portfolio_report', methods=["GET"])
def get_portfolio_report():
    x_token = request.args.get('token', type=str)
    if not x_token:
        raise BadRequest('Invalid Request.')
    
    try:
        data = validate_jwt_token(current_app.store.db, x_token, current_app.config['SECRET_KEY'], get_full_data=True)
        
        if data['exp'] >= datetime.datetime.now().timestamp():
            if data['File']:
                return send_file(data['File'])
    except Exception as ex:
        raise BadRequest('This report link has expired. You can regenerate it from portal.')

    return jsonify({"msg": 'File not found.'})
