from datetime import datetime
from flask import Blueprint, current_app, json, jsonify, render_template, render_template_string, request
from fin_models.controller_master_models import User
from fin_models.controller_transaction_models import OrgFundSettings
from sqlalchemy import and_, distinct, func
from werkzeug.exceptions import BadRequest, Conflict, Forbidden, BadGateway
from bizlogic.importer_helper import get_sql_fund_byplanid
from src.utils import get_user_info

from .access_bl import NotUniqueValueException, update_user_profile
from .digital_ob_bl import *

from bizlogic.report_issue import user_is_interested_in_amc_fund

user_service_bp = Blueprint("user_service_bp", __name__)

# TODO: Remove following API after Dec 2022. 
@user_service_bp.route("/activate_account", methods=['GET'])
def activate_account():

    Activation_Code = request.args.get('ac', type=str)
    Organization_Id = request.args.get('oi', type=str)
    User_Id = request.args.get('ui', type=str)

    sql_user = current_app.store.db.query(User).filter(User.User_Id==User_Id).filter(User.Organization_Id==Organization_Id).filter(User.Activation_Code==Activation_Code).one_or_none()

    msg = ""
    if sql_user:
        if sql_user.Is_Active:
            msg = "Account is already activated. Please log-in to access Finalyca."
        else:
            sql_user.Is_Active = True
            msg = "Account activated successfully. Please log-in to access Finalyca."
            current_app.store.db.commit()
    else:
        raise BadRequest(description="Invalid activation link. Please contact administrator.")

    return jsonify({'msg': msg})


@user_service_bp.route("/check_empanelment", methods=['GET'])
def check_digital_empanelment():
    plan_id = request.args.get("plan_id")
    sql_fund = get_sql_fund_byplanid(current_app.store.db, plan_id)
    fund_code = sql_fund.Fund_Code
    
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])

    is_investment_allowed = can_user_transact(current_app.store.db, user_info["id"], user_info["org_id"], fund_code)

    # if the url is valid send it else raise an alert
    return jsonify(is_investment_allowed)

@user_service_bp.route("/invest", methods=['GET'])
def get_investment_sso_url():
    fund_code = request.args.get("fund_code")
    if not fund_code:
        plan_id = request.args.get("plan_id")
        sql_fund = get_sql_fund_byplanid(current_app.store.db, plan_id)
        fund_code = sql_fund.Fund_Code
    
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])

    is_investment_allowed = can_user_transact(current_app.store.db, user_info["id"], user_info["org_id"], fund_code)

    if not is_investment_allowed:
        raise BadRequest(description="Digital onboarding is not allowed for this fund.")

    try:
        sso_url = get_fund_invest_url(current_app.store.db, user_info["id"], fund_code)
    except MissingConfigurationException as e:
        raise BadRequest(description=str(e))

    except UpstreamServerErrorException as e:
        raise BadGateway(description=F"Transaction server shared the following error: {str(e)}")

    # if the url is valid send it else raise an alert
    return jsonify(sso_url)

@user_service_bp.route("/access/user/profile", methods=['PUT'])
def save_profile():
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    form = request.form if request.form else request.json

    name = form["name"]
    email = form["email"]
    mobile = form["mobile_no"]
    designation = form.get("designation")
    city = form.get("city") 
    state = form.get("state")
    pin = form.get("pin_code")
    try:
        user_id = update_user_profile(current_app.store.db, user_info["id"], name, email, mobile, designation, city, state, pin, request.files, current_app.config)
    except NotUniqueValueException as exe:
        raise Conflict(str(exe))

    return jsonify({"msg": "User profile is updated."})

@user_service_bp.route("/interested", methods=['GET'])
def interested_in_amc_funds():
    try:
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        amc_id = request.args['amc_id']
        plan_id = request.args['plan_id']

        if not amc_id or not plan_id:
            raise BadRequest('amc id and plan_id parameter required')
        
        # get amc email id and send an email to amc
        log_id = user_is_interested_in_amc_fund(current_app.store.db, current_app.config, user_info, amc_id, plan_id)

        return jsonify({"id": log_id, "msg": "An email has been sent to respective person of AMC."})
    except Exception as exe:
        print(str(exe))
