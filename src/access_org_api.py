import base64
from datetime import datetime
import hashlib
import random
from itsdangerous import json
from fin_models.controller_master_models import API
from flask import current_app, Blueprint, jsonify, request, g
from werkzeug.exceptions import BadRequest, Conflict, Forbidden

from src.utils import get_user_info
from .access_bl import *
from .digital_ob_bl import *

org_admin_bp = Blueprint("org_admin_bp", __name__)

@org_admin_bp.route('/access/org/api', methods=['GET', 'POST'])
def api_create_api_key():
    if request.method == 'GET':
        # TODO: remote the org_id as we can get it from the user_id itself
        organization_id = request.args.get('org_id')
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        if not user_info["org_has_api"]:
            raise Forbidden("Please contact your administrator to enable this feature.")

        obj = get_api_key(current_app.store.db, organization_id)
        return jsonify(obj)

    if request.method == 'POST':
        try: 
            # TODO: requested by should come from the auth token. we should get org_id from user id and access level can be found from org_id.
            user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
            if not user_info["org_has_api"]:
                raise Forbidden("Please contact your administrator to enable this feature.")

            name = request.form['name']
            org_id = request.form['org_id']
            access_level = request.form['access_level']
            requested_by = request.form['requested_by']

            api_key = create_api_key(current_app.store.db, name, org_id, access_level, requested_by)
            return jsonify(api_key)

        except KeyError as exec:
            raise BadRequest(F"Please provide {''.join(exec.args)}.")

@org_admin_bp.route('/access/org/api/<int:id>', methods=['DELETE'])
def api_key_functions(id):
    if request.method == 'DELETE':
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        if not user_info["org_has_api"]:
            raise Forbidden("Please contact your administrator to enable this feature.")

        edited_by = g.access.entity_id 
        delete_api_key(current_app.store.db, id, edited_by)

@org_admin_bp.route('/access/org/api/<int:id>/regenerate', methods=['PATCH'])
def api_regenerate_key(id):
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    if not user_info["org_has_api"]:
        raise Forbidden("Please contact your administrator to enable this feature.")
    edited_by = g.access.entity_id 
    api_key = change_api_key(current_app.store.db, id, edited_by)
    return jsonify(api_key)

@org_admin_bp.route("/access/org/user", methods=['POST'])
def create_user():
    organization_id = request.args.get("organization_id")
    name = request.form.get("name")
    email = request.form.get("email")
    mobile = request.form.get("mobile")
    status = request.form.get("status", type=int)
    access_level = request.form.get("access_level", default='pro')
    role_id = request.form.get("role_id", type=int, default=3)
    # access_level = 3
    # role_id = 3

    try:
        create_new_user(current_app.store.db, current_app.config, current_app.jinja_env, name, email, mobile, status, organization_id, role_id, access_level, False)
    
    except NotUniqueValueException as exe:
        raise Conflict(str(exe))

    except LicenceLimitExceedException as exe:
        raise Conflict(str(exe))

    return jsonify({"msg": "A new user is created."})

@org_admin_bp.route("/access/org/user", methods=['GET'])
def admin_users():
    users_list = list()
    user_id = request.args.get('user_id', type=int)
    if not user_id:
        raise BadRequest(description="Please Provide the User Id")

    sql_user = current_app.store.db.query(User).filter(User.User_Id==user_id).one_or_none()

    if sql_user:
        sql_users = current_app.store.db.query(User).filter(User.Organization_Id==sql_user.Organization_Id).filter(User.Role_Id.in_((2,3))).filter(User.Is_Active==1).all()
        for user in sql_users:
            user_dict = dict()
            user_dict["name"] = user.Display_Name
            user_dict["email"] = user.Email_Address
            user_dict["mobile"] = user.Contact_Number
            user_dict["status"] = 1 if User.Is_Active else 0
            user_dict["user_id"] = user.User_Id
            user_dict["organization_id"] = user.Organization_Id
            user_dict["access_level"] = user.Access_Level
            user_dict["role_id"] = user.Role_Id
            user_dict["download_nav"] = user.downloadnav_enabled
            users_list.append(user_dict)
    return jsonify(users_list)

@org_admin_bp.route("/access/org/user/<int:id>", methods=['PUT'])
def save_user(id):
    name = request.form.get("name")
    email = request.form.get("email")
    mobile = request.form.get("mobile")
    status = request.form.get("status", type=int)
    access_level = request.form.get("access_level", default='pro')
    role_id = request.form.get("role_id", type=int, default=3)
    download_nav = request.form.get("download_nav", type=int, default=0)

    try:
        update_user(current_app.store.db, id, name, email, mobile, status, role_id, access_level, None, None, None, None, download_nav)
    
    except NotUniqueValueException as exe:
        raise Conflict(str(exe))

    except LicenceLimitExceedException as exe:
        raise Conflict(str(exe))

    return jsonify({"msg": "User information has been edited."})

# TODO: Rename to /access/org/dashboard
@org_admin_bp.route("/access/org/dashboard", methods=['GET'])
def admin_profile():
    admin_profile = {}
    user_id = request.args.get('user_id', type=int)
    if not user_id:
        raise BadRequest(description="Please Provide the User Id")

    sql_user = current_app.store.db.query(User).filter(User.User_Id==user_id).one_or_none()

    if sql_user:
        # occupied_licenses = current_app.store.db.query(func.count(User.User_Id)).filter(User.Organization_Id==sql_user.Organization_Id).filter(User.Role_Id.in_((2,3))).filter(User.Is_Active==1).scalar()
        lite_occupied_licenses = current_app.store.db.query(func.count(User.User_Id)).filter(User.Organization_Id==sql_user.Organization_Id).filter(User.Role_Id.in_((2,3))).filter(User.Access_Level=='lite').filter(User.Is_Active==1).scalar()

        pro_occupied_licenses = current_app.store.db.query(func.count(User.User_Id)).filter(User.Organization_Id==sql_user.Organization_Id).filter(User.Role_Id.in_((2,3))).filter(User.Access_Level=='pro').filter(User.Is_Active==1).scalar()

        rm_occupied_licenses = current_app.store.db.query(func.count(User.User_Id)).filter(User.Organization_Id==sql_user.Organization_Id).filter(User.Role_Id.in_((2,3))).filter(User.Access_Level=='rm').filter(User.Is_Active==1).scalar()

        sql_org = current_app.store.db.query(Organization).filter(Organization.Organization_Id == sql_user.Organization_Id).filter(Organization.Is_Active == 1).one_or_none()
        
        admin_profile["Profile_Name"] = sql_user.Display_Name
        admin_profile["Profile_Email"] = sql_user.Email_Address
        admin_profile["Profile_Mobile"] = sql_user.Contact_Number
        admin_profile["Profile_Designation"] = sql_user.Designation
        admin_profile["Profile_City"] = sql_user.City
        admin_profile["Profile_State"] = sql_user.State
        admin_profile["Profile_Pin_Code"] = sql_user.Pin_Code
        
        # admin_profile["Total_Licenses"] = sql_org.No_Of_Licenses
        # admin_profile["Total_L1_Licenses"] = sql_org.No_Of_L1_Licenses
        # admin_profile["Total_L2_Licenses"] = sql_org.No_Of_L2_Licenses
        # admin_profile["Total_L3_Licenses"] = sql_org.No_Of_L3_Licenses
        # # admin_profile["Occupied_Licenses"] = occupied_licenses
        # admin_profile["L1_Occupied_Licenses"] = l1_occupied_licenses
        # admin_profile["L2_Occupied_Licenses"] = l2_occupied_licenses
        # admin_profile["L3_Occupied_Licenses"] = l3_occupied_licenses

        admin_profile["Total_Lite_Licenses"] = sql_org.No_Of_Lite_Licenses
        admin_profile["Total_Pro_Licenses"] = sql_org.No_Of_Pro_Licenses
        admin_profile["Total_RM_Licenses"] = sql_org.No_Of_Pro_Licenses
        # admin_profile["Occupied_Licenses"] = occupied_licenses
        admin_profile["Lite_Occupied_Licenses"] = lite_occupied_licenses
        admin_profile["Pro_Occupied_Licenses"] = pro_occupied_licenses
        admin_profile["RM_Occupied_Licenses"] = rm_occupied_licenses

        total_available_licenses = sql_org.No_Of_Lite_Licenses + sql_org.No_Of_Pro_Licenses + sql_org.No_Of_RM_Licenses
        total_occupied_licenses = lite_occupied_licenses + pro_occupied_licenses + rm_occupied_licenses
        admin_profile["Progress_Occupied_Licenses"] = int(total_occupied_licenses/total_available_licenses) * 100

        admin_profile["is_api_enabled"] = sql_org.is_api_enabled
        admin_profile["is_buy_enable"] = sql_org.is_buy_enable
        admin_profile["api_access_level"] = sql_org.api_access_level
        profile_updated = None

        if sql_user.Designation == '' and sql_user.City == '' and sql_user.State == '' and sql_user.Pin_Code == '':
            profile_updated = 60
        elif  sql_user.City == '' and sql_user.State == '' and sql_user.Pin_Code == '':
            profile_updated = 70
        elif sql_user.State == '' and sql_user.Pin_Code == '':
            profile_updated = 80
        elif sql_user.Pin_Code == '':
            profile_updated = 90
        else:
            profile_updated = 100
        
        admin_profile["Profile_Updated"] = profile_updated
        admin_profile["Expiry_Date"] = sql_org.License_Expiry_Date
    return jsonify(admin_profile)

# TODO: Rename to /access/org/invest_settings
@org_admin_bp.route("/access/admin/invest_settings", methods=['GET', 'POST', 'DELETE'])
def admin_invest_settings():
    if request.method == 'GET':
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        org_id = user_info["org_id"]
        if not user_info["org_has_buy"]:
            raise Forbidden("Please contact your administrator to enable this feature.")

        results = get_investment_settings(current_app.store.db, org_id)
        return jsonify(results)

    elif request.method == 'POST':
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        user_id = user_info["id"]
        org_id = user_info["org_id"]
        if not user_info["org_has_buy"]:
            raise Forbidden("Please contact your administrator to enable this feature.")

        amc_id = request.form.get("amc_id", type=int)
        fund_code = request.form.get("fund_code")
        facilitator_id = request.form.get("facilitator_id", type=int)
        # distributor_pan_no = request.form.get("distributor_pan_no")
        distributor_org_id = request.form.get("distributor_org_id")
        # Now we do not use distributor_token as we don't need it
        # distributor_token = request.form.get("distributor_token")
        distributor_token = ""

        save_investment_setting(current_app.store.db, org_id, amc_id, fund_code, facilitator_id, distributor_org_id, distributor_token, user_id)

        return jsonify({"msg": "New Fund has been added for digital onboarding."})
        
    elif request.method == 'DELETE':
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        user_id = user_info["id"]
        org_id = user_info["org_id"]
        if not user_info["org_has_buy"]:
            raise Forbidden("Please contact your administrator to enable this feature.")
            
        amc_id = request.args["amc_id"]
        fund_code = request.args["fund_code"]

        remove_investment_setting(current_app.store.db, org_id, amc_id, fund_code, user_id)

        return jsonify({"msg": "A Fund has been removed from digital onboarding."})