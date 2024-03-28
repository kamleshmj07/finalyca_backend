from datetime import date, datetime
from random import randint
import uuid
from flask import Blueprint, current_app, jsonify, request, make_response
from sqlalchemy import or_
from fin_models.controller_master_models import *
from fin_models.controller_transaction_models import *
from async_tasks.send_email import send_email_async
from async_tasks.send_sms import send_sms, SMSConfig

from utils.utils import decrypt_aes
from utils.utils import generate_jwt_token, validate_jwt_token
from .access_bl import create_new_user, check_user_inputs_for_sso


login_bp = Blueprint("login_bp", __name__)

@login_bp.route("/login/generate_otp", methods=['POST'])
def api_generate_otp():
    f = request.form
    encrypted_uname = f["User_Name"]

    # Decrypt string AES
    user_name = decrypt_aes(encrypted_uname, current_app.config["AES_KEY"], current_app.config["AES_IV"])

    has_error = False
    share_otp_over_mail = False

    sql_user = current_app.store.db.query(User).filter(or_(User.Email_Address==user_name, User.Contact_Number==user_name)).one_or_none()
    if not sql_user:
        msg = "Email or Mobile number is not registered."
        has_error = True

    if not has_error and sql_user and not sql_user.Is_Active:
        msg = "Account is not active. Please contact your administrator."
        has_error = True

    if not has_error:
        sql_org = current_app.store.db.query(Organization).filter(Organization.Organization_Id==sql_user.Organization_Id).one_or_none()
        if sql_org:
            if sql_org.otp_allowed_over_mail:
                share_otp_over_mail = True

            if sql_org.License_Expiry_Date < date.today():
                msg = F"Subscription has expired on {sql_org.License_Expiry_Date}. Please contact finalyca support."
                has_error = True

    if not has_error:
        
        otp = randint(100000, 999999) if not sql_user.Email_Address == 'demo1@finalyca.com' else '810349'
        sql_user.OTP = otp
        current_app.store.db.commit()
        # send it via email and mobile number
        current_app.store.db, current_app.config, current_app.jinja_env
        templ = current_app.jinja_env.get_or_select_template("send_otp.html")
        html_message = templ.render(name=user_name, OTP=otp)
        config = current_app.config

        # if share_otp_over_mail:
        send_email_async(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], sql_user.Email_Address, "Welcome to Finalyca", html_message)

        sms_text = F"{otp} is your login OTP, please use this OTP for Finalyca account login."
        sms_config = SMSConfig()
        sms_config.url = config["SMS_URL"]
        sms_config.sender_id = config["SMS_SENDER_ID"]
        sms_config.is_unicode = config["SMS_IS_UNICODE"]
        sms_config.is_flash = config["SMS_IS_FLASH"]
        sms_config.api_key = config["SMS_API_KEY"]
        sms_config.client_id = config["SMS_CLIENT_ID"]
        if sql_user.Contact_Number:
            send_sms(sms_config, sql_user.Contact_Number, sms_text)
        msg = "OTP generated successfully. Please check your registered email or mobile."  

    if has_error:
        resp  = {
            "ResponseStatus": 0,
            "ResponseMessage": "ERROR",
            "ResponseObject": msg
        }
    else:
        resp  = {
            "ResponseStatus": 1,
            "ResponseMessage": "SUCCESS",
            "ResponseObject": msg
        }
    return jsonify(resp)


@login_bp.route("/login/validate_otp", methods=['POST'])
def api_validate_otp():
    f = request.form
    encrypted_uname = f["User_Name"]
    encrypted_pwd = f["Password"]

    # Decrypt string AES
    user_name = decrypt_aes(encrypted_uname, current_app.config["AES_KEY"], current_app.config["AES_IV"])
    pwd = decrypt_aes(encrypted_pwd, current_app.config["AES_KEY"], current_app.config["AES_IV"])

    resp = dict()
    db_session = current_app.store.db

    sql_user = db_session.query(User).filter(User.Is_Active == 1).filter(or_(User.Email_Address==user_name, User.Contact_Number==user_name)).filter(User.OTP == pwd).one_or_none()
    if sql_user:
        session_id = uuid.uuid4()
        sql_user.Session_Id = session_id
        sql_user.OTP = None
        db_session.commit()

        sql_user_org = db_session.query(User.User_Id,
                                        User.Organization_Id,
                                        User.Display_Name,
                                        User.Email_Address,
                                        User.Contact_Number,
                                        User.Role_Id,
                                        User.Session_Id,
                                        User.downloadnav_enabled,
                                        User.Access_Level,
                                        User.Contact_Number,
                                        User.Designation,
                                        User.Profile_Picture,
                                        User.City,
                                        User.State,
                                        User.Pin_Code,
                                        Organization.Organization_Name,
                                        Organization.AMC_Id,
                                        Organization.Is_Enterprise_Value,
                                        Organization.Is_WhiteLabel_Value,
                                        Organization.Logo_Img,
                                        Organization.Disclaimer_Img,
                                        Organization.Disclaimer_Img2,
                                        Organization.Application_Title,
                                        Organization.License_Expiry_Date,
                                        Organization.usertype_id)\
                                    .join(Organization, User.Organization_Id == Organization.Organization_Id)\
                            .filter(User.Is_Active == 1, Organization.Organization_Id == sql_user.Organization_Id)\
                            .filter(User.Email_Address == sql_user.Email_Address).first()

        token = generate_jwt_token(sql_user_org, current_app.config['SECRET_KEY'], expiry=480)

        resp_obj = dict()
        resp_obj["Token"] = token
        resp_obj["Display_Name"] = sql_user_org.Display_Name
        resp_obj["Is_Enterprise_Value"] = sql_user_org.Is_Enterprise_Value
        resp_obj["Is_WhiteLabel_Value"] = sql_user_org.Is_WhiteLabel_Value            
        resp_obj["Logo_Img"] = sql_user_org.Logo_Img
        resp_obj["Disclaimer_Img"] = sql_user_org.Disclaimer_Img
        resp_obj["Disclaimer_Img2"] = sql_user_org.Disclaimer_Img2
        resp_obj["Application_Title"] = sql_user_org.Application_Title
        resp_obj["User_Id"] = sql_user_org.User_Id
        resp_obj["Organization_Id"] = sql_user_org.Organization_Id
        resp_obj["Email_Address"] = sql_user_org.Email_Address
        resp_obj["Contact_Number"] = sql_user_org.Contact_Number
        resp_obj["Role_Id"] = sql_user_org.Role_Id
        resp_obj["Access_Level"] = sql_user_org.Access_Level
        resp_obj["Session_Id"] = session_id
        resp_obj["Downloadnav_Enabled"] = sql_user_org.downloadnav_enabled
        resp_obj["Organization_Name"] = sql_user_org.Organization_Name
        resp_obj['License_Expiry_Date']: sql_user_org.License_Expiry_Date
        resp_obj['User_Type_Id']: sql_user_org.usertype_id
        resp_obj["AMC_Id"] = sql_user_org.AMC_Id

        user_log = UserLog()
        user_log.User_Id = sql_user.User_Id
        user_log.login_timestamp = datetime.now()
        current_app.store.db.add(user_log)
        current_app.store.db.commit()

        resp["ResponseStatus"] = 1
        resp["ResponseMessage"] = "SUCCESS"
        resp["ResponseObject"] = resp_obj

    else:
        msg = 'Invalid OTP! Please check that you have entered correct OTP.'
        resp["ResponseStatus"] = 0
        resp["ResponseMessage"] = "ERROR"
        resp["ResponseObject"] = msg

    return jsonify(resp)


@login_bp.route("/login/generate_token", methods=['POST'])
def generate_token():
    # creates dictionary of form data
    f = request.form

    client_ip = request.access_route     # Get the client's IP address and user info
    db_session = current_app.store.db

    for remote_ip in client_ip:
        # check if ip matches any registered active organization
        sql_org_info = db_session.query(Organization).filter(Organization.Is_Active == 1, Organization.api_remote_addr.like(f'%{remote_ip}%')).first()
        if sql_org_info:
            break

    if not sql_org_info:
        return make_response(jsonify({'ResponseStatus': 0,
                                      'ResponseMessage': 'ERROR',
                                      'ResponseObject': 'Could not verify, please subscribe to finalyca.com. If already a subscriber, then please contact support@finalyca.com'}), 401)

    if not f or not f.get('user_email') or not f.get('user_name') or not f.get('plan_type'):
        # returns 401 if necessary information is missing
        return make_response(jsonify({'ResponseStatus': 0,
                                      'ResponseMessage': 'ERROR',
                                      'ResponseObject': 'Could not verify, please provide user and plan information.'}), 401)

    if sql_org_info.Is_Mobile_Mandatory and not f.get('user_mobile'):
        # check if mobile is mandatory or not, if yes then return 401 
        return make_response(jsonify({'ResponseStatus': 0,
                                      'ResponseMessage': 'ERROR',
                                      'ResponseObject': 'Could not verify, please provide user_mobile.'}), 401)

    # read the user information
    user_name = f.get("user_name")
    user_email = f.get("user_email")
    user_mobile = f.get("user_mobile")
    access_level = f.get('plan_type')

    # check if email and mobile via sso is valid or not
    is_email_valid = check_user_inputs_for_sso('email', user_email)
    is_mobile_valid = check_user_inputs_for_sso('mobile', user_mobile) if user_mobile else None

    if not is_email_valid or (not is_mobile_valid and sql_org_info.Is_Mobile_Mandatory):
        # check if mobile is mandatory or not, if yes then return 401 
        return make_response(jsonify({'ResponseStatus': 0,
                                      'ResponseMessage': 'ERROR',
                                      'ResponseObject': 'Please provide valid useremail and/or mobile.'}), 401)

    # check if user exists under the identifed organization, if not then create a new user
    sql_user_qry = db_session.query(User.User_Id,
                                    User.Organization_Id,
                                    User.Display_Name,
                                    User.Email_Address,
                                    User.Contact_Number,
                                    User.Role_Id,
                                    User.Session_Id,
                                    User.downloadnav_enabled,
                                    User.Access_Level,
                                    User.Contact_Number,
                                    User.Designation,
                                    User.Profile_Picture,
                                    User.City,
                                    User.State,
                                    User.Pin_Code,
                                    Organization.Organization_Name,
                                    Organization.AMC_Id,
                                    Organization.Is_Enterprise_Value,
                                    Organization.Is_WhiteLabel_Value,
                                    Organization.Logo_Img,
                                    Organization.Disclaimer_Img,
                                    Organization.Disclaimer_Img2,
                                    Organization.Application_Title).join(Organization, User.Organization_Id == Organization.Organization_Id)\
                              .filter(User.Is_Active == 1, Organization.Organization_Id == sql_org_info.Organization_Id)

    sql_user_info = sql_user_qry.filter(User.Email_Address == user_email).first()

    if not sql_user_info:
        try:
            user_id = create_new_user(db_session, None, None, user_name, user_email, user_mobile, 1, sql_org_info.Organization_Id, role_id=3, access_level=access_level, download_nav=False, is_sso_login=True)
        except Exception as ex:
            return make_response(jsonify({'ResponseStatus': 0,
                                        'ResponseMessage': 'ERROR',
                                        'ResponseObject': ex.description}), 401)

        sql_user_info = sql_user_qry.filter(User.User_Id == user_id).first()

    token = generate_jwt_token(sql_user_info, current_app.config['SECRET_KEY'], for_sso_url=True)

    resp_obj = dict()
    resp_obj["Token"] = token
    resp_obj["Display_Name"] = sql_user_info.Display_Name
    resp_obj["Is_Enterprise_Value"] = sql_user_info.Is_Enterprise_Value
    resp_obj["Is_WhiteLabel_Value"] = sql_user_info.Is_WhiteLabel_Value            
    resp_obj["Logo_Img"] = sql_user_info.Logo_Img
    resp_obj["Disclaimer_Img"] = sql_user_info.Disclaimer_Img
    resp_obj["Disclaimer_Img2"] = sql_user_info.Disclaimer_Img2
    resp_obj["Application_Title"] = sql_user_info.Application_Title

    # add log
    user_log = UserLog()
    user_log.User_Id = sql_user_info.User_Id
    user_log.login_timestamp = datetime.now()
    db_session.add(user_log)
    db_session.commit()

    return make_response(jsonify({'ResponseStatus': 1,
                                  'ResponseMessage': 'SUCCESS',
                                  'ResponseObject': resp_obj}), 201)


@login_bp.route("/login/validate_token", methods=['GET'])
def validate_token():
    token = request.args.get("X_Token", type=str)
    result = validate_jwt_token(current_app.store.db, token, current_app.config['SECRET_KEY'])
    token_valid = result[0]
    user_id = result[1]

    db_session = current_app.store.db
    response_status = 0
    response_msg = 'ERROR'
    response_obj = 'Token validation failed. Please re-generate token, or contact support@finalyca.com'
    resp_obj = {}

    if token_valid:
        # generate a new session id for the user and update the same in the database.
        sql_user_update = db_session.query(User).filter(User.Is_Active == 1, User.User_Id == user_id).first()
        if sql_user_update:
            session_id = uuid.uuid4()
            sql_user_update.Session_Id = session_id
            sql_user_update.OTP = None
            db_session.commit()

        sql_user = db_session.query(User.User_Id,
                                    User.Organization_Id,
                                    User.Display_Name,
                                    User.Email_Address,
                                    User.Contact_Number,
                                    User.Role_Id,
                                    User.Session_Id,
                                    User.downloadnav_enabled,
                                    User.Access_Level,
                                    User.Contact_Number,
                                    User.Designation,
                                    User.Profile_Picture,
                                    User.City,
                                    User.State,
                                    User.Pin_Code,
                                    Organization.Organization_Name,
                                    Organization.AMC_Id,
                                    Organization.Is_Enterprise_Value,
                                    Organization.Is_WhiteLabel_Value,
                                    Organization.Logo_Img,
                                    Organization.Disclaimer_Img,
                                    Organization.Disclaimer_Img2,
                                    Organization.Application_Title).join(Organization, User.Organization_Id == Organization.Organization_Id)\
                                .filter(User.Is_Active == 1, User.User_Id == user_id).first()

        token = generate_jwt_token(sql_user, current_app.config['SECRET_KEY'])
        
        resp_obj["Token"] = token
        resp_obj["Display_Name"] = sql_user.Display_Name
        resp_obj["Is_Enterprise_Value"] = sql_user.Is_Enterprise_Value
        resp_obj["Is_WhiteLabel_Value"] = sql_user.Is_WhiteLabel_Value            
        resp_obj["Logo_Img"] = sql_user.Logo_Img
        resp_obj["Disclaimer_Img"] = sql_user.Disclaimer_Img
        resp_obj["Disclaimer_Img2"] = sql_user.Disclaimer_Img2
        resp_obj["Application_Title"] = sql_user.Application_Title
        resp_obj["User_Id"] = sql_user.User_Id
        resp_obj["Organization_Id"] = sql_user.Organization_Id
        resp_obj["Email_Address"] = sql_user.Email_Address
        resp_obj["Contact_Number"] = sql_user.Contact_Number
        resp_obj["Role_Id"] = sql_user.Role_Id
        resp_obj["Session_Id"] = sql_user.Session_Id
        resp_obj["Downloadnav_Enabled"] = sql_user.downloadnav_enabled
        resp_obj["Organization_Name"] = sql_user.Organization_Name
        resp_obj["AMC_Id"] = sql_user.AMC_Id
        
        response_status = 1
        response_msg = 'SUCCESS'
        response_obj = resp_obj

        return make_response(jsonify({'ResponseStatus': response_status,
                                      'ResponseMessage': response_msg,
                                      'ResponseObject': resp_obj}), 200)


    return make_response(jsonify({'ResponseStatus': response_status,
                                  'ResponseMessage': response_msg,
                                  'ResponseObject': response_obj}), 401)

