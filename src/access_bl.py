from datetime import datetime, date
import os, re
from fin_models.controller_master_models import API, Organization, User
from fin_models.masters_models import AMC
from fin_models.servicemanager_models import DeliveryManager, DeliveryRequest
from sqlalchemy.util.langhelpers import NoneType
from .utils import get_user_name
from uuid import uuid4
from sqlalchemy import func
from async_tasks.send_email import send_email_async
from async_tasks.send_sms import send_sms, SMSConfig
from utils.access_control import *
from jinja2 import Environment, FileSystemLoader
from .upload_service import secure_filename, save_file

class NotUniqueValueException(Exception):
    def __init__(self, info):
        self.description = info

    def __str__(self):
        return self.description

class LicenceLimitExceedException(Exception):
    def __init__(self, info):
        self.description = info

    def __str__(self):
        return self.description

def create_api_key(db_session, name, org_id, access_level, requested_by):
    sql_api = API()
    sql_api.name = name
    sql_api.org_id = org_id
    api_key = set_api_key(db_session, sql_api)
    # sql_api.access_level = access_level
    sql_api.requested_by = requested_by
    sql_api.requested_at = datetime.now()
    sql_api.is_active = True

    db_session.add(sql_api)
    db_session.commit()

    return api_key

def change_api_key(db_session, api_id, edited_by):
    sql_api = db_session.query(API).filter(API.id==api_id).one_or_none()
    api_key = set_api_key(db_session, sql_api)
    sql_api.edited_by = edited_by
    sql_api.edited_at = datetime.now()

    db_session.commit()

    return api_key

def delete_api_key(db_session, api_id, edited_by):
    sql_api = db_session.query(API).filter(API.id==api_id).one_or_none()
    sql_api.edited_by = edited_by
    sql_api.edited_at = datetime.now()
    sql_api.is_active = False

    db_session.commit()

    return sql_api.api_id

def get_api_key(db_session, org_id):
    resp = dict()
    sql_api = db_session.query(API).filter(API.org_id==org_id).one_or_none()
    if sql_api:
        resp["id"] = sql_api.id
        resp["name"] = sql_api.name
        resp['org_id'] = sql_api.org_id
        resp['requested_by'] = get_user_name(db_session, sql_api.requested_by)
        resp['requested_at'] = sql_api.requested_at
        resp['edited_by'] = get_user_name(db_session, sql_api.edited_by)
        resp['edited_at'] = sql_api.edited_at
        resp['is_active'] = sql_api.is_active
        resp['inactive_reason'] = sql_api.inactive_reason

    return resp

def convert_organization_to_json(sql_org):
    obj = dict()

    obj['id'] = sql_org.Organization_Id
    obj['name'] = sql_org.Organization_Name
    # obj['license_count'] = sql_org.No_Of_Licenses
    # obj['l1_license_count'] = sql_org.No_Of_L1_Licenses
    # obj['l2_license_count'] = sql_org.No_Of_L2_Licenses
    # obj['l3_license_count'] = sql_org.No_Of_L3_Licenses
    obj['lite_license_count'] = sql_org.No_Of_Lite_Licenses
    obj['pro_license_count'] = sql_org.No_Of_Pro_Licenses
    obj['license_expiry'] = sql_org.License_Expiry_Date
    obj['admin_name'] = sql_org.Adminuser_Fullname
    obj['admin_email'] = sql_org.Adminuser_Email
    obj['admin_mobile'] = sql_org.Adminuser_Mobile
    obj['is_data_control_enable'] = sql_org.Is_DatacontrolEnable
    obj['amc_id'] = sql_org.AMC_Id
    obj['is_enterprise_value'] = sql_org.Is_Enterprise_Value
    obj['is_whitelabel'] = sql_org.Is_WhiteLabel_Value
    obj['application_title'] = sql_org.Application_Title
    obj['logo_img'] = sql_org.Logo_Img
    obj['disclaimer'] = sql_org.disclaimer
    obj['is_api_enabled'] = sql_org.is_api_enabled
    obj['api_access_level'] = sql_org.api_access_level
    obj['api_available_hits'] = sql_org.api_available_hits
    obj['api_remote_addresses'] = sql_org.api_remote_addr
    obj['is_excel_export_enabled'] = sql_org.is_excel_export_enabled
    obj['excel_export_count'] = sql_org.excel_export_count
    obj['is_buy_enable'] = sql_org.is_buy_enable
    obj['is_active'] = sql_org.Is_Active
    obj['user_type_id'] = sql_org.usertype_id
    obj['otp_allowed_over_mail'] = sql_org.otp_allowed_over_mail

    return obj

def save_organization_from_prelogin(db_session, org_name, admin_name, admin_email, admin_mobile, lite_license_count, pro_license_count, license_expiry_date, gst_no, config, jinja_env, is_free_trial=0):

    # if organization has a free trial then do not check it's contact info
    resp_organization = check_organization_free_trial(db_session, admin_email, admin_mobile, is_free_trial=is_free_trial)

    if not resp_organization[0]:
        # if it is a new organization, check if this email or mobile phone is already used
        check_user_contact_info(db_session, admin_email, admin_mobile)

    if not resp_organization[0]:
        sql_org = Organization()
    else:
        sql_org = resp_organization[1]

    sql_org.Organization_Name = org_name
    # TODO: Remove following column from database and remove the following code.
    sql_org.No_Of_Licenses = lite_license_count + pro_license_count
    sql_org.No_Of_Lite_Licenses = lite_license_count
    sql_org.No_Of_Pro_Licenses = pro_license_count
    # We are not allowing for subcription of RM model from prelogin, so storing RM License as 0.
    sql_org.No_Of_RM_Licenses = 0
    sql_org.License_Expiry_Date = license_expiry_date
    sql_org.Adminuser_Fullname = admin_name
    sql_org.Adminuser_Email = admin_email
    sql_org.Adminuser_Mobile = admin_mobile
    sql_org.otp_allowed_over_mail = False

    if gst_no:
        sql_org.gst_number = gst_no

    # Wait for the payment
    # do this steps if not free trial
    if not is_free_trial:
        sql_org.Is_Active = False
        sql_org.is_self_subscribed = True
        sql_org.is_payment_pending = True
    else:
        sql_org.Is_Active = True
        sql_org.usertype_id = 10

    if not resp_organization[0]:
        db_session.add(sql_org)

    db_session.commit()

    # Find the highest access level available and give org-admin that access
    if sql_org.No_Of_Pro_Licenses > 0:
        access_level = 'pro'
    elif sql_org.No_Of_Lite_Licenses > 0:
        access_level = 'lite'
    else:
        raise LicenceLimitExceedException("Please provide at least one user with L1 or L2 or L3 access.")

    # access_level = 3
    user_role_id = 2
    
    # create the user only if user has come for self subscription and he have not taken free trial, because if he have took an free trial access, so at the time of free trial user has already been created.
    if not resp_organization[0]:
        # User creation will send the email so no need to send the email twice
        create_new_user(db_session, config, jinja_env, admin_name, admin_email, admin_mobile, 1, sql_org.Organization_Id, user_role_id, access_level, False)
    else:
        if resp_organization[2]:
            send_welcome_email(jinja_env, config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], admin_name, admin_email)

            #send details to business team
            environment = Environment(loader=FileSystemLoader('./src/templates'), keep_trailing_newline=False, trim_blocks=True, lstrip_blocks=True)
            template = environment.get_template("new_trial_user_details_to_business_team.html")
            html_msg = template.render(name=admin_name, email_id=admin_email, mobile=admin_mobile, end_date=license_expiry_date, msg='New user onboarded on portal from prelogin.', total_login='NA', user_type='Trial - prelogin')

            send_email_async(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], config["TRIAL_USER_MAIL_TO"], "New user onboarded on portal from prelogin.", html_msg)


    return sql_org.Organization_Id

def save_organization(db_session, config, jinja_env, form, req_files, org_id = None ):
    admin_name = form['admin_name']
    admin_email = form['admin_email']
    admin_mobile = form['admin_mobile']
    license_expiry_date = date.fromisoformat(form['license_expiry'])

    admin_user_id = None

    if org_id:
        sql_org = db_session.query(Organization).filter(Organization.Organization_Id == org_id).one_or_none()

        # TODO: although following should be one_or_none() query, due to bad data in database, we have to pick the first one.
        sql_user = db_session.query(User).filter(User.Organization_Id==org_id).filter(User.Email_Address==Organization.Adminuser_Email).filter(User.Contact_Number==Organization.Adminuser_Mobile).first()
        admin_user_id = sql_user.User_Id
    else:
        sql_org = Organization()

    send_email = False
    update_user = False

    # if it is a new organization, check if this email or mobile phone is already used
    if not org_id:
        check_user_contact_info(db_session, admin_email, admin_mobile)
        send_email = True
    else:
        # if the email or phone number is being update, check if it is already used or not
        if sql_org.Adminuser_Email != admin_email or sql_org.Adminuser_Mobile != admin_mobile:
            check_user_contact_info(db_session, admin_email, admin_mobile, admin_user_id)
            send_email = True
            update_user = True

        # if the license has expired and has been extended, send an email
        if license_expiry_date > sql_org.License_Expiry_Date and license_expiry_date >= date.today():
            send_email = True

    sql_org.Organization_Name = form['name']
    # TODO: Remove following column from database and remove the following code.
    # sql_org.No_Of_Licenses = form['l1_license_count'] + form['l2_license_count'] + form['l3_license_count']
    # sql_org.No_Of_L1_Licenses = form['l1_license_count']
    # sql_org.No_Of_L2_Licenses = form['l2_license_count']
    # sql_org.No_Of_L3_Licenses = form['l3_license_count']
    sql_org.No_Of_Licenses = int(form['lite_license_count']) + int(form['pro_license_count']) + int(form['rm_license_count'])
    sql_org.No_Of_Lite_Licenses = int(form['lite_license_count'])
    sql_org.No_Of_Pro_Licenses = int(form['pro_license_count'])
    sql_org.No_Of_RM_Licenses = int(form['rm_license_count'])
    sql_org.License_Expiry_Date = license_expiry_date
    sql_org.Adminuser_Fullname = admin_name
    sql_org.Adminuser_Email = admin_email
    sql_org.Adminuser_Mobile = admin_mobile
    sql_org.usertype_id = form["user_type_id"]
    
    otp_allowed_over_mail = False
    if form["otp_allowed_over_mail"]:
        otp_allowed_over_mail = bool(int(form["otp_allowed_over_mail"]))
    sql_org.otp_allowed_over_mail = otp_allowed_over_mail

    if form.get('is_data_control_enable'):
        sql_org.Is_DatacontrolEnable = True
        sql_org.AMC_Id = form['amc_id']

    if form.get('is_enterprise_value'):
        sql_org.Is_Enterprise_Value = True

    if form.get('is_whitelabel'):
        sql_org.Is_WhiteLabel_Value = True
        sql_org.Application_Title = form['application_title']
        if req_files.get('logo_img'):
            file_obj = req_files['logo_img']
            filename, file_extension = os.path.splitext(file_obj.filename)
            sql_org.Logo_Img = save_whitelabel_file(config['DOC_ROOT_PATH'], config['WHITELABEL_DIR'], file_obj, file_extension)
        
        sql_org.disclaimer = form["disclaimer"]

    if form.get('is_api_enabled'):
        sql_org.is_api_enabled = True
        sql_org.api_access_level = form['api_access_level']
        sql_org.api_available_hits = form['api_available_hits']
        sql_org.api_remote_addr = form['api_remote_addresses']

    if form.get('is_buy_enable'):
        sql_org.is_buy_enable = True

    if form.get('is_excel_export_enabled'):
        sql_org.is_excel_export_enabled = True

    if form.get('excel_export_count'):
        sql_org.excel_export_count = True

    if not org_id:
        sql_org.Is_Active = True
        db_session.add(sql_org)

    db_session.commit()

    if update_user:
        sql_user = db_session.query(User).filter(User.User_Id==admin_user_id).one_or_none()
        if sql_user:
            sql_user.Email_Address = admin_email
            sql_user.Contact_Number = admin_mobile
            sql_user.Display_Name = admin_name
            db_session.commit()
        else:
            # TODO: Think about an alternative flow when the user is not active
            pass

    if not org_id:
        # Find the highest access level available and give org-admin that access
        if sql_org.No_Of_Pro_Licenses > 0:
            access_level = 'pro'
        elif sql_org.No_Of_Lite_Licenses > 0:
            access_level = 'lite'
        elif sql_org.No_Of_RM_Licenses > 0:
            access_level = 'rm'
        else:
            raise LicenceLimitExceedException("Please provide at least one user with L1 or L2 or L3 access.")

        # access_level = 3
        user_role_id = 2
        create_new_user(db_session, config, jinja_env, admin_name, admin_email, admin_mobile, 1, sql_org.Organization_Id, user_role_id, access_level, False)
        # User creation will send the email so no need to send the email twice
        send_email = False

    if send_email:
        # TO send email
        send_welcome_email(jinja_env, config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], admin_name, admin_email)

    return sql_org.Organization_Id

def delete_organization(db_session, org_id):
    sql_org = db_session.query(Organization).filter(Organization.Organization_Id == org_id).one_or_none()
    if sql_org:
        sql_org.Is_Active = False
    db_session.commit()

def get_organization(db_session, org_id):
    obj = dict()
    sql_org = db_session.query(Organization).filter(Organization.Organization_Id == org_id).one_or_none()
    if sql_org:
        obj = convert_organization_to_json(sql_org)
    return obj

def get_all_organization(db_session):
    resp = list()
    amc_lookup = dict()
    sql_amcs = db_session.query(AMC).all()
    for sql_amc in sql_amcs:
        amc_lookup[sql_amc.AMC_Id] = sql_amc.AMC_Name


    sql_orgs = db_session.query(Organization).all()
    for sql_org in sql_orgs:
        obj = convert_organization_to_json(sql_org)
        if "amc_id" in obj and obj["amc_id"] is not None:
            obj["amc_name"] = amc_lookup[obj["amc_id"]]
        resp.append(obj)
    return resp

def save_whitelabel_file(root_path, dir, req_file, img_extension=""):
    # get an uuid for the file
    unique_file_name = str(uuid4())
    image_name = unique_file_name + img_extension
    file_path = os.path.join(dir, image_name)
    total_path = os.path.join(root_path, file_path)

    # save the file -> expecting a flask request file object.
    req_file.save(total_path)

    # TODO: make an entry in the database for the docs (or images or assets)
    
    return image_name

def check_user_contact_info(db_session, email, mobile, user_id_to_skip=None):
    result = True

    user_qurery = db_session.query(User).filter(User.Email_Address==email).filter(User.Is_Active==1)
    if user_id_to_skip:
        user_qurery = user_qurery.filter(User.User_Id != user_id_to_skip)
    sql_user = user_qurery.one_or_none()

    if sql_user:
        raise NotUniqueValueException("Same email id is already registered, please use different email id.")

    user_qurery = db_session.query(User).filter(User.Contact_Number==mobile).filter(User.Is_Active==1)
    if user_id_to_skip:
        user_qurery = user_qurery.filter(User.User_Id != user_id_to_skip)
    
    sql_user = user_qurery.one_or_none() if mobile else None

    if sql_user:
        raise NotUniqueValueException("Same mobile number is already registered, please use different mobile number.")
    
    return result

def check_organization_free_trial(db_session, admin_email, admin_mobile, is_free_trial=0):
    email_query = db_session.query(Organization).filter(Organization.Adminuser_Email == admin_email)

    if admin_mobile:
        mobile_query = db_session.query(Organization).filter(Organization.Adminuser_Mobile == admin_mobile)

    org_query = email_query if email_query else mobile_query
    sql_org = org_query.filter(Organization.usertype_id == 10).one_or_none()
    
    # suppose user doest not request for free trial, but first time he applies for self subscription but fails for payment, after that user request for free trial, so user should get free trial
    if not sql_org:
        self_query = org_query.filter(Organization.is_self_subscribed == 1).filter(Organization.is_payment_pending == True).filter(Organization.Is_Active == False).one_or_none()

        if self_query:
            return [True, self_query, False]
        
    if sql_org and is_free_trial:
        raise NotUniqueValueException("You have been already given an access")
    
    if sql_org:
        return [True, sql_org, True]
    else:
        return [False, None, True]
    
def check_user_inputs_for_sso(key, value):
    """
    key >> can be 'email' or 'mobile'
    """
    if key == 'email':
        pattern = re.compile(r"[^@]+@[^@]+\.[^@]+")
    elif key == 'mobile':
        pattern = re.compile("^\\d{10}$")

    if pattern.match(value):
        return True
    
    return False


def create_new_user(db_session, config, jinja_env, name, email, mobile, is_active_status, organization_id, role_id, access_level, download_nav, is_sso_login=False):
    send_email = False
    sql_user = None
  
    # # Check licence count for the access level
    # if access_level == 1:
    #     licence_count = db_session.query(Organization.No_Of_L1_Licenses).filter(Organization.Organization_Id==organization_id).scalar()
    # elif access_level == 2:
    #     licence_count = db_session.query(Organization.No_Of_L2_Licenses).filter(Organization.Organization_Id==organization_id).scalar()
    # elif access_level == 3:
    #     licence_count = db_session.query(Organization.No_Of_L3_Licenses).filter(Organization.Organization_Id==organization_id).scalar()
    # Check licence count for the access level
    if access_level == "lite":
        licence_count = db_session.query(Organization.No_Of_Lite_Licenses).filter(Organization.Organization_Id==organization_id).scalar()
    elif access_level == "pro":
        licence_count = db_session.query(Organization.No_Of_Pro_Licenses).filter(Organization.Organization_Id==organization_id).scalar()
    elif access_level == "rm":
        licence_count = db_session.query(Organization.No_Of_RM_Licenses).filter(Organization.Organization_Id==organization_id).scalar()
    elif access_level == "silver":
        licence_count = db_session.query(Organization.No_Of_Silver_Licenses).filter(Organization.Organization_Id==organization_id).scalar()


    if is_sso_login:
        user_qry = db_session.query(func.count(User.User_Id)).filter(User.Organization_Id==organization_id).filter(User.Access_Level==access_level).filter(User.Is_Active==1)
    else:
        user_qry = db_session.query(func.count(User.User_Id)).filter(User.Organization_Id==organization_id).filter(User.Role_Id.in_((2,3))).filter(User.Access_Level==access_level).filter(User.Is_Active==1)

    user_count = user_qry.scalar()
    if user_count >= licence_count:
        raise LicenceLimitExceedException("You cannot create users more than licence limit.")

    check_user_contact_info(db_session, email, mobile)

    sql_user = User()
    sql_user.User_Name = name
    sql_user.Display_Name = name
    sql_user.Salutation = ""
    sql_user.First_Name = ""
    sql_user.Middle_Name = ""
    sql_user.Last_Name = ""
    sql_user.Gender = 0
    sql_user.Marital_Status = 0
    sql_user.Birth_Date = datetime(2999, 12, 31)
    sql_user.Email_Address = email
    sql_user.Contact_Number = mobile
    sql_user.Login_Failed_Attempts = 0
    sql_user.Is_Account_Locked = 0
    sql_user.Last_Login_Date_Time = datetime.now()
    sql_user.Is_Active = is_active_status
    sql_user.Created_By_User_Id = 0
    sql_user.Created_Date_Time = datetime.now()
    sql_user.Role_Id = role_id
    sql_user.Access_Level = access_level
    sql_user.Organization_Id = organization_id
    sql_user.Activation_Code = str(uuid4())
    sql_user.downloadnav_enabled = download_nav
    sql_user.Is_SSO_Login = is_sso_login
    db_session.add(sql_user)
    db_session.commit()
    send_email = True
    
    if send_email and not is_sso_login:
        mail_sent = False

        organization_details = db_session.query(Organization).filter(Organization.Organization_Id==organization_id).one_or_none()

        if organization_details:
            if organization_details.usertype_id == 4:#trial
                mail_sent = True
                from_date = date.today()
                to_date = organization_details.License_Expiry_Date

                environment = Environment(loader=FileSystemLoader('./src/templates'), keep_trailing_newline=False, trim_blocks=True, lstrip_blocks=True)

                #Send mail to user
                template = environment.get_template("trial_user_welcome_mail.html")
                html_msg = template.render(name=name, start_date=from_date, end_date=to_date)
                
                send_email_async(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], email, "Welcome to Finalyca", html_msg)


                #send details to business team
                template = environment.get_template("new_trial_user_details_to_business_team.html")
                html_msg = template.render(name=name, email_id=email, mobile=mobile, end_date=to_date, msg='New trial user onboarded on portal.', total_login='NA', user_type='NA')

                send_email_async(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], config["TRIAL_USER_MAIL_TO"], "New Trial user onboarded - Portal", html_msg)

                if mobile:
                    sms_text = F"Welcome {name}! \n Explore, learn, and thrive with Finalyca's 2-day Trial. \n Enjoy the journey by clicking on https://portal.finalyca.com/  \n If you need assistance, feel free to reach out. Dimple Turakhia: 9840775773.  \n FNLYCA"
                    
                    sms_config = SMSConfig()
                    sms_config.url = config["SMS_URL"]
                    sms_config.sender_id = config["SMS_SENDER_ID"]
                    sms_config.is_unicode = config["SMS_IS_UNICODE"]
                    sms_config.is_flash = config["SMS_IS_FLASH"]
                    sms_config.api_key = config["SMS_API_KEY"]
                    sms_config.client_id = config["SMS_CLIENT_ID"]
                    if sql_user.Contact_Number:
                        send_sms(sms_config, mobile, sms_text)

        if not mail_sent:        
            send_welcome_email(jinja_env, config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], name, email)

    return sql_user.User_Id


def update_user(db_session, user_id, name, email, mobile, is_active_status, role_id, access_level, designation, city, pin_code, state, download_nav):
    check_user_contact_info(db_session, email, mobile, user_id)

    sql_user = db_session.query(User).filter(User.User_Id==user_id).one_or_none()
    sql_user.Display_Name = name
    sql_user.Email_Address = email
    sql_user.Contact_Number = mobile
    sql_user.Is_Active = is_active_status
    sql_user.Role_Id = role_id
    sql_user.Access_Level = access_level
    sql_user.Designation = designation
    sql_user.City = city
    sql_user.State = state
    sql_user.Pin_Code = pin_code
    sql_user.downloadnav_enabled = download_nav
    db_session.commit()

def update_user_profile(db_session, user_id, name, email, mobile, designation, city, state, pin_code, files, config):
    check_user_contact_info(db_session, email, mobile, user_id)

    sql_user = db_session.query(User).filter(User.User_Id==user_id).one_or_none()
    sql_user.Display_Name = name
    sql_user.Email_Address = email
    sql_user.Contact_Number = mobile
    sql_user.Designation = designation
    sql_user.City = city
    sql_user.State = state
    sql_user.Pin_Code = pin_code

    if files.get('profile_upload'):
        file_obj = files['profile_upload']
        filename = secure_filename(file_obj.filename)
        file_path = save_file(config["DOC_ROOT_PATH"], config["USER_PROFILE_DIR"], file_obj, True)
        sql_user.Profile_Picture = file_path

    db_session.commit()

def send_welcome_email(jinja_env, finalyca_email_server, finalyca_email_port, finalyca_email_id, finalyca_email_pwd, user_name, user_email, mail_body_message=None):
    if not mail_body_message:
        mail_body_message = "Thanks for signing up for Finalcya - India's First All - In - One Investment Analytics Platform."

    templ = jinja_env.get_or_select_template("welcome_email.html")
    html_message = templ.render(name=user_name, message=mail_body_message)
    send_email_async(finalyca_email_server, finalyca_email_port, finalyca_email_id, finalyca_email_pwd, user_email, "Welcome to Finalyca", html_message)

def convert_user_to_json(db_session, sql_user):
    user = dict()
    user["id"] = sql_user.User_Id
    user["name"] = sql_user.User_Name
    user["first_name"] = sql_user.First_Name
    user["middle_name"] = sql_user.Middle_Name
    user["last_name"] = sql_user.Last_Name
    user["mobile"] = sql_user.Contact_Number
    user["email"] = sql_user.Email_Address
    user["is_active"] = sql_user.Is_Active
    user["organization_id"] = sql_user.Organization_Id
    if sql_user.Organization_Id:
        sql_org = db_session.query(Organization).filter(Organization.Organization_Id==sql_user.Organization_Id).one_or_none()
        if sql_org:
            user["organization_name"] = sql_org.Organization_Name if sql_org.Organization_Name else None
    user["role_id"] = sql_user.Role_Id
    user["access_level"] = sql_user.Access_Level
    user["download_nav"] = sql_user.downloadnav_enabled
    return user

def get_all_users(db_session):
    resp = list()
    sql_users = db_session.query(User).all()
    for sql_user in sql_users:
        obj = convert_user_to_json(db_session, sql_user)
        resp.append(obj)
    return resp

import re
def email_validation_for_free_trial(email_id):
    avoid_emails = ["gmail", "yahoo", "outlook", "hotmail", "aol", "msn", "ymail", "rediffmail", "refiffmail"]
    domain_of_email = email_id.split('@')[1]
    for email in avoid_emails:
        if bool(re.search(email, domain_of_email)):
            raise Exception('We do not accept email ids like gmail, yahoo, outlook, etc.')
            
def get_all_delivery_requests(db_session, user_id):
    resp = list()
    
    sql_users_delivery_requests = db_session.query(DeliveryRequest)\
                                    .join(DeliveryManager, DeliveryManager.Channel_Id == DeliveryRequest.Channel_Id)\
                                    .filter(DeliveryManager.Enabled == 1)\
                                    .filter(DeliveryRequest.Is_Deleted != 1)\
                                    .filter(DeliveryRequest.Type == 'X-Ray PDF')\
                                    .filter(DeliveryRequest.Created_By == user_id).all()
    for sql_request in sql_users_delivery_requests:
        obj = dict()
        obj['type'] = sql_request.Type
        obj['request_time'] = sql_request.Request_Time
        obj['status_msg'] = sql_request.Status_Message
        obj['status'] = sql_request.Status
        obj['x_token'] = sql_request.X_Token
        resp.append(obj)
    return resp