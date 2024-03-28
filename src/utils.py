import json
from collections import defaultdict
from fin_models.controller_master_models import User, Organization
from utils.utils import AuthEntityType
from functools import wraps
from flask import g
from werkzeug.exceptions import Forbidden
from utils.utils import validate_jwt_token


def get_user_info(req, db_session, secret_key):
    if "X_User_Id" in req.headers:
        user_id = req.headers.get("X_User_Id", type=int)

    if "X_Token" in req.headers:
        token = req.headers.get("X_Token")
        user_id = validate_jwt_token(db_session, token, secret_key)[1]

    user_info = dict()
    sql_user = db_session.query(User, Organization).join(Organization, User.Organization_Id==Organization.Organization_Id).filter(User.User_Id == user_id).one_or_none()
    if sql_user:
        user_info["id"] = user_id
        user_info["user_name"] = sql_user[0].Display_Name
        user_info["email"] = sql_user[0].Email_Address
        user_info["org_id"] = sql_user[0].Organization_Id
        user_info["org_name"] = sql_user[1].Organization_Name
        user_info["role_id"] = sql_user[0].Role_Id
        user_info["org_has_api"] = sql_user[1].is_api_enabled
        user_info["org_has_buy"] = sql_user[1].is_buy_enable

    return user_info

def get_user_name(db_session, user_id):
    user_name = None
    if user_id:
        user_name = db_session.query(User.Display_Name).filter(User.User_Id==user_id).scalar()
    return user_name

def get_access_level():
    access_level = 0
    
    if "access" in g:
        access_level = g.access.entity_access_level

    return access_level

def is_api_auth():
    is_api = False
    
    if "access" in g:
        if g.access.entity_type == AuthEntityType.api:
            is_api = True

    return is_api

def is_user_auth():
    is_user = False
    
    if "access" in g:
        if g.access.entity_type == AuthEntityType.user:
            is_user = True

    return is_user

def required_user_access_pro(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_user_auth():
            return f(*args, **kwargs)

        access_level = get_access_level()
        if access_level == "pro":
            return f(*args, **kwargs)
        else:
            raise Forbidden(description="current access level does not allow to access this resource.")
        
    return decorated_function

def required_access_l1(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_api_auth():
            return f(*args, **kwargs)

        access_level = get_access_level()
        if access_level == 1 or access_level == 2 or access_level == 3:
            return f(*args, **kwargs)
        else:
            raise Forbidden(description="current access level does not allow to access this resource.")
        
    return decorated_function

def required_access_l2(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_api_auth():
            return f(*args, **kwargs)

        access_level = get_access_level()        
        if access_level == 2 or access_level == 3:
            return f(*args, **kwargs)
        else:
            raise Forbidden(description="current access level does not allow to access this resource.")
        
    return decorated_function

def required_access_l3(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_api_auth():
            return f(*args, **kwargs)

        access_level = get_access_level()        
        if access_level == 3:
            return f(*args, **kwargs)
        else:
            raise Forbidden(description="current access level does not allow to access this resource.")
        
    return decorated_function

def get_org_name(db_session, user_id):
    org_name = None
    if user_id:
        org_name = db_session.query(Organization.Organization_Name).join(User, User.Organization_Id==Organization.Organization_Id).filter(User.User_Id==user_id).scalar()
    return org_name

def create_filter_obj(filter_name, min_value, max_value, unit, step):
    return {'Filters': filter_name, "min": min_value, "max": max_value, "unit": unit, "step": step}




def parse_nested_grouped_df_to_dict(df):
    if df.ndim == 1:
        return df.to_dict()

    ret = {}
    for key in df.index.get_level_values(0):
        sub_df = df.xs(key)
        ret[key] = parse_nested_grouped_df_to_dict(sub_df)

    return ret
