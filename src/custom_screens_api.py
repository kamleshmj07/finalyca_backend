from datetime import datetime
import json
from uuid import uuid4
from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import and_
import werkzeug
from werkzeug.exceptions import BadRequest
from fin_models.controller_transaction_models import CustomScreens, CustomScreenAccess
from fin_models.transaction_models import FundScreener, DebtScreener
from fin_models.masters_models import AssetClass, Options
from .data_access import *
from fin_resource.screener import process_query_builder
from fin_resource.exceptions import NotSupportedException, MissingInfoException
from .utils import get_user_info, get_org_name
from .utils import required_user_access_pro

screener_bp = Blueprint("screener_bp", __name__)

def custom_screen_sql_to_json(db_session, sql_obj: CustomScreens):
    obj = dict()
    obj["id"] = sql_obj.id
    obj["uuid"] = sql_obj.uuid
    obj["name"] = sql_obj.name
    obj["description"] = sql_obj.description
    obj["query_json"] = json.dumps(sql_obj.query_json)
    obj["access"] = CustomScreenAccess[sql_obj.access].value
    obj["is_active"] = sql_obj.is_active
    # obj["created_by"] = get_user_name(db_session, sql_obj.created_by)
    obj["created_by_org"] = get_org_name(db_session, sql_obj.created_by)
    obj["created_by_user_id"] = sql_obj.created_by
    obj["created_at"] = sql_obj.created_at
    # obj["modified_by"] = get_user_name(db_session, sql_obj.modified_by)
    # obj["modified_at"] = sql_obj.modified_at
    return obj

def get_screen_obj(screen_id, user_id, org_id):
    sql_obj = current_app.store.db.query(CustomScreens).filter(CustomScreens.id == screen_id).one_or_none()

    if sql_obj:
        # either it has to be public
        if sql_obj.access == CustomScreenAccess.public.name:
            return sql_obj

        # else restricted and same org id
        if sql_obj.access == CustomScreenAccess.organization.name and sql_obj.org_id == org_id:
            return sql_obj

        # else private and same user id
        if sql_obj.access == CustomScreenAccess.personal.name and sql_obj.created_by == user_id:
            return sql_obj

        raise werkzeug.exceptions.Unauthorized(description="User does not have access to this screen")


@screener_bp.route('/screener/entities', methods=['GET'])
@required_user_access_pro
def get_screener_entities():
    entities = dict()
    entities['FundScreener'] = "Funds"
    entities['DebtScreener'] = "Debt"
    return jsonify(entities)


def validate_screener(query_json):
    # Has to have integer version info 
    version = query_json["version"]
    
    if "rules" not in query_json:
        raise MissingInfoException("Query does not have any rule.")

    if "entity" not in query_json:
        raise MissingInfoException("Query does not have any entity.")

    entity = query_json["entity"]
    if entity == "Funds":
        model_name = FundScreener.__tablename__
    elif entity == "Debt":
        model_name = DebtScreener.__tablename__

    ids = process_query_builder(current_app.store.db, model_name, query_json)
    
    return ids


def build_screener(query_json):
    try:
        res = validate_screener(query_json)
        return res

    except MissingInfoException as ex:
        raise BadRequest(description=str(ex))
    except NotSupportedException as ex:
        raise BadRequest(description=str(ex))


@screener_bp.route("/screener/try", methods=['POST'])
@required_user_access_pro
def screener():
    f = request.json

    res = build_screener(f)
    return jsonify(res)    

@screener_bp.route('/screener/<int:screen_id>', methods=['GET'])
@required_user_access_pro
def get_screen(screen_id):
    res = dict()
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]
    sql_obj = get_screen_obj(screen_id, user_id, org_id)

    query_json = sql_obj.query_json
    
    res = build_screener(query_json)
    return jsonify(res)  

    # model_name = query_json["entity"]
    # version = query_json["version"]

    # fund_ids = process_query_builder_for_entity(current_app.store.db, model_name, "fund_id", query_json)
    # res = get_fund_views(current_app.store.db, fund_ids)
    # return jsonify(res)


@screener_bp.route('/screener', methods=['GET'])
@required_user_access_pro
def get_all_screens():
    screens = dict()

    # Check user id of the request and get organization id
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    custom_screen_qry = current_app.store.db.query(CustomScreens)

    # get all public screens
    public_screens = list()
    sql_public = custom_screen_qry.filter(CustomScreens.access==CustomScreenAccess.public.name).all()
    for sql_p in sql_public:
        public_screens.append(custom_screen_sql_to_json(current_app.store.db, sql_p))
    screens["public"] = public_screens

    # get restricted screens for the organization id if any
    restricted_screens = list()
    sql_restricted = custom_screen_qry.filter(CustomScreens.access==CustomScreenAccess.organization.name,
                                              CustomScreens.org_id==org_id).all()
    for sql_r in sql_restricted:
        restricted_screens.append(custom_screen_sql_to_json(current_app.store.db, sql_r))
    screens["organization"] = restricted_screens

    # get all private screens
    private_screens = list()
    sql_private = custom_screen_qry.filter(CustomScreens.access==CustomScreenAccess.personal.name,
                                           CustomScreens.created_by==user_id).all()
    for sql_pr in sql_private:
        private_screens.append(custom_screen_sql_to_json(current_app.store.db, sql_pr))
    screens["personal"] = private_screens

    return jsonify(screens)


@screener_bp.route('/screener', methods=['POST'])
@required_user_access_pro
def create_new_screens():
    # TODO: make name and UUID4 unique in database. and make checks for that.
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    input = request.json
    new_screen = CustomScreens()
    # TODO: check if uuid4 ensures us to get a unique value else we may have to put a check in database
    new_screen.uuid = str(uuid4())
    new_screen.name = input["name"]
    new_screen.description = input["description"]
    new_screen.query_json = input["query_json"]
    new_screen.access = input["access"]
    new_screen.is_active = True
    new_screen.created_by = user_id
    new_screen.created_at = datetime.now()
    new_screen.org_id = org_id
    current_app.store.db.add(new_screen)
    current_app.store.db.commit()

    id = new_screen.id

    return jsonify({"id": id})

@screener_bp.route('/screener/<int:screen_id>', methods=['PUT'])
@required_user_access_pro
def edit_screens(screen_id):
    screens = dict()

    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    sql_obj = get_screen_obj(screen_id, user_id, org_id)
    input = request.json

    sql_obj.name = input["name"]
    sql_obj.description = input["description"]
    sql_obj.query_json = input["query_json"]
    sql_obj.access = input["access"]

    current_app.store.db.commit()

    id = sql_obj.id

    return jsonify({"id": id})

@screener_bp.route('/screener/<int:screen_id>', methods=['DELETE'])
@required_user_access_pro
def delete_screens(screen_id):
    screens = dict()

    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    sql_obj = get_screen_obj(screen_id, user_id, org_id)
    current_app.store.db.delete(sql_obj)
    current_app.store.db.commit()

    return jsonify(screens)

@screener_bp.route('/screener/helper/<fund_name>', methods=['GET'])
@required_user_access_pro
def get_plan_info(fund_name):
    obj = dict()
    sql_plans = current_app.store.db.query(Plans.Plan_Id, Plans.Plan_Name, Plans.Plan_Code, Fund.Fund_Id, Fund.Fund_Name, Fund.Fund_Code, MFSecurity.AMC_Id, MFSecurity.Classification_Id).join(MFSecurity, and_(MFSecurity.MF_Security_Id==Plans.MF_Security_Id, MFSecurity.Is_Deleted != 1)).join(Options, Options.Option_Id == Plans.Option_Id).join(Fund, Fund.Fund_Id==MFSecurity.Fund_Id).filter(Fund.Fund_Name==fund_name).filter(Fund.Is_Deleted!= 1).filter(Options.Option_Name.like('%G%')).filter(MFSecurity.Status_Id==1).first()

    if sql_plans:
        obj["plan_id"] = sql_plans[0]
        obj["plan_name"] = sql_plans[1]
        obj["plan_code"] = sql_plans[2]
        obj["fund_id"] = sql_plans[3]
        obj["fund_name"] = sql_plans[4]
        obj["fund_code"] = sql_plans[5]
        obj["amc_id"] = sql_plans[6]
        obj["classification_id"] = sql_plans[7]
    return jsonify(obj)

@screener_bp.route('/screener/fund_helper', methods=['GET'])
@required_user_access_pro
def get_plan_info_from_fund_name():

    fund_names = request.args.getlist("fund_name")

    plans = list()
    
    sql_plans = current_app.store.db.query(Plans.Plan_Id, Plans.Plan_Name, Plans.Plan_Code, Fund.Fund_Id, Fund.Fund_Name, Fund.Fund_Code, MFSecurity.AMC_Id, MFSecurity.Classification_Id).join(MFSecurity, and_(MFSecurity.MF_Security_Id==Plans.MF_Security_Id, MFSecurity.Is_Deleted != 1)).join(Options, Options.Option_Id == Plans.Option_Id).join(Fund, Fund.Fund_Id==MFSecurity.Fund_Id).filter(Fund.Fund_Name.in_(fund_names)).filter(Fund.Is_Deleted!= 1).filter(Options.Option_Name.like('%G%')).filter(MFSecurity.Status_Id==1).all()

    for sql_plan in sql_plans:
        obj = dict()
        obj["plan_id"] = sql_plan[0]
        obj["plan_name"] = sql_plan[1]
        obj["plan_code"] = sql_plan[2]
        obj["fund_id"] = sql_plan[3]
        obj["fund_name"] = sql_plan[4]
        obj["fund_code"] = sql_plan[5]
        obj["amc_id"] = sql_plan[6]
        obj["classification_id"] = sql_plan[7]
        plans.append(obj)
    
    return jsonify(plans)

def get_fund_views(db_session, fund_id_list):
    obj_list = list()
    sql_objs = db_session.query(FundScreener, Product, Plans, Fund, AMC, Classification, AssetClass).join(Product, Product.Product_Id==FundScreener.product_id).join(Plans, Plans.Plan_Id==FundScreener.plan_id).join(Fund, Fund.Fund_Id==FundScreener.fund_id).join(AMC, AMC.AMC_Id==FundScreener.amc_id).join(Classification, Classification.Classification_Id==FundScreener.classification_id).join(AssetClass, AssetClass.AssetClass_Id==FundScreener.asset_class_id).filter(FundScreener.fund_id.in_(fund_id_list)).all()
    for sql_obj in sql_objs:
        obj = dict()
        sql_fund_screener = sql_obj[0]
        obj['plan_id'] = sql_fund_screener.plan_id
        obj['fund_id'] = sql_fund_screener.fund_id
        obj['transaction_date'] = sql_fund_screener.transaction_date
        obj['portfolio_date'] = sql_fund_screener.portfolio_date
        obj['expense_ratio'] = sql_fund_screener.expense_ratio
        obj['total_stocks'] = sql_fund_screener.total_stocks
        obj['asset_class_id'] = sql_fund_screener.asset_class_id
        obj['net_assets_in_cr'] = sql_fund_screener.net_assets_in_cr
        obj['avg_market_cap_in_cr'] = sql_fund_screener.avg_market_cap_in_cr
        obj['pb_ratio'] = sql_fund_screener.pb_ratio
        obj['pe_ratio'] = sql_fund_screener.pe_ratio
        obj['returns_1_month'] = sql_fund_screener.returns_1_month
        obj['returns_3_months'] = sql_fund_screener.returns_3_months
        obj['returns_6_months'] = sql_fund_screener.returns_6_months
        obj['returns_1_yr'] = sql_fund_screener.returns_1_yr
        obj['returns_2_yr'] = sql_fund_screener.returns_2_yr
        obj['returns_3_yr'] = sql_fund_screener.returns_3_yr
        obj['returns_5_yr'] = sql_fund_screener.returns_5_yr
        obj['returns_10_yr'] = sql_fund_screener.returns_10_yr
        obj['returns_since_inception'] = sql_fund_screener.returns_since_inception

        sql_product = sql_obj[1]
        obj['product'] = sql_product.Product_Name

        sql_plan = sql_obj[2]
        obj['plan_name'] = sql_plan.Plan_Name

        sql_fund = sql_obj[3]
        obj['fund'] = sql_fund.Fund_Name

        sql_amc = sql_obj[4]
        obj['amc_logo'] = sql_amc.AMC_Logo
        obj['amc_name'] = sql_amc.AMC_Name
        obj['amc_id'] = sql_amc.AMC_Id

        sql_classification = sql_obj[5]
        obj['classification'] = sql_classification.Classification_Name
        
        sql_asset_class = sql_obj[6]
        obj['asset_class'] = sql_asset_class.AssetClass_Name

        obj_list.append(obj)

    return obj_list