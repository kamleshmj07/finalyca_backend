from fin_models.controller_master_models import API
from flask import current_app, Blueprint, jsonify, request, g
from werkzeug.exceptions import BadRequest, Conflict, Forbidden
from src.utils import get_user_info

from .access_bl import *

access_admin_bp = Blueprint("access_admin_bp", __name__)

@access_admin_bp.route("/access/admin/organization", methods=['GET', 'POST'])
def organization_functions():
    if request.method == 'GET':
        objs = get_all_organization(current_app.store.db)
        return jsonify(objs)
    elif request.method == 'POST':
        try:
            id = save_organization(current_app.store.db, current_app.config, current_app.jinja_env, request.form, request.files)
            return jsonify({"id": id})
        except NotUniqueValueException as exe:
            raise Conflict(str(exe))

@access_admin_bp.route("/access/admin/organization/<int:org_id>", methods=["GET", "PUT", "DELETE"])
def organization_functions_single(org_id):
    if request.method == 'GET':
        obj = get_organization(current_app.store.db, org_id)
        return jsonify(obj)

    elif request.method == 'PUT':
        try:
            id = save_organization(current_app.store.db, current_app.config, current_app.jinja_env, request.form, request.files, org_id)
            return jsonify({"id": id})
        except NotUniqueValueException as exe:
            raise Conflict(str(exe))

    elif request.method == 'DELETE':
        delete_organization(current_app.store.db, org_id)
        return jsonify({"id": org_id})

@access_admin_bp.route('/access/admin/user', methods=['GET', 'POST'])
def user_functions():
    if request.method == 'GET':
        objs = get_all_users(current_app.store.db)
        return jsonify(objs)

    if request.method == 'POST':
        try:
            # create_admin_user(current_app.store.db, request.form, current_app.config, current_app.jinja_env)
            organization_id = request.form.get("organization_id")
            name = request.form.get("name")
            email = request.form.get("email")
            mobile = request.form.get("mobile")
            is_active = request.form.get("is_active", type=int)
            access_level = request.form.get("access_level", default='pro')
            role_id = request.form.get("role_id", type=int, default=3)
            download_nav = request.form.get("download_nav", type=int, default=0)
            
            new_user = create_new_user(current_app.store.db, current_app.config, current_app.jinja_env, name, email, mobile, is_active, organization_id, role_id, access_level, download_nav)

        except NotUniqueValueException as exe:
            raise Conflict(str(exe))

        except LicenceLimitExceedException as exe:
            raise Conflict(str(exe))

        return jsonify({"msg": "A new user is created."})

@access_admin_bp.route("/access/admin/user/<int:user_id>", methods=['PUT'])
def save_admin_user(user_id):
    try:
        if request.method == 'PUT':
            # update_admin_user(current_app.store.db, user_id, request.form)
            name = request.form.get("name")
            email = request.form.get("email")
            mobile = request.form.get("mobile")
            status = request.form.get("is_active", type=int)
            access_level = request.form.get("access_level", default='pro')
            role_id = request.form.get("role_id", type=int, default=3)
            download_nav = request.form.get("download_nav", type=int, default=0)
            user_obj = update_user(current_app.store.db, user_id, name, email, mobile, status, role_id, access_level, None, None, None, None, download_nav)

            return jsonify({"msg": "User information has been edited."})
            
    except NotUniqueValueException as exe:
        raise Conflict(str(exe))

    except LicenceLimitExceedException as exe:
        raise Conflict(str(exe))

@access_admin_bp.route('/access/admin/deliveryrequest', methods=['GET'])
def deliveryrequest():
    if request.method == 'GET':
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        user_id = user_info["id"]

        objs = get_all_delivery_requests(current_app.store.db, user_id)
        return jsonify(objs)