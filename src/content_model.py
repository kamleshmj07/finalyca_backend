from flask import request, jsonify, Blueprint, current_app
from fin_models.masters_models import Content
from bizlogic.content_model import get_model_content, save_model_content, edit_model_content, get_data_by_request, delete_model_content
from src.utils import get_user_info

content_model_bp = Blueprint("content_model_bp", __name__)

@content_model_bp.route('/content_model', methods=["GET"])
def api_get_all_model_content():
    # TODO: If require add filters
    resp = get_model_content(current_app.store.db, None)
    return jsonify(resp)


@content_model_bp.route('/content_model/<int:content_id>', methods=["GET"])
def api_get_one_model_content(content_id):
    # TODO: If require add filters
    resp = get_model_content(current_app.store.db, content_id)
    return jsonify(resp)

@content_model_bp.route('/content_model/<int:content_id>', methods=["DELETE"])
def api_delete_one_model_content(content_id):
    # TODO: If require add filters
    resp = delete_model_content(current_app.store.db, content_id)
    return jsonify(resp)

@content_model_bp.route('/content_model/<int:content_id>', methods=["PUT"])
def api_edit_model_content(content_id):
    # TODO: If require add filters
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    request_data = request.form

    content_request = dict()
    content_request = get_data_by_request(request_data, user_id)

    resp = edit_model_content(current_app.store.db, content_request, content_id)
    return jsonify(resp)


@content_model_bp.route('/content_model', methods=["POST"])
def api_save_model_content():
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    user_id = user_info["id"]
    org_id = user_info["org_id"]

    request_data = request.form

    content_request = dict()
    content_request = get_data_by_request(request_data, user_id)

    content_id = save_model_content(current_app.store.db, content_request)

    return jsonify({"content_id": content_id})
    
    