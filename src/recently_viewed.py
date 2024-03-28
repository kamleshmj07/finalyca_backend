from flask import request, Blueprint, jsonify, current_app
from .utils import get_user_info
from bizlogic.recently_viewed import get_recently_viewed, save_recently_viewed, get_top_searched_amc_and_funds
from .access_bl import NotUniqueValueException
from werkzeug.exceptions import Conflict

recently_viewed_bp = Blueprint("recently_viewed_bp", __name__)

@recently_viewed_bp.route('/recently_viewed', methods=['GET', 'POST'])
def api_recently_viewed():
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    if request.method == 'GET':
        resp = get_recently_viewed(current_app.store.db, user_info["id"])
        return jsonify(resp)
        
    if request.method == 'POST':
        try:
            data = request.json if request.json else request.form
            resp_recently_viewed_id = save_recently_viewed(current_app.store.db, data, user_info["id"])
            return jsonify({"msg": F'Added to recently viewed storage', "id": resp_recently_viewed_id})
        except NotUniqueValueException as exe:
            raise Conflict(str(exe))
        
@recently_viewed_bp.route('/top_searched', methods=['GET'])
def api_top_searched():
    resp = get_top_searched_amc_and_funds(current_app.store.db)
    return jsonify(resp)
