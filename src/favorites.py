from flask import request, Blueprint, jsonify, current_app
from .utils import get_user_info
from bizlogic.favorites import get_favorites, save_favorite, delete_favorite
from .access_bl import NotUniqueValueException
from werkzeug.exceptions import Conflict

favorites_bp = Blueprint("favorites_bp", __name__)

@favorites_bp.route('/favorites', methods=['GET', 'POST'])
def api_favorites():
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    if request.method == 'GET':
        resp = get_favorites(current_app.store.db, user_info["id"])
        return jsonify(resp)
        
    if request.method == 'POST':
        try:
            data = request.json if request.json else request.form
            resp_favorite_id = save_favorite(current_app.store.db, data, user_info["id"])
            return jsonify({"msg": F'Added to favorites', "id": resp_favorite_id})
        except NotUniqueValueException as exe:
            raise Conflict(str(exe))
    
@favorites_bp.route('/favorites/<int:favorites_id>', methods=['DELETE'])
def api_delete_favorites(favorites_id):
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    resp = delete_favorite(current_app.store.db, favorites_id, user_info["id"])
    return jsonify({"msg": resp, "id": favorites_id})


