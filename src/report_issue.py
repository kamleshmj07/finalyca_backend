from flask import request, Blueprint, jsonify, current_app
from bizlogic.report_issue import get_reported_issues, save_report_issue
from .utils import get_user_info

report_issue_bp = Blueprint("report_issue_bp", __name__)

@report_issue_bp.route('/report_issue', methods=['GET', 'POST'])
def api_favorites():
    user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
    if request.method == 'GET':
        resp = get_reported_issues(current_app.store.db,  current_app.config)
        return jsonify(resp)
    
    if request.method == 'POST':
        data = request.json if request.json else request.form
        file = request.files
        resp = save_report_issue(current_app.store.db, current_app.config, data, file, user_info)
        return jsonify({"msg": F'Issue Reported', "id": resp})


