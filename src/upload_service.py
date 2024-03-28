from datetime import datetime as dt
from fin_models.servicemanager_models import UploadRequest, UploadTemplates
from flask import current_app, Blueprint, jsonify, request, g
from src.utils import get_user_info
from werkzeug.utils import secure_filename
from uuid import uuid4
import os

upload_bp = Blueprint("upload_bp", __name__)

# TODO: Merge this function with 2 other functions (from lib->admin_store->admin.py->save_image_file and backend->src->access_bl.py)
def save_file(root_path, dir, req_file, save_with_name):
    if save_with_name:
        filename = secure_filename(req_file.filename)
    else:
        # get an uuid for the file
        filename = str(uuid4())

    file_path = os.path.join(dir, filename)
    total_path = os.path.join(root_path, file_path)

    # save the file -> expecting a flask request file object.
    req_file.save(total_path)
    
    return file_path

def save_upload_request(db_session, template_id, file_obj, user_id):
    sql_request = UploadRequest()
    sql_request.UploadTemplates_Id = template_id
    filename = secure_filename(file_obj.filename)
    file_path = save_file(current_app.config["DOC_ROOT_PATH"], current_app.config["UPLOAD_DIR"], file_obj, True)
    sql_request.File_Name = filename
    sql_request.Request_Time = dt.now()
    sql_request.Status = 0
    sql_request.Status_Message = 'Pending'
    sql_request.Is_Deleted = 0
    sql_request.File_Url = F"{current_app.config['IMAGE_PATH']}{current_app.config['UPLOAD_DIR']}/"
    sql_request.Created_By = user_id
    db_session.add(sql_request)
    db_session.commit()
    return sql_request.UploadRequest_Id    

def delete_upload_request(db_session, request_id, user_id):
    sql_request = db_session.query(UploadRequest).filter(UploadRequest.UploadRequest_Id==request_id).one_or_none()
    if sql_request:
        sql_request.Is_Deleted = 1
        db_session.commit()

def get_all_upload_request(db_session):
    response = list()
    sql_requests = db_session.query(UploadRequest, UploadTemplates).join(UploadTemplates, UploadTemplates.UploadTemplates_Id==UploadRequest.UploadTemplates_Id).all()
    for sql_req in sql_requests:
        obj = dict()
        obj["id"] = sql_req[0].UploadRequest_Id
        obj["template_name"] = sql_req[1].UploadTemplates_Name
        obj["file_name"] = sql_req[0].File_Name
        obj["request_file"] = sql_req[0].File_Url
        obj["response_file"] = sql_req[0].File_Url
        obj["request_time"] = sql_req[0].Request_Time
        obj["pick_time"] = sql_req[0].Pick_Time
        obj["completion_time"] = sql_req[0].Completion_Time
        obj["status"] = sql_req[0].Status
        obj["status_message"] = sql_req[0].Status_Message
        response.append(obj)

    return response

@upload_bp.route("/upload", methods=['GET', 'POST'])
def upload_functions():
    if request.method == 'GET':
        objs = get_all_upload_request(current_app.store.db)
        return jsonify(objs)

    elif request.method == 'POST':
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        user_id = user_info["id"]
        template_id = request.form.get("template_id", type=int)
        file_obj = request.files.get("file")        
        id = save_upload_request(current_app.store.db, template_id, file_obj, user_id)
        return jsonify({"id": id})

@upload_bp.route("/upload/<int:req_id>)", methods=['DELETE'])
def api_delete_request(req_id):
    if request.method == 'DELETE':
        user_info = get_user_info(request, current_app.store.db, current_app.config['SECRET_KEY'])
        user_id = user_info["id"]
        objs = delete_upload_request(current_app.store.db, req_id, user_id)
        return jsonify(objs)
