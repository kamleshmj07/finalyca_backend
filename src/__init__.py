import imp
from logging.handlers import RotatingFileHandler
import os
import logging
from flask import Flask, _app_ctx_stack
from flask_cors import CORS
from sqlalchemy import orm
from utils.finalyca_store import *
from utils.utils import *
from utils.finalyca_json_encoder import FinalycaJSONEncoder
from utils.flask_helper import *
from fin_models.controller_master_models import Organization

def create_app():
    app = Flask(__name__)
    app.json_encoder = FinalycaJSONEncoder

    # log = logging.getLogger('werkzeug')
    # logging.getLogger().setLevel(logging.INFO)
    handler = RotatingFileHandler(os.path.join(app.root_path, '../logs', 'log.log'), maxBytes=102400, backupCount=10)
    logging_format = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
    handler.setFormatter(logging_format)
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)

    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )

    config = get_config(config_file_path)
    app.store = get_data_store(config, _app_ctx_stack.__ident_func__)
    app.config.update(config)
    db_session = get_finalyca_scoped_session(True)
    
    list_of_origins = [ 
            "http://localhost:*", 
            "https://demo.finalyca.com",
            "https://demoui.finalyca.com", 
            "https://portal.finalyca.com", 
            "https://preprod.finalyca.com/",
            "https://finalyca.com"            
            ]
    
    whitelisted_ips = db_session.query(Organization.api_remote_addr)\
                                .filter(Organization.is_api_enabled == 1,
                                        Organization.Is_Active == 1).all()
    for ips in whitelisted_ips:
        list_of_origins.extend(json.loads(ips.api_remote_addr))
   

    cors = CORS()
    cors.init_app(
        app, 
        supports_credentials=True, 
        methods=['GET', 'HEAD', 'POST', 'OPTIONS', 'PUT', 'PATCH', 'DELETE', 'DIALOG'] , 
        resources={r"*": {"origins": list_of_origins}} 
    )
    app.config['CORS_HEADERS'] = 'Content-Type'

    import razorpay
    app.razor = razorpay.Client(auth=(app.config['RAZORPAY_KEY_ID'], app.config['RAZORPAY_KEY_SECRET']))
    app.razor.set_app_details({"title" : "Finalyca", "version" : "1."})

    app.before_request(fn_before_request)
    app.after_request(fn_after_request)
    app.teardown_request(fn_teardown_request)
    
    app.register_error_handler(Exception, exception_jsonifier)

    from .portfolio import portfolio_bp
    with app.app_context():
        app.register_blueprint(portfolio_bp)
    
    from .fin_razorpay import payment_bp
    with app.app_context():
        app.register_blueprint(payment_bp)

    from .api import api_bp
    with app.app_context():
        app.register_blueprint(api_bp)

    from .favorites import favorites_bp
    with app.app_context():
        app.register_blueprint(favorites_bp)

    from .access_admin_api import access_admin_bp
    with app.app_context(): 
        app.register_blueprint(access_admin_bp)

    from .access_org_api import org_admin_bp
    with app.app_context():
        app.register_blueprint(org_admin_bp)

    from .access_user_api import user_service_bp
    with app.app_context():
        app.register_blueprint(user_service_bp)

    from .exposure import exposure_bp
    with app.app_context():
        app.register_blueprint(exposure_bp)

    from .exposure_sp import exposure_sp_bp
    with app.app_context():
        app.register_blueprint(exposure_sp_bp)

    from .client_api import client_bp
    with app.app_context():
        app.register_blueprint(client_bp)

    from .custom_screens_api import screener_bp
    with app.app_context():
        app.register_blueprint(screener_bp)

    from .account_aggregator import account_aggregator_bp
    with app.app_context():
        app.register_blueprint(account_aggregator_bp)

    from .report_issue import report_issue_bp
    with app.app_context():
        app.register_blueprint(report_issue_bp)

    from .recently_viewed import recently_viewed_bp
    with app.app_context():
        app.register_blueprint(recently_viewed_bp)

    from .fixed_income_factsheet import fixed_income_factsheet_bp
    with app.app_context():
        app.register_blueprint(fixed_income_factsheet_bp)

    from .fixed_income_issuers import fixed_income_issuers_bp
    with app.app_context():
        app.register_blueprint(fixed_income_issuers_bp)

    from .fixed_income_dashboard import fixed_income_dashboard_bp
    with app.app_context():
        app.register_blueprint(fixed_income_dashboard_bp)


    @app.route('/')
    def heartbeat():
        return "app is alive."
    
    from .final_api import final_api_bp
    with app.app_context():
        app.register_blueprint(final_api_bp)

    from .data_api import data_api_bp
    with app.app_context():
        app.register_blueprint(data_api_bp)
    
    from fin_resource.flask_api import admin_bp
    with app.app_context():
        app.register_blueprint(admin_bp)

    from .login import login_bp
    with app.app_context():
        app.register_blueprint(login_bp)

    from .dirty_api import dirty_api_bp
    with app.app_context():
        app.register_blueprint(dirty_api_bp)

    from .factsheet import factsheet_bp
    with app.app_context():
        app.register_blueprint(factsheet_bp)

    from .upload_service import upload_bp
    with app.app_context():
        app.register_blueprint(upload_bp)

    from .portfolio_model import portfolio_model_bp
    with app.app_context():
        app.register_blueprint(portfolio_model_bp)

    from .content_model import content_model_bp
    with app.app_context():
        app.register_blueprint(content_model_bp)

    

    return app
