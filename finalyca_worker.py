from datetime import datetime as dt
import logging
import os
from utils.utils import get_config
from utils.finalyca_store import is_production_config, get_finalyca_scoped_session
from fin_models.servicemanager_models import *
import tasks.mf_holdings, tasks.mf_factsheet, tasks.pms_factsheet, tasks.ulip_factsheet, tasks.mf_fundmanagers, tasks.ulip_fundmanagers,\
       tasks.pms_fundmanagers, tasks.mf_fundmaster, tasks.mf_nav, tasks.pms_nav, tasks.ulip_nav, tasks.benchmark_equity_closing_price,\
       tasks.pms_fundmaster, tasks.ulip_fund_master, tasks.pms_holdings, tasks.ulip_holdings, tasks.equityscriptmaster,\
       tasks.pms_factsheet_portfolioanalysis, tasks.pms_factsheet_portfoliosectors, tasks.sectors_subsectors,\
       tasks.pms_factsheet_portfolioholdings, tasks.aif_factsheet, tasks.aif_nav, tasks.aif_holdings, tasks.aif_fundmaster, tasks.pms_aif_holdings,\
       tasks.bilav_debt_importer, tasks.bilav_debt_price_importer


from tasks.worker_helper import process_job_requests, process_delivery_requests

def get_import_template_lookup():
    Template_Lookup = dict()
    # 1: MF - Underlying Holdings
    Template_Lookup[1] = tasks.mf_holdings.import_mf_holding_file
    # 2: MF - Factsheet
    Template_Lookup[2] = tasks.mf_factsheet.import_mf_factsheet_file
    # 3: PMS - Factsheet
    Template_Lookup[3] = tasks.pms_factsheet.import_pms_factsheet_file
    # 4: MF - Fund Manager
    Template_Lookup[4] = tasks.mf_fundmanagers.import_mf_fundmanagers_file
    # 5: PMS - Fund Manager
    Template_Lookup[5] = tasks.pms_fundmanagers.import_pms_fundmanagers_file
    # 6: ULIP - Fund Manager
    Template_Lookup[6] = tasks.ulip_fundmanagers.import_ulip_fundmanagers_file
    # 7: MF - Fund Master
    Template_Lookup[7] = tasks.mf_fundmaster.import_mf_fundmaster_file
    # 8: MF - NAV
    Template_Lookup[8] = tasks.mf_nav.import_mf_nav_file
    # 9: PMS - NAV
    Template_Lookup[9] = tasks.pms_nav.import_pms_nav_file
    # 10: ULIP - NAV
    Template_Lookup[10] = tasks.ulip_nav.import_ulip_nav_file
    # 11: Benchmark And Equity Closing Prices
    Template_Lookup[11] = tasks.benchmark_equity_closing_price.import_benchmark_file
    # 12: PMS - Fund Master
    Template_Lookup[12] = tasks.pms_fundmaster.import_pms_fundmaster_file
    # 13: ULIP - Fund Master
    Template_Lookup[13] = tasks.ulip_fund_master.import_ulip_fund_master_file
    # 14: ULIP - Factsheet
    Template_Lookup[14] = tasks.ulip_factsheet.import_ulip_factsheet_file
    # 15: PMS - Underlying Holdings
    Template_Lookup[15] = tasks.pms_holdings.import_pms_holdings_file
    # 16: ULIP - Underlying Holdings
    Template_Lookup[16] = tasks.ulip_holdings.import_ulip_holdings_file
    # 17: AIF - Factsheet
    Template_Lookup[17] = tasks.aif_factsheet.import_aif_factsheet_file
    # 18: AIF - Underlying Holdings
    Template_Lookup[18] = tasks.aif_holdings.import_aif_holdings_file
    # 19: AIF - NAV
    Template_Lookup[19] = tasks.aif_nav.import_aif_nav_file
    # 20: Equity Script Master - VR
    Template_Lookup[20] = tasks.equityscriptmaster.import_equityscriptmaster_file
    # 21: Equity Master - Vidal
    Template_Lookup[21] = None
    # 22: PMS - Factsheet - Portfolio Analysis
    Template_Lookup[22] = tasks.pms_factsheet_portfolioanalysis.import_factsheet_portfolioanalysis_file
    # 23: PMS - Factsheet - Portfolio Holdings
    Template_Lookup[23] = tasks.pms_factsheet_portfolioholdings.import_pms_factsheet_portfolioholdings_file
    # 24: PMS - Factsheet - Portfolio Sectors
    Template_Lookup[24] = tasks.pms_factsheet_portfoliosectors.import_pms_factsheet_portfoliosectors_file
    # 25: Sectors_Subsectors
    Template_Lookup[25] = tasks.sectors_subsectors.import_sectors_subsectors_file
    # 28: AIF - Fund Master
    Template_Lookup[28] = tasks.aif_fundmaster.import_aif_fundmaster_file
    # 29: PMS/AIF - Common Holdings File
    Template_Lookup[29] = tasks.pms_aif_holdings.import_holdings_file
    # 30: Fixed Income - Master
    Template_Lookup[30] = tasks.bilav_debt_importer.import_bilav_debt_data
    # 31: Fixed Income - Price Master
    Template_Lookup[31] = tasks.bilav_debt_price_importer.import_bilav_price_file


    return Template_Lookup


def process_upload_requests(db_session, template_lookup, config):
    # Data Upload Status
    # 0 -> pending
    # 1 -> success
    # 2 -> Fail
    # 3 -> Executing
    # Check for all requests that have status 0
    sql_requests = db_session.query(UploadRequest)\
                             .join(UploadTemplates, UploadRequest.UploadTemplates_Id == UploadTemplates.UploadTemplates_Id)\
                             .filter(UploadTemplates.Is_Deleted != 1, UploadRequest.Is_Deleted != 1, UploadTemplates.Enabled_Python == 1)\
                             .filter(UploadRequest.Status == 0).all()

    # for each sub services, check the pending tasks and execute them if required.
    for sql_request in sql_requests:
        logging.warning(F"Upload request for template id - {sql_request.UploadTemplates_Id} start" )
        # Add info
        sql_request.Pick_Time = dt.now()
        sql_request.Status = 3
        sql_request.Status_Message = "Executing"
        db_session.commit()

        user_id = sql_request.Created_By
        # TODO: make a better way to find the base file path
        dir_prefix = os.path.join(config["DOC_ROOT_PATH"], config["UPLOAD_DIR"])
        incoming_file_path = os.path.join(dir_prefix, sql_request.File_Name)
        sl = os.path.splitext(sql_request.File_Name)
        read_file_name = F"{sl[0]}_R{sl[1]}"
        read_file_path = os.path.join(dir_prefix, read_file_name)

        # process the request
        try:
            func_exec = template_lookup[sql_request.UploadTemplates_Id]
            func_exec(incoming_file_path, read_file_path, user_id)

            sql_request.Status = 1
            sql_request.Status_Message = "Success"
            sql_request.Completion_Time = dt.now()
            db_session.commit()

        except Exception as ex:
            logging.warning(F"Error: Upload request for template id - {sql_request.UploadTemplates_Id} Error : {str(ex)}" )
            sql_request.Status = 2
            sql_request.Status_Message = F"Fail: {str(ex)}"
            sql_request.Completion_Time = dt.now()
            db_session.commit()  

logging.warning("nssm runner test is alive.")

while True:
    
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "config/local.config.yaml"
    )
    config = get_config(config_file_path)

    # logging.warning(F"config path: {config_file_path}")

    # db_session = get_finalyca_scoped_session(is_production_config(config))
    db_session = get_finalyca_scoped_session(True)

    templates = get_import_template_lookup()
    process_upload_requests(db_session, templates, config)

    process_delivery_requests(db_session)

    process_job_requests(db_session)

    db_session.remove()
