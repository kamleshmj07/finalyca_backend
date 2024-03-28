import csv
from datetime import datetime as dt
from sqlalchemy import and_, func
import os
from bizlogic.importer_helper import get_rel_path, write_csv, fundmanager_upload

from utils import *
from utils.finalyca_store import *
from fin_models import *

def import_ulip_fundmanagers_file(input_file_path, output_file_path, user_id):
    header = [ "FundID", "Person Code", "Designation", "Fund Manager","Experience", "Qualification", "Fund", "DateFrom", "DateTo", "Remarks"]
    items = list()

    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    PRODUCT_ID = 2
    NAV_TYPE = 'P'
    TODAY = dt.today()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            fundid = "INS_" + str(row["FundID"])
            fundmanagercode = row["Person Code"]
            fundname = row["Fund"]
            fundmanagername = row["Fund Manager"]
            experience = row["Experience"]
            qualification = row["Qualification"]
            designation = row["Designation"]
            dt_from = row["DateFrom"]
            datefrom = None
            dateto = None
            remark = None
            if dt_from != '':
                datefrom = dt.strptime(dt_from, '%d-%m-%Y')
            
            dt_to = row["DateTo"]
            if dt_to != '':
                dateto = dt.strptime(dt_to, '%d-%m-%Y')            

            remark = fundmanager_upload(db_session, fundid, fundmanagercode, fundname, fundmanagername, experience, qualification, designation, datefrom, dateto)
            
            item = row.copy()
            item["Remarks"] = remark
            items.append(item)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/PMS - Fund Manager_11683.csv"
    READ_PATH = "read/PMS - Fund Manager_11683"
    import_ulip_fundmanagers_file(FILE_PATH, READ_PATH, USER_ID)