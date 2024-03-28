import csv
from datetime import datetime as dt
from sqlalchemy import and_, desc, func
import os
from bizlogic.importer_helper import save_nav, get_rel_path, write_csv, get_plan_codefornav

from utils import *
from utils.finalyca_store import *
from fin_models import *
from bizlogic.fill_nav_gap import *

def import_pms_nav_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    header = ["SchemeCode", "Date", "NAV", "Remarks"]
    list_plan_id = list()
    try:
        items = list()

        PRODUCT_ID = 4
        NAV_TYPE = 'P'
        TODAY = dt.today()

        with open(get_rel_path(input_file_path, __file__), 'r') as f:
            csvreader = csv.DictReader(f)
            for row in csvreader:
                scheme_code = row["SchemeCode"]
                pf_date = row["Date"].replace("/","-")
                nav = row["NAV"]
                nav_date =  dt.strptime(pf_date, '%d-%m-%Y')
                remark = None

                if NAV_TYPE == "P" or NAV_TYPE == "A":
                    plan_code= get_plan_codefornav(scheme_code, PRODUCT_ID)

                    sql_plan = db_session.query(Plans.Plan_Id, MFSecurity.MF_Security_OpenDate).select_from(Plans).join(PlanProductMapping, PlanProductMapping.Plan_Id==Plans.Plan_Id).join(MFSecurity, MFSecurity.MF_Security_Id==Plans.MF_Security_Id).filter(Plans.Plan_Code == plan_code).filter(PlanProductMapping.Product_Id == PRODUCT_ID).filter(PlanProductMapping.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).all()
                    if not sql_plan:
                        remark = "Strategy not available in system."

                    if nav_date > TODAY:
                        remark = "NAV cannot be uploaded for Future date."
                    
                    if remark == None:
                        for sql_plan1 in sql_plan:
                            if sql_plan1.MF_Security_OpenDate > nav_date:
                                remark = "NAV cannot be uploaded for previous dates to funds inception date."

                            if remark == None:
                                if sql_plan1.Plan_Id not in list_plan_id:
                                    list_plan_id.append(sql_plan1.Plan_Id)  
                                
                                remark = save_nav(db_session, sql_plan1.Plan_Id, NAV_TYPE, nav_date, nav, user_id, TODAY)                    
                else:
                    sql_ben = db_session.query(BenchmarkIndices).filter(BenchmarkIndices.BenchmarkIndices_Code == scheme_code, BenchmarkIndices.Is_Deleted != 1).order_by(desc(BenchmarkIndices.BenchmarkIndices_Id)).all()
                    if not sql_ben:
                        remark = "Benchmark Index not available in system."

                    if nav_date > TODAY:
                        remark = "NAV cannot be uploaded for Future date."

                    if remark == None:
                        for sql_ben1 in sql_ben:
                            remark = save_nav(db_session, sql_ben1.BenchmarkIndices_Id, NAV_TYPE, nav_date, nav, user_id, TODAY )                        

                item = row.copy()
                item["Remarks"] = remark
                items.append(item)
            
            #fill nav gaps
            if list_plan_id:
                for plan_id in list_plan_id:
                    fill_missing_nav(db_session, [], plan_id)

    finally:   
        write_csv(output_file_path, header, items, __file__)


if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/date miss NAV_1 (3).csv"
    READ_PATH = "read/date miss NAV_1 (3).csv"
    import_pms_nav_file(FILE_PATH, READ_PATH, USER_ID)