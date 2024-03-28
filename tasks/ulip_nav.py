import csv
from datetime import datetime as dt
import pandas as pd
import os
from bizlogic.importer_helper import save_nav, get_rel_path, write_csv, get_plan_codefornav

from utils import *
from utils.finalyca_store import *
from fin_models import *
from bizlogic.common_helper import schedule_email_activity

def import_ulip_nav_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)

    db_session = get_finalyca_scoped_session(is_production_config(config))
    header = [ "SchemeCode", "Date", "NAV","SFINCode", "FundID", "NAVSCHEME_NAME", "Flag", "Remarks"]
    exceptions_data = []

    items = list()
    PRODUCT_ID = 2
    NAV_TYPE = 'P'
    TODAY = dt.today()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f, delimiter="|")
        for row in csvreader:
            try:
                # Not actually scheme code, here CMOTS is sharing their NAV Code. 
                # To understand the structure one needs to know how ULIP Schemes are sold and managed
                scheme_code = row["SchemeCode"]
                fund_id = row["FundID"]
                pf_date = row["Date"].replace("/","-")
                nav = row["NAV"]
                nav_date = dt.strptime(pf_date, '%d-%m-%Y')
                remark = None

                if NAV_TYPE == "P" or NAV_TYPE == "A":
                    plan_code= get_plan_codefornav(fund_id, PRODUCT_ID)

                    sql_plan = db_session.query(Plans.Plan_Id,
                                                MFSecurity.MF_Security_OpenDate)\
                                        .select_from(Plans)\
                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id==Plans.Plan_Id)\
                                        .join(MFSecurity, MFSecurity.MF_Security_Id==Plans.MF_Security_Id)\
                                        .filter(Plans.Plan_Code == plan_code, PlanProductMapping.Product_Id == PRODUCT_ID)\
                                        .filter(PlanProductMapping.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).all()

                    if not sql_plan:
                        remark = "Strategy not available in system."

                    if nav_date == None:
                        remark = "NAV cannot be uploaded without any date."
                        raise Exception("NAV cannot be uploaded without any date.")

                    if nav_date > TODAY:
                        remark = "NAV cannot be uploaded for Future date."

                    if remark == None:
                        for sql_plan1 in sql_plan:
                            if sql_plan1.MF_Security_OpenDate:
                                if sql_plan1.MF_Security_OpenDate > nav_date:
                                    remark = "NAV cannot be uploaded for previous dates to funds inception date."

                            if remark == None:
                                remark = save_nav(db_session, sql_plan1.Plan_Id, NAV_TYPE, nav_date, nav, user_id, TODAY)
                else:
                    # TODO: This else part does not make any sense. Do we even need this code? Can we clean this up?
                    sql_plan = db_session.query(BenchmarkIndices).filter(BenchmarkIndices.BenchmarkIndices_Code == scheme_code, BenchmarkIndices.Is_Deleted != 1).all()
                    if not sql_plan:
                        remark = "Benchmark Index not available in system."

                    if nav_date > TODAY:
                        remark = "NAV cannot be uploaded for Future date."

                    if remark == None:
                        for sql_plan1 in sql_plan:
                            remark = save_nav(db_session, sql_plan1.BenchmarkIndices_Id, NAV_TYPE, nav_date, nav, user_id, TODAY )

                print(remark)
            except Exception as ex:
                ex_record = {}
                ex_record['CMOTS_SchemeCode'] = row["SchemeCode"]
                ex_record['Remarks'] = remark
                ex_record['Exception'] = str(ex)
                exceptions_data.append(ex_record)
                print('Exception Recorded!!!')
                
                db_session.rollback()
                db_session.flush()
                continue


            item = row.copy()
            item["Remarks"] = remark
            items.append(item)

        if exceptions_data:
            exception_file_path = get_rel_path(output_file_path, __file__).lower().replace('navupdateinsurance', 'navupdateinsurance_exception')
            exception_file_path = exception_file_path.replace('/',r'\\')
            # check whether the specified path exists or not
            read_folder = '\\'.join(get_rel_path(output_file_path, __file__).split('/')[:-1])
            isExist = os.path.exists(read_folder)
            if not isExist:
                # create a new directory because it does not exist
                try:
                    os.makedirs(read_folder)
                except Exception as ex:
                    pass # File could not be created. Moving on....

            df_exception = pd.DataFrame(exceptions_data)
            df_exception.to_csv(exception_file_path)

            attachements = list()
            attachements.append(exception_file_path)
            schedule_email_activity(db_session, 'devteam@finalyca.com', '', '', F"ULIP - NAV - Exception file{TODAY.strftime('%Y-%b-%d')}", F"ILIP - NAV - Exception file{TODAY.strftime('%Y-%b-%d')}", attachements)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/cmots/29032023_ulip/NavUpdateInsurance.csv"
    READ_PATH = "read/cmots/29032023_ulip/NavUpdateInsurance.csv"
    import_ulip_nav_file(FILE_PATH, READ_PATH, USER_ID)