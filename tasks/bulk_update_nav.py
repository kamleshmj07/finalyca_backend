"""
TODO: 
    # Update the inputs before executing this file.

"""

import os
import pandas as pd
from datetime import datetime as dt
from sqlalchemy import text

from utils.finalyca_store import *
from mf_nav import import_mf_nav_file
from fin_models.transaction_models import NAV


def bulk_update_nav(PRODUCT_ID, NAV_TYPE, USER_ID, SAMPLE_PATH, READ_PATH):

    # Read config
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)

    # Get db session and/or db_engine
    db_session = get_finalyca_scoped_session(is_production_config(config))
    engine = get_unsafe_db_engine(config)

    chunk_size = 100000  # define the chunk size the file needs to be broken down.
    all_file_names = []

    # Read the file and break down to smaller chunks
    with open(SAMPLE_PATH, "r") as f:
        count = 0
        header = f.readline()
        lines = []
        for line in f:
            count += 1
            lines.append(line)
            if count % chunk_size == 0:
                all_file_names.append(write_chunk(count // chunk_size, lines, header, SAMPLE_PATH.replace('.csv', '')))
                lines = []
        # write remainder
        if len(lines) > 0:
            all_file_names.append(write_chunk((count // chunk_size) + 1, lines, header, SAMPLE_PATH.replace('.csv', '')))


    # print('Names of all the files is - ', all_file_names)h


    # load the smaller files in the database one by one.
    for fp in all_file_names:
        FILE_PATH = fp
        READ_PATH_1 = READ_PATH + fp.split('\\\\')[-1]
        # print(READ_PATH_1)

        # delete the previous records to load fresh set of records
        df_nv = pd.read_csv(fp, delimiter='|', encoding= 'unicode_escape')

        df_nv["Plan_Code"] = df_nv["SchemeCode"].astype(str)
        df_nv["Plan_Code"] = df_nv["Plan_Code"] + '_01' if PRODUCT_ID == 1 else print("Set the Product Id as inputs")

        plan_codes = set(df_nv["Plan_Code"].to_list())

        with db_session.begin():
            try:
                for plan_code in plan_codes:
                    sql_plan = db_session.query(Plans.Plan_Id).select_from(Plans)\
                                                              .join(PlanProductMapping, PlanProductMapping.Plan_Id==Plans.Plan_Id)\
                                                              .join(MFSecurity, MFSecurity.MF_Security_Id==Plans.MF_Security_Id)\
                                                              .filter(Plans.Plan_Code == plan_code, PlanProductMapping.Product_Id == PRODUCT_ID)\
                                                              .filter(PlanProductMapping.Is_Deleted != 1, Plans.Is_Deleted != 1).first()
                    plan_id = sql_plan.Plan_Id

                    delete_query = text(F"UPDATE TRANSACTIONS.NAV SET IS_DELETED = 1, UPDATED_BY = 1, UPDATED_DATE=GETDATE() WHERE PLAN_ID={plan_id} AND IS_DELETED <> 1 AND NAV_TYPE='{NAV_TYPE}' AND NAV_DATE <= '2023-02-21'")
                    delete_query.is_update = True
                    engine.execute(delete_query)

                import_mf_nav_file(FILE_PATH, READ_PATH_1, USER_ID)
            except Exception as ex:
                print('Could not load nav for plan id - ', plan_id)


def write_chunk(part, lines, header, filepath):
    outfilepath = filepath + '_' + str(part) +'.csv'
    with open(outfilepath, 'w') as f_out:
        f_out.write(header)
        f_out.writelines(lines)
        print('File generated - ', outfilepath)
    
    return outfilepath



if __name__ == '__main__':
    # TODO Update the inputs before executing this file.
    PRODUCT_ID = 1
    NAV_TYPE = 'P'
    USER_ID = 1
    READ_PATH = r"C:\\dev\\backend\\tasks\\read\\cmots\\navfull\\"
    SAMPLE_PATH = r"C:\\dev\\backend\\tasks\\samples\\cmots\\navfull\\NavUpdate_Sample.csv"     # NAV file path
    bulk_update_nav(PRODUCT_ID, NAV_TYPE, USER_ID, SAMPLE_PATH, READ_PATH)

