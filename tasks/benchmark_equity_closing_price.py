# NOT USED AS OF NOW.

import csv
from datetime import datetime as dt
import os

from utils import *
from utils.finalyca_store import *
from fin_models import *
from bizlogic.importer_helper import save_nav, get_rel_path, write_csv

def import_benchmark_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))
    header = [ "SchemeCode", "Date", "NAV", "Remarks"]
    items = list()
    TODAY = dt.today()
    NAV_TYPE = 'I'

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            scheme_code = row["SchemeCode"]
            pf_date = row["Date"]
            nav = row["NAV"]
            nav_date = dt.strptime(pf_date, '%d-%b-%y')
            remark = None

            sql_bench = db_session.query(BenchmarkIndices).filter(BenchmarkIndices.BenchmarkIndices_Code == scheme_code, BenchmarkIndices.Is_Deleted != 1).all()
            if not sql_bench:
                remark = "Benchmark Index not available in system."

            if nav_date > TODAY:
                    remark = "NAV cannot be uploaded for Future date."        

            if remark == None:
                for sql_ben in sql_bench:
                    remark = save_nav(db_session, sql_ben.BenchmarkIndices_Id, NAV_TYPE, nav_date, nav, user_id, TODAY)
                
            item = row.copy()
            item["Remarks"] = remark
            items.append(item)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/Benchmarks NAV_11 (2).csv"
    READ_PATH = "read/Benchmarks NAV_11 (2).csv"
    import_benchmark_file(FILE_PATH, READ_PATH, USER_ID)