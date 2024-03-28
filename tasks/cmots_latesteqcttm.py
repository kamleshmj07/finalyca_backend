import os
import csv
from datetime import datetime as dt
from bizlogic.cmots_helper import write_csv_v2, cmots_save_fundamentals
from utils.finalyca_store import get_finalyca_scoped_session

def import_cmotsupload_latesteqcttm_file(filepath, readpath, user_id):
    CMOTS_SEP = "|"
    db_session = get_finalyca_scoped_session(True)
    items = list()
    
    TODAY = dt.today()
    with open(filepath, 'r') as f:
        csvreader = csv.reader(f,delimiter = CMOTS_SEP)    
        for row in csvreader:        
            CO_CODE = row[0]
            PriceDate = dt.strptime(row[1], '%d/%m/%Y')
            PE = row[2]
            EPS = row[3]
            DivYield = row[4]
            PBV = row[5]
            mcap = row[6]
            pe_cons = row[7]
            eps_cons = row[8]
            pbv_cons = row[9]
            remark = None

            remark = cmots_save_fundamentals(db_session, CO_CODE, PriceDate, PE, EPS, DivYield, PBV, mcap, user_id, TODAY, pe_cons, eps_cons, pbv_cons)         
            item = row.copy()
            item.append(remark)
            items.append(item)
        
    write_csv_v2(readpath, [], items, CMOTS_SEP)

if __name__ == '__main__':
    USER_ID = 1
    input_file = "samples\\cmots\\latesteqcttm_12-09-2022.csv"
    read_file = "read\\cmots\\latesteqcttm_12-09-2022.csv"

    FILE_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), input_file )
    READ_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), read_file )

    import_cmotsupload_latesteqcttm_file(FILE_PATH, READ_PATH, '', USER_ID)