import os
import csv
from datetime import datetime as dt
from utils.finalyca_store import get_finalyca_scoped_session
from bizlogic.cmots_helper import write_csv_v2, cmots_save_bm, cmots_save_security

def import_cmots_cmasmaster(filepath, readpath, user_id):
    CMOTS_SEP = "|"
    db_session = get_finalyca_scoped_session(True)
    TODAY = dt.today()
    
    items = list()
    with open(filepath, 'r') as f:
        csvreader = csv.reader(f,delimiter = CMOTS_SEP)    
        for row in csvreader:        
            co_code = row[0]
            sc_code = row[1]
            symbol = row[2]
            small_name = row[3]
            long_name = row[4]
            isin = row[5]
            sect_name = row[6]
            sc_group = row[7]
            remark = None
            
            if sect_name == "Stock Exchanges":
                remark = cmots_save_bm(db_session, co_code, small_name, long_name, sc_code, symbol, sc_group, user_id, TODAY)
            else:
                remark = cmots_save_security(db_session, isin, co_code, small_name, long_name, sc_code, symbol, sc_group, sect_name, user_id, TODAY)
             
            item = row.copy()
            item.append(remark)
            items.append(item)
        
    write_csv_v2(readpath, None, items, CMOTS_SEP)

if __name__ == "__main__":
    USER_ID = 1
    input_file = "samples\\cmots\\cmas_13-09-2022-03-00-19.csv"
    read_file = "read\\cmots\\cmas_13-09-2022-03-00-19.csv"

    FILE_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), input_file )
    READ_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), read_file )

    import_cmots_cmasmaster(FILE_PATH, READ_PATH, USER_ID)
