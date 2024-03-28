import os
import csv
from datetime import datetime as dt
from bizlogic.cmots_helper import write_csv_v2, cmots_save_index_weight, cmots_delete_index_weight
from utils.finalyca_store import get_finalyca_scoped_session

def import_cmotsupload_nsebseweight(filepath, readpath, index_type, user_id):
    CMOTS_SEP = "|"
    db_session = get_finalyca_scoped_session(True)
    TODAY = dt.today()
    items = list()
    
    with open(filepath, 'r') as f:
        csvreader = csv.reader(f,delimiter = CMOTS_SEP)    
        for row in csvreader:        
            CO_CODE = row[0]
            WDATE = dt.strptime(row[1], '%d/%m/%Y')
            INDEXCODE = row[2]
            CLOSEPRICE = row[3]
            NOOFSHARES = row[4]
            FULLMCAP = row[5]
            FF_ADJFACTOR = row[6]
            FF_MCAP = row[7]
            WEIGHT_INDEX = row[8]
            FLAG = row[9]    
            remark = None

            if FLAG == "D":
                remark = cmots_delete_index_weight(db_session, CO_CODE, WDATE, INDEXCODE, user_id, TODAY)
            else:
                remark = cmots_save_index_weight(db_session, CO_CODE, WDATE, INDEXCODE, CLOSEPRICE, NOOFSHARES, FULLMCAP, FF_ADJFACTOR, FF_MCAP, WEIGHT_INDEX, index_type, user_id, TODAY)

            item = row.copy()
            item.append(remark)
            items.append(item)
        
    write_csv_v2(readpath, [], items, CMOTS_SEP)

if __name__ == '__main__':
    USER_ID = 1
    input_file = "samples\\cmots\\nseweight_12-09-2022.csv"
    read_file = "read\\cmots\\nseweight_12-09-2022.csv"
    INDEX_TYPE = "NSE" #this will be BSE in case of CMOTS Upload - bseweight

    FILE_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), input_file )
    READ_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), read_file )

    import_cmotsupload_nsebseweight(FILE_PATH, READ_PATH, INDEX_TYPE, USER_ID)

    input_file = "samples\\cmots\\bseweight_12-09-2022.csv"
    read_file = "read\\cmots\\bseweight_12-09-2022.csv"
    INDEX_TYPE = "BSE"

    FILE_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), input_file )
    READ_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), read_file )

    import_cmotsupload_nsebseweight(FILE_PATH, READ_PATH, INDEX_TYPE, USER_ID)

