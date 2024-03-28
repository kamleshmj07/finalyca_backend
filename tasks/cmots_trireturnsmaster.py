import os
import csv
from datetime import datetime as dt
from bizlogic.cmots_helper import write_csv_v2, cmots_save_tri_returns
from utils.finalyca_store import get_finalyca_scoped_session

def import_cmotsupload_trireturnsmaster_file(filepath, readpath, user_id):
    CMOTS_SEP = "|"

    db_session = get_finalyca_scoped_session(True)
    items = list()
    TODAY = dt.today()

    with open(filepath, 'r') as f:
        csvreader = csv.reader(f,delimiter = CMOTS_SEP)    
        for row in csvreader:        
            exchange = row[0]
            indexCode = row[1]
            indexName = row[2]
            indexDate = dt.strptime(row[3], '%d/%m/%Y')
            ret1W = row[4]
            ret1M = row[5]
            ret3M = row[6]
            ret6M = row[7]
            ret1year = row[8]
            ret3Year = row[9]
            baseindexcode = row[10]        
            remark = None

            remark = cmots_save_tri_returns(db_session, exchange, indexCode, indexName, indexDate, ret1W, ret1M, ret3M, ret6M, ret1year, ret3Year, baseindexcode, user_id, TODAY)

            item = row.copy()
            item.append(remark)
            items.append(item)
        
    write_csv_v2(readpath, [], items, CMOTS_SEP)

if __name__ == '__main__':
    USER_ID = 1
    input_file = "samples\\cmots\\trireturns_12-09-2022.csv"
    read_file = "read\\cmots\\trireturns_12-09-2022.csv"   

    FILE_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), input_file )
    READ_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), read_file )

    import_cmotsupload_trireturnsmaster_file(FILE_PATH, READ_PATH,'', USER_ID)
