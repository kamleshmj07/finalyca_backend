import csv
from datetime import datetime as dt
import os
from sqlalchemy import desc
from fin_models.masters_models import BenchmarkIndices, HoldingSecurity
from utils.finalyca_store import get_finalyca_scoped_session
from bizlogic.cmots_helper import write_csv_v2, cmots_save_bm_values, cmots_delete_bm_values, cmots_save_security_values, cmots_delete_security_values

def import_cmotsupload_dlyprice_file(file_path, read_path, user_id):
    CMOTS_SEP = "|"
    db_session = get_finalyca_scoped_session(True)
    TODAY = dt.today()

    items = list()
    with open(file_path, 'r') as f:
        csvreader = csv.reader(f,delimiter = CMOTS_SEP)    
        for row in csvreader:     
            SC_CODE = row[0]
            DATE =  dt.strptime(row[1], '%d/%m/%Y')
            ST_EXCHNG = row[2]
            CO_CODE = row[3]
            HIGH = row[4]
            LOW = row[5]
            OPEN = row[6]
            CLOSE = row[7]
            TDCLOINDI = row[8]
            VOLUME = row[9]
            NO_TRADES = row[10]        
            NET_TURNOV = row[11]        
            FLAG = row[12]        
            remark = None

            sql_bm = db_session.query(BenchmarkIndices).filter(BenchmarkIndices.Co_Code == CO_CODE).one_or_none()
            if sql_bm:
                if FLAG == "D":
                    remark = cmots_delete_bm_values(db_session, sql_bm.BenchmarkIndices_Id, DATE,  user_id, TODAY)
                else:                    
                    remark = cmots_save_bm_values(db_session, sql_bm.BenchmarkIndices_Id, DATE, CLOSE, user_id, TODAY)
            else:
                # TODO: Ideally there should not be any need to check in Holding Security table.
                sql_security = db_session.query(HoldingSecurity).filter(HoldingSecurity.Co_Code == CO_CODE).filter(HoldingSecurity.Is_Deleted != 1).order_by(desc(HoldingSecurity.HoldingSecurity_Id)).first()

                if sql_security:
                    if FLAG == "D":
                        remark = cmots_delete_security_values(db_session, DATE, ST_EXCHNG, CO_CODE, user_id, TODAY)
                    else:
                        remark = cmots_save_security_values(db_session, SC_CODE, sql_security.ISIN_Code, DATE, ST_EXCHNG, CO_CODE, HIGH, LOW, OPEN, CLOSE, TDCLOINDI, VOLUME, NO_TRADES, NET_TURNOV, user_id, TODAY)
                else:
                    remark = 'Warning: CO Code does not exist in system.'

            item = row.copy()
            item.append(remark)
            items.append(item)
        
    # write_csv(read_path, None, items, __file__, True)
    write_csv_v2(read_path, None, items, CMOTS_SEP)

if __name__ == '__main__':
    USER_ID = 1
    input_file = "samples\\cmots\\dlyprice_12-09-2022.csv"
    read_file = "read\\cmots\\dlyprice_12-09-2022.csv"

    FILE_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), input_file )
    READ_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), read_file )

    import_cmotsupload_dlyprice_file(FILE_PATH, READ_PATH, '', USER_ID)
