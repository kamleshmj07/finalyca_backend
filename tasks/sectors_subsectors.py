import csv
from datetime import datetime as dt
from sqlalchemy import and_, func
import os
from bizlogic.importer_helper import save_planproductmapping, get_rel_path, write_csv

from utils import *
from utils.finalyca_store import *
from fin_models import *

def import_sectors_subsectors_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))
    header = [ "CAPITALINE_CODE", "Company_Long_Name", "Sourced", "ISIN","NSE_Symbol", "Macro_Economic_Sector", "Sector", "Industry", "Basic_Industry", "Remarks"]
    items = list()
    PRODUCT_ID = 1
    TODAY = dt.today()

    fund_dict = dict()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            capitaline_code = row["CAPITALINE_CODE"]
            companylogoname = row["Company_Long_Name"]
            sourced = row["Sourced"]
            isin = row["ISIN"]
            nse_symbol = row["NSE_Symbol"]        
            macro_exonomic_sector = row["Macro_Economic_Sector"]
            sector = row["Sector"]
            industry = row["Industry"]
            basic_industry = row["Basic_Industry"]
            Remarks = None
            
            sql_HoldingSecurity_sectorids = db_session.query(HoldingSecurity).filter(HoldingSecurity.ISIN_Code == isin).filter(HoldingSecurity.Is_Deleted != 1).all()
            
            if not sql_HoldingSecurity_sectorids:
                Remarks = "This ISIN Code is not available."

            if Remarks == None:
                for sql_HoldingSecurity_sectorid in sql_HoldingSecurity_sectorids:
                    sql_HoldingSecurity_sectorid.Sub_SectorName = industry
                    sql_HoldingSecurity_sectorid.Updated_By = user_id
                    sql_HoldingSecurity_sectorid.Updated_Date = TODAY                

                    db_session.commit()
                            
                    update_sector = {
                        Sector.Sector_Name : sector,
                        Sector.Updated_Date : TODAY,
                        Sector.Updated_By : user_id
                    }

                    sql_sector = db_session.query(Sector).filter(Sector.Sector_Id == sql_HoldingSecurity_sectorid.Sector_Id).update(update_sector)
                db_session.commit()
                Remarks = "Uploaded Successfully."
        
            item = row.copy()
            item["Remarks"] = Remarks
            items.append(item)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/Sectors_Subsectors_12434.csv"
    READ_PATH = "read/Sectors_Subsectors_12434.csv"
    import_sectors_subsectors_file(FILE_PATH, READ_PATH, USER_ID)