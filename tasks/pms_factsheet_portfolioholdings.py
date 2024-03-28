import csv
from datetime import datetime as dt
from sqlalchemy import and_, desc, func
import os
from bizlogic.importer_helper import save_nav, get_rel_path, write_csv, get_schemecode_factsheet

from utils import *
from utils.finalyca_store import *
from fin_models import *

def import_pms_factsheet_portfolioholdings_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)   
     
    db_session = get_finalyca_scoped_session(is_production_config(config))

    header = [ "AMCCode", "SchemeCode", "Date", "Attribute Type","Attribute Text", "Attribute Value", "Attribute Sub Text", "Remarks"]
    items = list()
    PRODUCT_ID = 2
    TODAY = dt.today()
    fund_dict = dict()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            amccode = row["AMCCode"]
            schemecode = get_schemecode_factsheet(row["SchemeCode"], 4)
            date = row["Date"]
            portfoliodate = dt.strptime(date, '%d-%m-%Y')
            holdingtype = row["Holding Type"]
            isincode = row["ISIN Code"]
            securityname = row["Security Name"]
            currentweight = row["Current Weight"]
            difference = row["Difference"]
            Remarks = None

            sql_planid = db_session.query(Plans.Plan_Id).filter(Plans.Plan_Code == schemecode).filter(Plans.Is_Deleted != 1).order_by(desc(Plans.Plan_Id)).limit(1).scalar()

            update_values = {
                PortfolioHoldings.Is_Deleted : 1,
                PortfolioHoldings.Updated_By : user_id,
                PortfolioHoldings.Updated_Date : TODAY
            }

            sql_up = db_session.query(PortfolioHoldings).filter(PortfolioHoldings.Plan_Id == sql_planid.Plan_Id).filter(PortfolioHoldings.Portfolio_Date == portfoliodate)
            db_session.commit()

            sql_PortfolioHoldings = PortfolioHoldings()
            sql_PortfolioHoldings.Plan_Id = sql_planid.Plan_Id 
            sql_PortfolioHoldings.Portfolio_Date = portfoliodate
            sql_PortfolioHoldings.Holding_Type = holdingtype
            sql_PortfolioHoldings.ISIN_Code = isincode
            sql_PortfolioHoldings.Security_Name = securityname
            sql_PortfolioHoldings.Current_Weight = currentweight
            sql_PortfolioHoldings.Difference_Weight = difference
            sql_PortfolioHoldings.Is_Deleted = 0
            sql_PortfolioHoldings.Created_By = user_id
            sql_PortfolioHoldings.Created_Date = TODAY

            db_session.add(sql_PortfolioHoldings)
            db_session.commit()

            update_factsheet = {
                FactSheet.Portfolio_Date : portfoliodate,
                FactSheet.Updated_Date : TODAY,
                FactSheet.Updated_By : user_id
            }

            sql_fs = db_session.query(FactSheet).filter(FactSheet.Plan_Id == sql_planid).filter(FactSheet.TransactionDate == portfoliodate).filter(FactSheet.Is_Deleted != 1).update(update_factsheet)
            db_session.commit()
            remark = "Uploaded Successfully."        

            item = row.copy()
            item["Remarks"] = Remarks
            items.append(item)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/PMS - Factsheet - Portfolio Analysis_14113.csv"
    READ_PATH = "read/PMS - Factsheet - Portfolio Analysis_14113"
    import_pms_factsheet_portfolioholdings_file(FILE_PATH, READ_PATH, USER_ID)