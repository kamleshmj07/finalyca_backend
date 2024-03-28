import csv
from datetime import datetime as dt
from sqlalchemy import and_, desc, func
import os
from bizlogic.importer_helper import save_nav, get_rel_path, write_csv, get_schemecode_factsheet
from bizlogic.fund_portfolio_analysis import save_portfolio_sector

from utils import *
from utils.finalyca_store import *
from fin_models import *

def import_pms_factsheet_portfoliosectors_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    header = [ "AMCCode", "SchemeCode", "Date", "Sector Code","Sector Name", "Sub Sector Name", "PercentageToAUM", "Remarks"]
    items = list()
    TODAY = dt.today()

    fund_dict = dict()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            amccode = row["AMCCode"]
            schemecode = get_schemecode_factsheet(row["SchemeCode"],4)
            sectorcode = row["Sector Code"]
            sectorname = row["Sector Name"]
            subsectorname = row["Sub Sector Name"]
            percentagetoaum = row["PercentageToAUM"]        
            date = row["Date"]               
            remark = None
            portfoliodate = dt.strptime(date, '%d-%m-%Y')
            
            sql_planid = db_session.query(Plans.Plan_Id)\
                                    .filter(Plans.Plan_Code == schemecode)\
                                    .filter(Plans.Is_Deleted != 1)\
                                    .order_by(desc(Plans.Plan_Id))\
                                    .limit(1).scalar()

            if schemecode not in fund_dict:
                fund_dict[schemecode] = list()
            
            if portfoliodate not in fund_dict[schemecode]:
                fund_dict[schemecode].append(portfoliodate)
                update_values = {
                    PortfolioSectors.Is_Deleted : 1,
                    PortfolioSectors.Updated_By : user_id,
                    PortfolioSectors.Updated_Date : TODAY
                }

                sql_del = db_session.query(PortfolioSectors)\
                                    .filter(PortfolioSectors.Plan_Id == sql_planid)\
                                    .filter(PortfolioSectors.Portfolio_Date == portfoliodate)\
                                    .filter(PortfolioSectors.Is_Deleted != 1)\
                                    .update(update_values)               
                db_session.commit()
            
            if schemecode in fund_dict:
                if portfoliodate in fund_dict[schemecode]:        
                    save_portfolio_sector(db_session, sql_planid, portfoliodate, sectorcode, sectorname, subsectorname, percentagetoaum, 'L', user_id)

                    update_factsheet = {
                        FactSheet.Portfolio_Date : portfoliodate,
                        FactSheet.Updated_Date : TODAY,
                        FactSheet.Updated_By : user_id
                    }

                    sql_fs = db_session.query(FactSheet)\
                                        .filter(FactSheet.Plan_Id == sql_planid)\
                                        .filter(FactSheet.TransactionDate == portfoliodate)\
                                        .filter(FactSheet.Is_Deleted != 1).update(update_factsheet)
                    db_session.commit()
                    remark = "Uploaded Successfully."
        
            item = row.copy()
            item["Remarks"] = remark
            items.append(item)

        
        for fund_dic in fund_dict:
            dates = fund_dict[fund_dic]
            for date in dates:
                sql_planid = db_session.query(Plans.Plan_Id)\
                                        .filter(Plans.Plan_Code == schemecode)\
                                        .filter(Plans.Is_Deleted != 1)\
                                        .order_by(desc(Plans.Plan_Id))\
                                        .limit(1).scalar()

                cash_perc = db_session.query(FactSheet.Cash).filter(FactSheet.Plan_Id == sql_planid)\
                                                        .filter(FactSheet.TransactionDate == date)\
                                                        .filter(FactSheet.Is_Deleted != 1).scalar()
                
                save_portfolio_sector(db_session, sql_planid, date, 'Cash_01', 'Cash', '', cash_perc if cash_perc else 0, 'L', user_id)
            

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/30281e32-80f7-4a80-b91d-c4a81785e402.csv"
    READ_PATH = "read/30281e32-80f7-4a80-b91d-c4a81785e402.csv"
    import_pms_factsheet_portfoliosectors_file(FILE_PATH, READ_PATH, USER_ID)

