import csv
from datetime import datetime as dt
from sqlalchemy import and_, desc, func
import os
from bizlogic.importer_helper import save_nav, get_rel_path, write_csv, get_schemecode_factsheet

from utils import *
from utils.finalyca_store import *
from fin_models import *

def import_factsheet_portfolioanalysis_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    header = [ "AMCCode", "SchemeCode", "Date", "Attribute Type","Attribute Text", "Attribute Value", "Attribute Sub Text", "Remarks"]
    items = list()

    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    db_session = get_data_store(config).db

    PRODUCT_ID = 1
    TODAY = dt.today()
    fund_dict = dict()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            amccode = row["AMCCode"]
            schemecode = get_schemecode_factsheet(row["SchemeCode"], 4)
            attributetype = row["Attribute Type"]
            attributetext = row["Attribute Text"]
            attributevalue = row["Attribute Value"]
            attributesubtext = row["Attribute Sub Text"]        
            date = row["Date"]               
            remark = None
            portfoliodate = dt.strptime(date, '%d-%m-%Y')
            
            sql_planid = db_session.query(Plans.Plan_Id).filter(Plans.Plan_Code == schemecode).filter(Plans.Is_Deleted != 1).order_by(desc(Plans.Plan_Id)).limit(1).scalar()

            if schemecode not in fund_dict:
                fund_dict[schemecode] = list()
            
            if portfoliodate not in fund_dict[schemecode]:
                fund_dict[schemecode].append(portfoliodate)
                update_values = {
                    PortfolioAnalysis.Is_Deleted : 1,
                    PortfolioAnalysis.Updated_By : user_id,
                    PortfolioAnalysis.Updated_Date : TODAY
                }

                sql_del = db_session.query(PortfolioAnalysis).filter(PortfolioAnalysis.Plan_Id == sql_planid).filter(PortfolioAnalysis.Portfolio_Date == portfoliodate).filter(PortfolioAnalysis.Is_Deleted != 1).update(update_values)
                db_session.commit()
            
            if schemecode in fund_dict:
                if portfoliodate in fund_dict[schemecode]:                
                    sql_PortfolioAnalysis = PortfolioAnalysis()
                    sql_PortfolioAnalysis.Plan_Id = sql_planid
                    sql_PortfolioAnalysis.Portfolio_Date = portfoliodate
                    sql_PortfolioAnalysis.Attribute_Type = attributetype
                    sql_PortfolioAnalysis.Attribute_Text = attributetext
                    sql_PortfolioAnalysis.Attribute_Value = attributevalue
                    sql_PortfolioAnalysis.Attribute_Sub_Text = attributesubtext
                    sql_PortfolioAnalysis.Is_Deleted = 0
                    sql_PortfolioAnalysis.Created_By = user_id
                    sql_PortfolioAnalysis.Created_Date = TODAY

                    db_session.add(sql_PortfolioAnalysis)
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
            item["Remarks"] = remark
            items.append(item)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/4 PMS - Factsheet - Portfolio Analysis.csv"
    READ_PATH = "read/4 PMS - Factsheet - Portfolio Analysis.csv"
    import_factsheet_portfolioanalysis_file(FILE_PATH, READ_PATH, USER_ID)