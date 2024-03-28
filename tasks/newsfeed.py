import csv
from datetime import datetime as dt
from flask import current_app
from sqlalchemy import and_, func
import os
from bizlogic.importer_helper import save_nav, get_rel_path, write_csv

from utils import *
from utils.finalyca_store import *
from fin_models import *

def import_newsfeed_file(input_file_path, output_file_path, user_id):
    header = ["Date", "StoryHeadline", "StoryText", "StoryTextHTML", "AMC Code", "Fund Codes", "Remarks"]
    items = list()

    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    PRODUCT_ID = 1
    TODAY = dt.today()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            storytexthtml = row["StoryTextHTML"] 
            storytext = row["StoryText"]
            storyheadline = row["StoryHeadline"] 
            amccode = row["AMC Code"] 
            fundcodes = row["Fund Codes"] 
            date = dt.strptime(row["Date"], '%d-%m-%Y')
            remark = None

            sql_amc_id = db_session.query(AMC.AMC_Id).filter(AMC.AMC_Code == amccode).filter(AMC.Is_Deleted != 1).scalar()

            sql_content_id = db_session.query(Content).filter(Content.Content_DateTime == date).filter(Content.Content_Header == storyheadline).filter(Content.Content_Type_Id == 4).one_or_none()
            if not sql_content_id:
                Content_insert = Content()
                Content_insert.Content_Category_Id = 3
                Content_insert.Content_Type_Id = 4
                Content_insert.Content_Header = storyheadline
                Content_insert.Content_SubHeader = None
                Content_insert.Content_Detail = '<div class="vr-articleContent__detail-block__article-body" id="story-details-text">' + storytexthtml + '</div>'
                Content_insert.Content_Source = 'Value Research'
                Content_insert.Content_DateTime = date
                Content_insert.Is_Deleted = 0
                Content_insert.Images_URL = None
                Content_insert.Content_Name = None
                Content_insert.Is_Front_Dashboard = None
                Content_insert.AMC_Id = sql_amc_id
                Content_insert.Product_Id = PRODUCT_ID
                Content_insert.Fund_Id = None
                Content_insert.Created_By = user_id
                Content_insert.Created_Date = TODAY
                Content_insert.Updated_By = None
                Content_insert.Updated_Date = None
                
                db_session.add(Content_insert)
                db_session.commit()
                remark = 'News feeds uploaded successfully.'

            else:
            
                Content.Content_Detail = '<div class="vr-articleContent__detail-block__article-body" id="story-details-text">' + storytexthtml + '</div>'
                Content.AMC_Id = sql_amc_id
                Content.Updated_By = user_id
                Content.Updated_Date = TODAY
                
                db_session.commit()
                remark = 'News feeds updated successfully.'

            item = row.copy()
            item["Remarks"] = remark
            items.append(item)
        
    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/DailyUpdateNews_17032022.csv"
    READ_PATH = "read/DailyUpdateNews_17032022.csv"
    import_newsfeed_file(FILE_PATH, READ_PATH, USER_ID)