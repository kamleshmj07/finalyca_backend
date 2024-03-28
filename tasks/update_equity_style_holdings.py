import os
from utils import get_config
from utils.finalyca_store import get_finalyca_scoped_session, is_production_config
from analytics.analytics import calculate_investment_style_for_stocks
from datetime import datetime, timedelta
from fin_models.transaction_models import Fundamentals
from fin_models.masters_models import HoldingSecurity
from sqlalchemy import and_
import pandas as pd

def update_equity_style(db_session):
    update_equity_style_holdings(db_session, False)

def update_equity_style_holdings(db_session, dry_run = True):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    TODAY = datetime.today()

    with db_session.begin():
        startdate = (datetime.today() - timedelta(days=55)).date()
        sql_f = db_session.query(Fundamentals.CO_CODE,
                                 Fundamentals.PriceDate,
                                 Fundamentals.PE,
                                 Fundamentals.PBV,
                                 Fundamentals.EPS,
                                 Fundamentals.mcap,
                                 HoldingSecurity.ISIN_Code).join(HoldingSecurity, HoldingSecurity.Co_Code == Fundamentals.CO_CODE)\
                                                           .filter(and_(Fundamentals.PriceDate >= startdate, Fundamentals.PE != 0, Fundamentals.PBV != 0))\
                                                           .filter(HoldingSecurity.ISIN_Code != None)\
                                                           .filter(and_(Fundamentals.Is_Deleted != 1, HoldingSecurity.Is_Deleted != 1))
        
        df_f = pd.DataFrame(sql_f)
        df_f = calculate_investment_style_for_stocks(df_f)

        # print(df_f)

        if dry_run == False:
            values = dict(zip(df_f.ISIN_Code, df_f.Equity_Style))

            for k, v in values.items():
                update_values = {
                    HoldingSecurity.Equity_Style: v,
                    HoldingSecurity.Updated_Date: TODAY,
                }

                stmt = db_session.query(HoldingSecurity).filter(HoldingSecurity.ISIN_Code == k).update(update_values)
                print('Rows Updated', stmt)



if __name__ == '__main__':
    update_equity_style_holdings(None, False)




