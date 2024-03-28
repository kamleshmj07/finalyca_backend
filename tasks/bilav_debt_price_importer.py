import os
import pandas as pd
import numpy as np
from datetime import date

from utils import *
from utils.finalyca_store import *
from bizlogic.importer_helper import get_fi_data_df, import_debt_security_price_data, get_rel_path

# TODO Complete run flag logic implementation
# TODO Convert the code to task running compatibilty
# TODO Add exception handling and manage transactions in the database
# TODO Setup ftp and scheduling
# TODO Move utility and helper functions to appropriate library
# TODO Email the exception report

# ---------------------------------------------------------------- main script
debug = True


def import_bilav_price_file(input_file_path, output_file_path, user_id):

    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    db_session = get_finalyca_scoped_session(is_production_config(config))
    asof_date = date.today()
    file_path = get_rel_path(input_file_path, __file__)


    # read data into dataframe
    # NOTE: maintain the columns sequence
    dict_columns = {
    'PRICE': ["DebtPrice_Id","Trading_Date","Exchange_Code","Segment_Code","Local_Code","ISIN","Issuer_Name",
              "Security_Name","Bond_Type","Issue_Date","Maturity_Date","No_Of_Trades","Traded_Qty","Traded_Value",
              "Open","High","Low","Close","Weighted_Avg_Price","WYTM","TT_Status","Trade_Type","Issue_Type",
              "FaceValuePrice","Settlement_Type","Residual_Maturity_Date","Residual_Maturity_Derived_From",
              "Clean_Dirty_Indicator","Dirty_Price"]
    }

    # TODO Need to review if we need the lookup columns from Bilav in sample "Exchange","Segment"
    # TODO Ask bilav to share the following additional fields - "DebtSecurity_Id","Currency"

    # read BOND price file
    df_price = get_fi_data_df(file_path)

    # prepare data

    # 1. drop columns
    # drop columns >> "Issuer_Name","Security_Name","Bond_Type","Issue_Date","Maturity_Date","Issue_Type"
    drop_columns = ["Issuer_Name","Security_Name","Bond_Type","Issue_Date","Maturity_Date","Issue_Type"]
    df_price.drop(drop_columns, axis=1, inplace=True)

    # 2. date formatting
    # TODO: Review if required or not as we have requested bilav to provide format
    date_columns = ['Trading_Date','Residual_Maturity_Date']
    for col in date_columns:
        df_price[col] = df_price[col].apply(pd.to_datetime, format=r'%d/%m/%Y')

    # 3. NaN handling
    blank_columns = ["Local_Code","Trade_Type","Settlement_Type"]

    for col in blank_columns:
        df_price[col] = df_price[col].astype(object).replace(np.nan, None)


    df_price['Residual_Maturity_Derived_From'] = df_price['Residual_Maturity_Derived_From'].str.strip()

    if debug == True:
        df_price.to_excel(file_path.replace('.csv', '.xlsx'))
        print(df_price.head())

    lst_exception = import_debt_security_price_data(db_session, df_price.copy(deep=True), user_id, asof_date)
    df_exceptions = pd.DataFrame(lst_exception, columns=lst_exception[0].keys()) if len(lst_exception) > 0 else pd.DataFrame()

    if debug == True:
        print(df_exceptions.head())


if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH =  r"C:\\dev\\backend\\tasks\\samples\\FIPR231121.csv"
    READ_PATH =  r"C:\\dev\\backend\\tasks\\read\\"
    import_bilav_price_file(FILE_PATH, READ_PATH, USER_ID)

