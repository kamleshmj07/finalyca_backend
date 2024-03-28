import os
import pandas as pd
import numpy as np
from datetime import date
from typing import List

from utils import *
from utils.finalyca_store import *
from bizlogic.importer_helper import create_or_update_holding_security_for_debt, create_or_update_debt_security,\
                                     create_or_update_debt_redemption, create_or_update_debt_calloption, create_or_update_debt_putoption,\
                                     create_or_update_debt_credit_ratings, get_fi_data_df, split_files_for_bilav
'''
Validations that can be added extra:
1] Any dates from any of the datasets cannot be greater than maturity_date + T5 days (T5 is an assumption)


'''

debug = False


def import_debt_security_data(db_session, asof_date, user_id, df_debt, df_redmt, df_call, df_put, df_credit):

    df_exceptions = pd.DataFrame(columns=['isin', 'dataset', 'exception_info'])
    try:
        # ------------ 1. Holding Security table data load
        df_holding = df_debt[["Security_Name", "Security_Type", "Bond_Type", "Issuer", "Bilav_Code", "Interest_Rate",
                              "Currency", "Is_Listed", "Face_Value", "Paid_Up_Value", "ISIN", "Maturity_Date"]].copy(deep=True)
        df_holding['HoldingSecurity_Type'] = 'DEBT'
        df_holding['Bilav_Code'] = 'BLV_' + df_holding['Bilav_Code'].astype(str)

        # renaming columns as per holding security table for crud operations
        df_holding.rename(columns={
            'Security_Name': 'HoldingSecurity_Name',
            'Security_Type': 'Asset_Class',
            'Bond_Type': 'Instrument_Type', 
            'Issuer': 'Issuer_Name',
            'Bilav_Code': 'Co_Code',
            # 'Bilav_Code': 'Vendor_Code',
            'ISIN': 'ISIN_Code'
        }, inplace=True)
        # NOTE: we are purposely skipping update to issuer code, to avoid overriding cmots issuer code i.e. 'Bilav_Internal_Issuer_Id': 'Issuer_Code'

        lst_data = df_holding.to_dict(orient='records')
        lst_exception = create_or_update_holding_security_for_debt(db_session, lst_data, user_id)
        df_exceptions = pd.concat([df_exceptions, pd.DataFrame(lst_exception)], ignore_index=True)

        # ------------ 2. Debt Security table data load
        lst_exception, lst_data = [], []
        drop_columns = ["File_Type", "Interest_Rate", "Currency",
                        "Is_Listed", "Face_Value", "Paid_Up_Value", "Maturity_Date"]
        df_debt.drop(drop_columns, axis=1, inplace=True)
        df_debt['Markup'] = df_debt['Markup'].astype(float)

        # Exclude all the isins that are not processed for holding security table to maintain data integrity
        if not df_exceptions.empty:
            lst_isins = list(df_exceptions['isin'])
            df_debt = df_debt[~df_debt['ISIN'].isin(lst_isins)]

        df_debt = df_debt.where(pd.notnull(df_debt), None)
        lst_data = df_debt.to_dict(orient='records')
        lst_exception = create_or_update_debt_security(db_session, lst_data, user_id)
        df_exceptions = pd.concat([df_exceptions, pd.DataFrame(lst_exception)], ignore_index=True)

        # ------------ 3. Debt Redemption table data load
        if not df_exceptions.empty:
            lst_isins = list(df_exceptions['isin'])
            df_redmt = df_redmt[~df_redmt['ISIN'].isin(lst_isins)]
            df_call = df_call[~df_call['ISIN'].isin(lst_isins)]
            df_put = df_put[~df_put['ISIN'].isin(lst_isins)]
            df_credit = df_credit[~df_credit['ISIN'].isin(lst_isins)]

        lst_exception, lst_data = [], []
        drop_columns = ["File_Type"]
        df_redmt.drop(drop_columns, axis=1, inplace=True)
        lst_data = df_redmt.to_dict(orient='records')
        lst_exception = create_or_update_debt_redemption(db_session, lst_data, user_id)
        df_exceptions = pd.concat([df_exceptions, pd.DataFrame(lst_exception)], ignore_index=True)

        # ------------ 4. Debt CallOption table data load
        lst_exception, lst_data = [], []
        drop_columns = ["File_Type"]
        df_call.drop(drop_columns, axis=1, inplace=True)
        lst_data = df_call.to_dict(orient='records')
        lst_exception = create_or_update_debt_calloption(db_session, lst_data, user_id)
        df_exceptions = pd.concat([df_exceptions, pd.DataFrame(lst_exception)], ignore_index=True)

        # ------------ 5. Debt PutOption table data load
        lst_exception, lst_data = [], []
        drop_columns = ["File_Type"]
        df_put.drop(drop_columns, axis=1, inplace=True)
        lst_data = df_put.to_dict(orient='records')
        lst_exception = create_or_update_debt_putoption(db_session, lst_data, user_id)
        df_exceptions = pd.concat([df_exceptions, pd.DataFrame(lst_exception)], ignore_index=True)

        # ------------ 6. Debt CreditRating table data load
        lst_exception, lst_data = [], []
        drop_columns = ["File_Type"]
        df_credit.drop(drop_columns, axis=1, inplace=True)
        lst_data = df_credit.to_dict(orient='records')
        lst_exception = create_or_update_debt_credit_ratings(db_session, lst_data, user_id, asof_date)
        df_exceptions = pd.concat([df_exceptions, pd.DataFrame(lst_exception)], ignore_index=True)

        return df_exceptions

    except Exception as ex:
        print(f'Exception occurred during data import {ex}')
        raise ex

def validate_mandatory_columns(df, list_mandatory_columns : List[str], dataset_name):
    data_masking = df[list_mandatory_columns].isna().any(axis=1)
    df_ex = df[data_masking][['ISIN']] if not df[data_masking].empty else pd.DataFrame()
    df_ex.rename(columns={'ISIN':'isin'}, inplace=True)
    df_ex["dataset"] = dataset_name
    df_ex["exception_info"] = 'data validation failed - mandatory fields missing'
    df = df[~data_masking]

    return df_ex, df


# ---------------------------------------------------------------- main script

def import_bilav_debt_data(input_file_path, output_file_path, user_id):

    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    db_session = get_finalyca_scoped_session(is_production_config(config))
    asof_date = date.today()

    # Step 1: read data into dataframe
    # NOTE: we need to maintain the columns sequence as in the data dictionary or file specs in the fixed income.
    dict_columns = {
        'BOND': ["File_Type", "DebtSecurity_Id", "Security_Name", "ISIN", "Exchange_1", "Exchange_1_Local_Code", "Exchange_2",
                 "Exchange_2_Local_Code", "Exchange_3", "Exchange_3_Local_Code", "Bilav_Code", "Security_Type", "Bond_Type_Code",
                 "Bond_Type", "Currency", "Country", "Face_Value", "Paid_Up_Value", "Bilav_Internal_Issuer_Id", "LEI", "CIN", "Issuer",
                 "Issue_Price", "Issue_Date", "Maturity_Price", "Maturity_Based_On", "Maturity_Benchmark_Index", "Maturity_Price_As_Perc",
                 "Maturity_Date", "Is_Perpetual", "On_Tap_Indicator", "Deemed_Allotment_Date", "Coupon_Type_Code", "Coupon_Type",
                 "Interest_Rate", "Interest_Payment_Frequency_Code", "Interest_Payment_Frequency", "Interest_Payout_1", "Is_Cumulative",
                 "Compounding_Frequency_Code", "Compounding_Frequency", "Interest_Accrual_Convention_Code", "Interest_Accrual_Convention",
                 "Is_Listed", "Min_Investment_Amount", "FRN_Index_Benchmark", "FRN_Index_Benchmark_Desc", "Interest_Pay_Date_1",
                 "Interest_Pay_Date_2", "Interest_Pay_Date_3", "Interest_Pay_Date_4", "Interest_Pay_Date_5", "Interest_Pay_Date_6",
                 "Interest_Pay_Date_7", "Interest_Pay_Date_8", "Interest_Pay_Date_9", "Interest_Pay_Date_10", "Interest_Pay_Date_11",
                 "Interest_Pay_Date_12", "Issuer_Type_Code", "Issuer_Type", "Issue_Size", "Outstanding_Amount", "Outstanding_Amount_Date",
                 "Yield_At_Issue", "Maturity_Structure_Code", "Maturity_Structure", "Convention_Method_Code", "Convention_Method",
                 "Interest_BDC_Code", "Interest_BDC", "Is_Variable_Interest_Payment_Date", "Interest_Commencement_Date",
                 "Coupon_Cut_Off_Days", "Coupon_Cut_Off_Day_Convention", "FRN_Type", "FRN_Interest_Adjustment_Frequency", "Markup",
                 "Minimum_Interest_Rate", "Maximum_Interest_Rate", "Is_Guaranteed", "Is_Secured", "Security_Charge", "Security_Collateral",
                 "Tier", "Is_Upper", "Is_Sub_Ordinate", "Is_Senior", "Is_Callable", "Is_Puttable", "Strip", "Is_Taxable",
                 "Latest_Applied_INTPY_Annual_Coupon_Rate", "Latest_Applied_INTPY_Annual_Coupon_Rate_Date", "Bond_Notes", "End_Use",
                 "Initial_Fixing_Date", "Initial_Fixing_Level", "Final_Fixing_Date", "Final_Fixing_Level", "PayOff_Condition",
                 "Majority_Anchor_Investor", "Security_Cover_Ratio", "Margin_TopUp_Trigger", "Current_Yield", "Security_Presentation_Link",
                 "Coupon_Reset_Event", "MES_Code", "Macro_Economic_Sector", "Sect_Code", "Sector", "Ind_Code", "Industry",
                 "Basic_Ind_Code", "Basic_Industry", "Record_Action"],
        'REDMT': ["File_Type", "DebtSecurity_Id", "DebtRedemption_Id", "ISIN", "Redemption_Date", "Redemption_Type_Code",
                  "Redemption_Type", "Redemption_Currency", "Redemption_Price", "Redemption_Amount", "Redemption_Price_As_Perc",
                  "Redemption_Percentage", "Redemption_Premium", "Redemption_Premium_As_Perc", "Is_Mandatory_Redemption",
                  "Is_Part_Redemption", "Record_Action"],
        'CALL': ["File_Type", "DebtSecurity_Id", "DebtCallOption_Id", "ISIN", "Call_Type_Code", "Call_Type", "From_Date", "To_Date",
                 "Notice_From_Date", "Notice_To_Date", "Min_Notice_Days", "Max_Notice_Days", "Currency", "Call_Price", "Call_Price_As_Perc",
                 "Is_Formulae_Based", "Is_Mandatory_Call", "Is_Part_Call", "Record_Action"],
        'PUT': ["File_Type", "DebtSecurity_Id", "DebtPutOption_Id", "ISIN", "Put_Type_Code", "Put_Type", "From_Date", "To_Date",
                "Notice_From_Date", "Notice_To_Date", "Min_Notice_Days", "Max_Notice_Days", "Currency", "Put_Price", "Put_Price_As_Perc",
                "Is_Formulae_Based", "Is_Mandatory_Put", "Is_Part_Put", "Record_Action"],
        'CRDRT': ["File_Type", "DebtSecurity_Id", "DebtCreditRating_Id", "ISIN", "Rating_Agency", "Rating_Date", "Rating_Symbol", "Rating_Direction_Code",
                  "Rating_Direction", "Watch_Flag_Code", "Watch_Flag", "Watch_Flag_Reason_Code", "Watch_Flag_Reason", "Rating_Prefix", "Prefix_Description",
                  "Rating_Suffix", "Suffix_Description", "Rating_Outlook_Description", "Expected_Loss", "Record_Action"]
    }

    dict_filenames = None
    df_import_exceptions, df_exceptions = pd.DataFrame(), pd.DataFrame()

    try:
        dict_filenames = split_files_for_bilav(input_file_path, asof_date, dict_columns)

        df_debt, df_sov = pd.DataFrame(), pd.DataFrame()
        df_redmt, df_credit = pd.DataFrame(), pd.DataFrame()
        df_call, df_put = pd.DataFrame(), pd.DataFrame()

        # read the fi datasets and process separately
        for file_type, file_name in dict_filenames.items():

            if file_type == 'BOND':
                # read BOND file
                df_debt = get_fi_data_df(file_name)

                # validate data
                # 1. mandatory fields validation
                mandatory_columns = ["DebtSecurity_Id", "Security_Name", "ISIN", "Exchange_1", "Bilav_Code", "Security_Type", "Bond_Type_Code",
                                     "Bond_Type", "Currency", "Country", "Face_Value", "Paid_Up_Value", "Bilav_Internal_Issuer_Id", "Issuer",
                                     "Issue_Price", "Issue_Date", "Maturity_Price", "Maturity_Price_As_Perc", "Maturity_Date", "Coupon_Type_Code", "Coupon_Type",
                                     "Is_Listed", "Min_Investment_Amount", "Issuer_Type_Code", "Issuer_Type", "Maturity_Structure_Code", "Maturity_Structure",
                                     "Is_Variable_Interest_Payment_Date", "Is_Callable", "Is_Puttable", "Is_Taxable"]
                df_ex, df_debt = validate_mandatory_columns(df_debt, mandatory_columns, dataset_name='DebtSecurity')
                df_exceptions = pd.concat([df_exceptions, df_ex], ignore_index=True)

                # prepare data
                # 1. date formatting
                date_columns = ['Issue_Date', 'Maturity_Date', 'Deemed_Allotment_Date', 'Latest_Applied_INTPY_Annual_Coupon_Rate_Date',
                                'Interest_Payout_1', 'Outstanding_Amount_Date', 'Interest_Commencement_Date']
                for col in date_columns:
                    df_debt[col] = df_debt[col].apply(pd.to_datetime, format=r'%d-%m-%Y')

                # 2. boolean conversion
                boolean_columns = ['On_Tap_Indicator', 'Is_Cumulative', 'Is_Variable_Interest_Payment_Date',
                                   'Is_Guaranteed', 'Is_Secured', 'Security_Collateral', 'Is_Upper',
                                   'Is_Sub_Ordinate', 'Is_Senior', 'Is_Callable', 'Is_Puttable', 'Is_Taxable']

                for col in boolean_columns:
                    df_debt.loc[df_debt[col] == 'Y', col] = True
                    df_debt.loc[df_debt[col] == 'N', col] = False

                # 3. NaN handling to None to push data to database without exception
                df_debt = df_debt.astype(object).replace(np.nan, None)

                # 4. Other dataprep steps
                df_debt.loc[df_debt['Is_Listed'] == 'Listed', 'Is_Listed'] = 1
                df_debt.loc[df_debt['Is_Listed'] == 'Unlisted', 'Is_Listed'] = 0

                df_debt.loc[df_debt['Is_Senior'] == 'S', 'Is_Senior'] = 'Senior'
                df_debt.loc[df_debt['Is_Senior'] == 'M', 'Is_Senior'] = 'Mezzanine'
                df_debt.loc[df_debt['Is_Senior'] == 'J', 'Is_Senior'] = 'Junior'

                df_debt.loc[df_debt['Strip'] == 'IS', 'Strip'] = 'Interest Stripped'
                df_debt.loc[df_debt['Strip'] == 'PS', 'Strip'] = 'Principal Stripped'

                # 5. Setup data for adding ratings for soverign bonds
                df_sov = df_debt[(df_debt['Issuer_Type'] == 'Government') | (df_debt['Issuer_Type'] == 'State Government')]
                df_sov['DebtCreditRating_Id'] = 1
                df_sov['Rating_Agency'] = 'Sovereign'
                df_sov['Rating_Date'] = asof_date
                df_sov['Rating_Symbol'] = 'SOV'
                df_sov['Record_Action'] = 'A'
                df_sov = df_sov[["DebtSecurity_Id", "DebtCreditRating_Id", "ISIN", "Rating_Agency", "Rating_Date", "Rating_Symbol"]]

                df_debt['Record_Action'] = df_debt['Record_Action'].str.replace(',','')

                if debug:
                    print(df_debt.head())
                    df_debt.to_excel(file_name.replace('.csv', '.xlsx'))

            elif file_type == 'REDMT':
                # read REDMT file
                df_redmt = get_fi_data_df(file_name)

                # validate data
                # 1. mandatory fields validation
                mandatory_columns = ["DebtSecurity_Id", "DebtRedemption_Id", "ISIN", "Redemption_Date", "Redemption_Type_Code",
                                     "Redemption_Type", "Redemption_Currency", "Redemption_Price", "Redemption_Amount", "Redemption_Price_As_Perc",
                                     "Redemption_Percentage", "Is_Mandatory_Redemption", "Is_Part_Redemption"]
                df_ex, df_redmt = validate_mandatory_columns(df_redmt, mandatory_columns, dataset_name='DebtRedemption')
                df_exceptions = pd.concat([df_exceptions, df_ex], ignore_index=True)

                # prepare data
                # 1. date formatting
                df_redmt['Redemption_Date'] = df_redmt['Redemption_Date'].apply(pd.to_datetime, format=r'%d-%m-%Y')

                # 2. boolean conversion
                boolean_columns = ['Is_Mandatory_Redemption', 'Is_Part_Redemption']

                for col in boolean_columns:
                    df_redmt.loc[df_redmt[col] == 'Y', col] = True
                    df_redmt.loc[df_redmt[col] == 'N', col] = False

                # 3. NaN handling to None to push data to database without exception
                df_redmt = df_redmt.astype(object).replace(np.nan, None)


                df_redmt['Record_Action'] = df_redmt['Record_Action'].str.replace(',','')

                if debug:
                    print(df_redmt.head())
                    df_redmt.to_excel(file_name.replace('.csv', '.xlsx'))

            elif file_type == 'CALL':
                # read CALL file
                df_call = get_fi_data_df(file_name)

                # validate data
                # 1. mandatory fields validation
                mandatory_columns = ["DebtSecurity_Id", "DebtCallOption_Id", "ISIN", "Call_Type_Code", "From_Date",
                                     "Currency", "Is_Mandatory_Call", "Is_Part_Call"]
                df_ex, df_call = validate_mandatory_columns(df_call, mandatory_columns, dataset_name='DebtCallOption')
                df_exceptions = pd.concat([df_exceptions, df_ex], ignore_index=True)

                # prepare data
                # 1. date formatting
                date_columns = ['From_Date', 'To_Date', 'Notice_From_Date', 'Notice_To_Date']
                for col in date_columns:
                    df_call[col] = pd.to_datetime(df_call[col], format=r'%d-%m-%Y', errors="coerce")

                # NOTE: 
                # KM:   The below date conversion code is alternate solution to the errors="coerce" while converting datetime,
                #       but the out of range date will never be caught in exceptions. 
                #       So please consider this only after discussion with Vijay.
                # import datetime
                # df["Date"] = df["Date"].apply(lambda x: datetime.datetime.strptime(x, '%Y-%d-%m').date())

                # 2. boolean conversion
                boolean_columns = ['Is_Formulae_Based', 'Is_Mandatory_Call', 'Is_Part_Call']

                for col in boolean_columns:
                    df_call.loc[df_call[col] == 'Y', col] = True
                    df_call.loc[df_call[col] == 'N', col] = False

                # 3. Re-validate again after data preparation
                df_ex, df_call = validate_mandatory_columns(df_call, mandatory_columns, dataset_name='DebtCallOption')
                df_exceptions = pd.concat([df_exceptions, df_ex], ignore_index=True)

                # 4. NaN handling to None to push data to database without exception
                df_call = df_call.astype(object).replace(np.nan, None)


                df_call['Record_Action'] = df_call['Record_Action'].str.replace(',','')

                if debug:
                    print(df_call.head())
                    df_call.to_excel(file_name.replace('.csv', '.xlsx'))

            elif file_type == 'PUT':
                # read PUT file
                df_put = get_fi_data_df(file_name)

                # validate data
                # 1. mandatory fields validation
                mandatory_columns = ["DebtSecurity_Id", "DebtPutOption_Id", "ISIN", "Put_Type_Code", "From_Date",
                                     "Currency", "Is_Mandatory_Put", "Is_Part_Put"]
                df_ex, df_put = validate_mandatory_columns(df_put, mandatory_columns, dataset_name='DebtPutOption')
                df_exceptions = pd.concat([df_exceptions, df_ex], ignore_index=True)

                # prepare data
                # 1. date formatting
                date_columns = ['From_Date', 'To_Date', 'Notice_From_Date', 'Notice_To_Date']
                for col in date_columns:
                    df_put[col] = df_put[col].apply(pd.to_datetime, format=r'%d-%m-%Y')

                # 2. boolean conversion
                boolean_columns = ['Is_Formulae_Based', 'Is_Mandatory_Put', 'Is_Part_Put']

                for col in boolean_columns:
                    df_put.loc[df_put[col] == 'Y', col] = True
                    df_put.loc[df_put[col] == 'N', col] = False

                # 3. NaN handling to None to push data to database without exception
                df_put = df_put.astype(object).replace(np.nan, None)


                df_put['Record_Action'] = df_put['Record_Action'].str.replace(',','')

                if debug:
                    print(df_put.head())
                    df_put.to_excel(file_name.replace('.csv', '.xlsx'))

            elif file_type == 'CRDRT':
                # read CRDRT file
                df_credit = get_fi_data_df(file_name)

                # validate data
                # 1. mandatory fields validation
                mandatory_columns = ["DebtSecurity_Id", "DebtCreditRating_Id", "ISIN", "Rating_Agency", "Rating_Date", "Rating_Symbol"]
                df_ex, df_credit = validate_mandatory_columns(df_credit, mandatory_columns, dataset_name='DebtCreditRating')
                df_exceptions = pd.concat([df_exceptions, df_ex], ignore_index=True)

                # prepare data
                # 1. date formatting
                df_credit['Rating_Date'] = df_credit['Rating_Date'].apply(pd.to_datetime, format=r'%d-%m-%Y')

                # 2. Append soverign credit rating
                df_credit = pd.concat([df_credit, df_sov], ignore_index=True, sort=False)

                # 3. NaN handling to None to push data to database without exception
                df_credit = df_credit.astype(object).replace(np.nan, None)

                df_credit['Record_Action'] = df_credit['Record_Action'].str.replace(',', '')

                if debug:
                    print(df_credit.head())
                    df_credit.to_excel(file_name.replace('.csv', '.xlsx'))

        # load data
        df_import_exceptions = import_debt_security_data(db_session, asof_date, user_id, df_debt.copy(deep=True), df_redmt.copy(deep=True), df_call.copy(deep=True), df_put.copy(deep=True), df_credit.copy(deep=True))
    except Exception as ex:
        raise Exception(f'Exception occurred during the import process - {ex}')
    finally:
        if dict_filenames:
            for file_type, file_name in dict_filenames.items():
                print('Deleting the files that were generated during the process from the source files.')
                os.unlink(file_name)

        df_exceptions = pd.concat([df_exceptions, df_import_exceptions], ignore_index=True)
        df_exceptions.to_csv(output_file_path)


if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "C:\\dev\\backend\\tasks\\samples\\FINA_202403281111.txt"
    READ_PATH = "C:\\dev\\backend\\tasks\\read\\FINA_202403281111.csv"
    import_bilav_debt_data(FILE_PATH, READ_PATH, USER_ID)
