import numpy as np
import pandas as pd
import os
import datetime
import json
from functools import reduce
from fin_models.servicemanager_models import ReportJobs
import xml.dom.minidom
from datetime import date, timedelta, datetime as dt
from tasks.ftp_fetch import download_cmots_files
from utils import *
from utils.finalyca_store import *
from bizlogic.importer_helper import fundmanager_upload
import math

ROOT_DIR = r"C:\dev\backend\tasks\samples\cmots"
mypath = os.path.dirname(os.path.abspath(__file__))
updated_list = list()

def upload_mf_fundmanager(db_session):
    # fetch files from FTP
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)    

    ftp_host =config["CMOTS_FTP_HOST"]
    ftp_port = config["CMOTS_FTP_PORT"]
    ftp_user = config["CMOTS_FTP_USER"]
    ftp_pwd = config["CMOTS_FTP_PASS"]
    local_dir = config["CMOTS_FTP_DIR"]
    remote_dir = config["CMOTS_FTP_REMOTE_DIR"]
    days_count = config["CMOTS_FTP_DAY"]     

    today = dt.today()
    target_date = today + timedelta(days=int(days_count))

    df_dbdmfmgr = pd.DataFrame()
    df_dbdprof = pd.DataFrame()
    df_dbdfundm = pd.DataFrame()
    
    remote_dir = remote_dir.replace("@ddmmyyyy",target_date.strftime('%d%m%Y'))
    
    # get data for MF Fund Manager Date History
    # 1] dbdmfmgr.csv
    try:
        resp_file_name = download_cmots_files(ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir, local_dir, target_date, 'dbdmfmgr.csv')
    
        response_file = os.path.join(local_dir, resp_file_name)
        # response_file = 'D:\\changes\\29-08-23 fund manager upload changes CMOTS\\Finalyca_MF_FundManagerDataDump_23Aug2023\\dbdmfmgr.csv'

        if resp_file_name:
            lst_col = ["MF_COCODE","MF_SCHCODE","SCHTYPCODE","FUNDCODE","AREA","WEF","TODATE","STATUS","SLNO","FLAG"]
            lst_used = ["MF_SCHCODE","FUNDCODE","AREA","WEF","TODATE","STATUS", "FLAG"]
            lst_drop = [item for item in lst_col if item not in lst_used]

            new_col = ["Plancode", "FM_Code", "Area_", "DateFrom", "DateTo", "Status_", "Flag_"]
            rename_col_mapping = dict(zip(lst_used, new_col))
            
            df_dbdmfmgr = get_df(response_file, lst_col, lst_drop, rename_col_mapping)
            # df_dbdmfmgr.sort_values("MF_SCHCODE", inplace=True)
            df_dbdmfmgr.head()
    except Exception as ex:
        df_dbdmfmgr = pd.DataFrame()


    # get data for MF Fund Manager Profile Details
    # 2] dbdprof.csv
    try:
        resp_file_name = download_cmots_files(ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir, local_dir, target_date, 'dbdprof.csv')
        
        response_file = os.path.join(local_dir, resp_file_name)
        # response_file = 'D:\\changes\\29-08-23 fund manager upload changes CMOTS\\Finalyca_MF_FundManagerDataDump_23Aug2023\\dbdprof.csv'
        if resp_file_name:
            lst_col = ["FUNDCODE","QUAL","DESIG","EXPERIENCE","REMARKS","FLAG"]
            lst_used = ["FUNDCODE","QUAL","DESIG","EXPERIENCE","REMARKS","FLAG"]
            lst_drop = [item for item in lst_col if item not in lst_used]

            new_col = ["FM_Code", "Qualification", "Designation", "Experience", "Remarks_", "Flag_"]
            rename_col_mapping = dict(zip(lst_used, new_col))

            df_dbdprof = get_df(response_file, lst_col, lst_drop, rename_col_mapping)
            # df_dbdprof.sort_values("MF_SCHCODE", inplace=True)
            df_dbdprof.head()
    except Exception as ex:
        df_dbdprof = pd.DataFrame()

    # get data for MF Fund Manager Names
    # 3] dbdfundm.csv
    try:
        resp_file_name = download_cmots_files(ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir, local_dir, target_date, 'dbdfundm.csv')
        
        response_file = os.path.join(local_dir, resp_file_name)
        # response_file = 'D:\\changes\\29-08-23 fund manager upload changes CMOTS\\Finalyca_MF_FundManagerDataDump_23Aug2023\\dbdfundm.csv'
        if resp_file_name:
            lst_col = ["FUNDCODE","FUND_MGR","FLAG"]
            lst_used = ["FUNDCODE","FUND_MGR","FLAG"]
            lst_drop = [item for item in lst_col if item not in lst_used]

            new_col = ["FM_Code", "Fund Manager", "Flag_"]
            rename_col_mapping = dict(zip(lst_used, new_col))

            df_dbdfundm = get_df(response_file, lst_col, lst_drop, rename_col_mapping)
            df_dbdfundm.head()    
    except Exception as ex:
        df_dbdfundm = pd.DataFrame()

    #1 - insert new fund manager - dbdfundm.csv join with other files to get newly added fund manager details
    if not df_dbdfundm.empty and not df_dbdprof.empty and not df_dbdmfmgr.empty:
        df1 = pd.merge(df_dbdfundm, df_dbdprof, on='FM_Code', how='inner')
        result = pd.merge(df1, df_dbdmfmgr, on='FM_Code', how='inner')
        result.fillna('')

        # result.to_csv('result.csv')
        if not result.empty:
            updated_list = list()

            for i in result.index:
                if result['DateTo'][i]:
                    a = result['DateTo'][i]
                fm_name= result['Fund Manager'][i]
                fm_code = F"fm_{result['FM_Code'][i]}_mf" if result['FM_Code'][i] else ''
                fm_qualification = result['Qualification'][i]
                fm_designation = result['Designation'][i]
                fm_experience = result['Experience'][i]
                remarks = result['Remarks_'][i]
                flag_y = result['Flag__y'][i]                
                area = result['Area_'][i]
                datefrom = dt.strptime(result['DateFrom'][i], '%d/%m/%Y') if result['DateFrom'][i] and str == type(result['DateFrom'][i]) else None #Not a date value handling like this as np.nan it not working
                dateto = dt.strptime(result['DateTo'][i], '%d/%m/%Y') if result['DateTo'][i] and str == type(result['DateTo'][i]) else None
                status = result['Status_'][i]
                flag = result['Flag_'][i]
                plan_code = F"{result['Plancode'][i]}_01" if result['Plancode'][i] else ''

                if plan_code and fm_code:
                    add_update_fm_details(db_session, plan_code, fm_code, datefrom, dateto, fm_name, fm_experience, fm_qualification, fm_designation)

    #2 - update dbdmfmgr.csv -- for incremental fund manager end date update
    if not df_dbdmfmgr.empty:        
        df_dbdmfmgr.replace(np.nan, '')
        updated_list = list()

        for i in df_dbdmfmgr.index:
            fm_code = F"fm_{df_dbdmfmgr['FM_Code'][i]}_mf" if df_dbdmfmgr['FM_Code'][i] else ''
            area = df_dbdmfmgr['Area_'][i]
            datefrom = dt.strptime(df_dbdmfmgr['DateFrom'][i], '%d/%m/%Y') if df_dbdmfmgr['DateFrom'][i] and str == type(df_dbdmfmgr['DateFrom'][i]) else None #Not a date value handling like this as np.nan it not working
            dateto = dt.strptime(df_dbdmfmgr['DateTo'][i], '%d/%m/%Y') if df_dbdmfmgr['DateTo'][i] and str == type(df_dbdmfmgr['DateTo'][i]) else None
            status = df_dbdmfmgr['Status_'][i]
            flag = df_dbdmfmgr['Flag_'][i]
            plan_code = F"{df_dbdmfmgr['Plancode'][i]}_01" if df_dbdmfmgr['Plancode'][i] else ''

            if plan_code and fm_code:
                add_update_fm_details(db_session, plan_code, fm_code, datefrom, dateto, fm_name, fm_experience, fm_qualification, fm_designation)
    
    #update fund manager profile
    if not df_dbdprof.empty:
        updated_list = list()

        for i in df_dbdprof.index:
            fm_code = F"fm_{df_dbdprof['FM_Code'][i]}_mf" if df_dbdprof['FM_Code'][i] else ''
            fm_qualification = df_dbdprof['Qualification'][i]
            fm_designation = df_dbdprof['Designation'][i]
            fm_experience = df_dbdprof['Experience'][i]
            fm_remarks = df_dbdprof['Remarks_'][i]

            if fm_code:
               remark = fundmanager_upload(db_session, None, fm_code, '', '', fm_experience, fm_qualification, fm_designation, '', '', True)
            

    # df_dbdmfmgr.to_csv('df_dbdmfmgr.csv')
    
    

def add_update_fm_details(db_session, plan_code, fm_code, datefrom, dateto, fm_name, fm_experience, fm_qualification, fm_designation):
    print(plan_code)
    fund_id = db_session.query(Fund.Fund_Id)\
                                            .select_from(Plans)\
                                            .join(MFSecurity, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                            .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                            .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                            .filter(Plans.Plan_Code == plan_code).filter(MFSecurity.Is_Deleted != 1).filter(MFSecurity.Status_Id == 1).filter(Plans.Is_Deleted != 1).filter(PlanProductMapping.Is_Deleted != 1).filter(Product.Product_Id == 1).scalar()          
          
    if not [fund_id, fm_code] in updated_list:#check if fund manager already updated or added fund wise. Preventing multiple calls bcoz cmots is giving us fund manager details at scheme level and we are maintaining fund manager at fund level
        remark = fundmanager_upload(db_session, fund_id, fm_code, '', fm_name, fm_experience, fm_qualification, fm_designation, datefrom, dateto)
        a =1

def get_df(file_path, lst_col, lst_drop_col, rename_col_mapping):    
    # print(file_path)
    if os.path.isfile(file_path):
        # print('Reading the file into df...')
        df = pd.read_csv(file_path, sep ='|', names=lst_col, encoding= 'unicode_escape' )
        df.iloc[:, -1] = df.iloc[:, -1].str.split('<</EOR>>', expand=True)
        # print(df.head())
        df = df.drop(lst_drop_col, axis=1)
        df.rename(columns=rename_col_mapping, inplace=True)
    else:
        df = pd.DataFrame()

    return df

if __name__ == '__main__':
    USER_ID = 1
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    upload_mf_fundmanager(db_session)

    