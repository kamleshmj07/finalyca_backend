import csv
from datetime import datetime as dt
import os
import re
import pandas as pd
from sqlalchemy import desc
from bizlogic.importer_helper import save_planproductmapping, get_rel_path, write_csv

from utils import *
from utils.finalyca_store import *
from fin_models import *
from bizlogic.common_helper import schedule_email_activity


classification_standard = {
    "Debt": "Debt",
    "Hybrid-Equity oriented": "Hybrid: Equity-oriented",
    "Discontinued": "Others",
    "Hybrid-Debt oriented": "Hybrid: Balanced Hybrid",
    "Equity-Index": "Other: Index Fund",
    "Equity-Diversified": "Equity: Others",
    "Liquid": "Debt: Liquid",
    "Guaranted": "Others",
    "Equity-Sector": "Equity: Others",
    "Gilt": "Debt: Gilt",
    "Fund of Funds": "Other: FoFs Domestic",
    "Fixed Maturity": "Debt: FMP",
    "Equity-Theme-Energy": "Equity: Sectoral : Thematic",
    "Equity-Theme-Infrastructure": "Equity: Sectoral : Thematic",
    "Equity-Thematic": "Equity: Sectoral : Thematic",
    "EQUITY Debt": "Debt",
    "DEBT Debt": "Debt"
    
}

assetclass_standard = {
    "BALANCED": "Hybrid"
}

def get_fund_or_amc_code(fund_or_amc_code, product_id):
    schemecode1 = ""
    if product_id == 2:
        schemecode1 = F"INS_{fund_or_amc_code}"
    else:
        schemecode1 = fund_or_amc_code
    return schemecode1

def get_plan_code(schemecode, product_id):
    schemecode1 = ""
    if product_id == 2:
        schemecode1 = F"{schemecode}_01"
    else:
        schemecode1 = schemecode
    return schemecode1

def import_ulip_fund_master_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    header = ["SCM_AMC_CD", "FundID", "SCM_SCHEME_NAME", "SCM_UNIT_FACE_VALUE", "SCM_TYPE_OF_FUND/NATURE","SCM_START_DATE","SCM_END_DATE",
              "SCH_REOPEN_DATE","BENCHMARK_NAME","Company","Classification","SCHEME_INVESTMENT_STRATEGY","FUND_MANAGER_NAME","SFINCode",
              "EquityMin","EquityMax","DebtMin","DebtMax","CommMin","CommMax","CashAnd_MoneyMarketMin","Cashand_MoneyMarketMax",
              "EquityDerivativesMin","EquityDerivativesMax","SCM_STATUS", "BnchmarkIndex_Co_Code", "Remarks"]
    items = list()
    exceptions_data = []

    PRODUCT_ID = 2
    TODAY = dt.today()
 
    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f, delimiter='|')
        for row in csvreader:
            try:
                row = {key.replace(',', ''):val for key, val in row.items()} #remove comma from dict key
                scm_start_date = None
                scm_end_date = None
                scm_reopen_date = None

                master_fund_id = 0
                master_classification_id = 0
                master_asset_class_id = 0
                master_benchmarkindices_id = 0

                scm_amc_cd = get_fund_or_amc_code(row["SCM_AMC_CD"], PRODUCT_ID)
                fund_id = get_fund_or_amc_code(row["FundID"], PRODUCT_ID)
                if fund_id == "":
                    raise Exception("FundID is empty. Review the exceptions and reload in a separate file.")

                scm_scheme_name = row["SCM_SCHEME_NAME"]
                scm_unit_face_value = row["SCM_UNIT_FACE_VALUE"]
                scm_type_of_fundnature = row["SCM_TYPE_OF_FUND/NATURE"]
                
                if row["SCM_START_DATE"] != "" and row["SCM_START_DATE"] != "0":
                    scm_start_date = dt.strptime(row["SCM_START_DATE"].replace("/","-"), '%d-%m-%Y')
                
                if row["SCM_END_DATE"] != "" and row["SCM_END_DATE"] != "0":
                    scm_end_date = dt.strptime(row["SCM_END_DATE"].replace("/","-"), '%d-%m-%Y')
                
                if row["SCH_REOPEN_DATE"] != "" and row["SCH_REOPEN_DATE"] != "0":
                    scm_reopen_date = dt.strptime(row["SCH_REOPEN_DATE"].replace("/","-"), '%d-%m-%Y')

                benchmark_name = row["BENCHMARK_NAME"]
                benchmarkindex_co_code = row["BnchmarkIndex_Co_Code"] if row["BnchmarkIndex_Co_Code"] != '0' else None
                company = row["Company"]
                # classification = re.sub(' Fund$', '', row["Classification"].split(',')[1].strip())

                classification = 'Others' if row["Classification"] == '' else ''
                
                if len(row["Classification"].split(','))>1:
                    classification = re.sub(' Fund$', '', row["Classification"].split(',')[1].strip())
                else:
                    classification = re.sub(' Fund$', '', row["Classification"].split(',')[0].strip())
                    
                scheme_invesyment_strategy = row["SCHEME_INVESTMENT_STRATEGY"]
                fund_manager_name = row["FUND_MANAGER_NAME"]
                sfin_code = row["SFINCode"]
                equity_min = row["EquityMin"]
                equity_max = row["EquityMax"]
                debt_min = row["DebtMin"]
                debt_max = row["DebtMax"]
                comm_min = row["CommMin"]
                comm_max = row["CommMax"]
                cash_money_market_min = row["CashAnd_MoneyMarketMin"]
                cash_money_market_max = row["Cashand_MoneyMarketMax"]
                equity_derivatives_min = row["EquityDerivativesMin"]
                equity_derivatives_max = row["EquityDerivativesMax"]
                
                if row["SCM_STATUS"] == 'Open':
                    scm_status = 'Active'
                elif row["SCM_STATUS"] == 'Close':
                    scm_status = 'InActive'
                else:
                    scm_status = row["SCM_STATUS"]

                Remarks = None

                master_amc_id = db_session.query(AMC.AMC_Id).filter(AMC.AMC_Code == scm_amc_cd).filter(AMC.Is_Deleted != 1).one_or_none()

                if not master_amc_id:
                    Remarks = "AMC not available, please add AMC first and upload again."
                    raise Exception("AMC not available, please add AMC first and upload again.")

                master_asset_class_id = db_session.query(AssetClass.AssetClass_Id)\
                                                  .filter(AssetClass.AssetClass_Name == assetclass_standard.get(scm_type_of_fundnature, scm_type_of_fundnature))\
                                                  .filter(AssetClass.Is_Deleted != 1).one_or_none()

                if not master_asset_class_id:
                    Remarks = f"Asset Class: {scm_type_of_fundnature} is not available in our system, please add Asset Class first and upload again."
                    raise Exception(Remarks)

                master_classification_id = db_session.query(Classification.Classification_Id)\
                                                     .filter(Classification.Classification_Name == classification_standard.get(classification, classification))\
                                                     .filter(Classification.Is_Deleted != 1).one_or_none()

                if not master_classification_id:
                    Remarks = f"Classification: {classification} is not available in our system, please add Classification first and upload again."
                    raise Exception(Remarks)

                if benchmarkindex_co_code:
                    sql_benchmarkindices = db_session.query(BenchmarkIndices)\
                                                     .filter(BenchmarkIndices.Co_Code == benchmarkindex_co_code,BenchmarkIndices.Is_Deleted != 1).first()
                else:
                    sql_benchmarkindices = db_session.query(BenchmarkIndices)\
                                                     .filter(BenchmarkIndices.BenchmarkIndices_Name == benchmark_name,BenchmarkIndices.Is_Deleted != 1).first()

                if not sql_benchmarkindices and benchmark_name != "0":
                    sql_benchmarkindices_insert = BenchmarkIndices()
                    sql_benchmarkindices_insert.BenchmarkIndices_Name = benchmark_name
                    sql_benchmarkindices_insert.BenchmarkIndices_Code = None
                    sql_benchmarkindices_insert.BenchmarkIndices_Description = benchmark_name
                    sql_benchmarkindices_insert.Co_Code = benchmarkindex_co_code
                    sql_benchmarkindices_insert.Is_Deleted = 0
                    sql_benchmarkindices_insert.Created_By = user_id
                    sql_benchmarkindices_insert.Created_Date = TODAY
                    sql_benchmarkindices_insert.Updated_By = None
                    sql_benchmarkindices_insert.Updated_Date = None

                    db_session.add(sql_benchmarkindices_insert)
                    db_session.commit()

                if benchmarkindex_co_code:
                    master_benchmarkindices_id = db_session.query(BenchmarkIndices.BenchmarkIndices_Id)\
                                                        .filter(BenchmarkIndices.Co_Code == benchmarkindex_co_code, BenchmarkIndices.Is_Deleted != 1).first()
                else:
                    master_benchmarkindices_id = db_session.query(BenchmarkIndices.BenchmarkIndices_Id)\
                                                        .filter(BenchmarkIndices.BenchmarkIndices_Name == benchmark_name, BenchmarkIndices.Is_Deleted != 1).first()

                if Remarks == None:
                    sql_fund = db_session.query(Fund).filter(Fund.Fund_Code == fund_id).filter(Fund.Is_Deleted != 1).one_or_none()

                    if sql_fund:
                        master_fund_id = sql_fund.Fund_Id                        
                        sql_fund.Fund_Name = scm_scheme_name
                        sql_fund.Updated_By = user_id
                        sql_fund.Updated_Date = TODAY
                        db_session.commit()
                    else:
                        sql_fund_insert = Fund()
                        sql_fund_insert.Fund_Name = scm_scheme_name
                        sql_fund_insert.Fund_Code = fund_id
                        sql_fund_insert.Fund_Description = None
                        sql_fund_insert.Fund_OfferLink = None
                        sql_fund_insert.Fund_OldName = None
                        sql_fund_insert.Is_Deleted = 0
                        sql_fund_insert.Top_Holding_ToBeShown = None
                        sql_fund_insert.Created_Date = user_id
                        sql_fund_insert.Created_Date = TODAY
                        db_session.add(sql_fund_insert)
                        db_session.commit()

                    if master_fund_id != 0:
                        sql_fund1 = db_session.query(Fund).filter(Fund.Fund_Code == fund_id).filter(Fund.Is_Deleted != 1).one_or_none()
                        master_fund_id = sql_fund1.Fund_Id

                    # Add new AssetClass if not present in the database
                    # TODO: Does not make sense as we are kicking out rows above if AssetClass is not available already in the system
                    sql_assetclass = db_session.query(AssetClass)\
                                               .filter(AssetClass.AssetClass_Name == assetclass_standard.get(scm_type_of_fundnature, scm_type_of_fundnature))\
                                               .filter(AssetClass.Is_Deleted != 1).one_or_none()

                    if not sql_assetclass and scm_type_of_fundnature != "" and scm_type_of_fundnature != "0":
                        sql_assetclass_insert = AssetClass()
                        sql_assetclass_insert.AssetClass_Name = scm_type_of_fundnature
                        sql_assetclass_insert.AssetClass_Description = None
                        sql_assetclass_insert.Is_Deleted = 0
                        sql_assetclass_insert.Created_Date = TODAY
                        sql_assetclass_insert.Updated_By = user_id
                        sql_assetclass_insert.Updated_Date = None
                        db_session.add(sql_assetclass)
                        db_session.commit()

                    master_assetclass_id = db_session.query(AssetClass.AssetClass_Id)\
                                                     .filter(AssetClass.AssetClass_Name == assetclass_standard.get(scm_type_of_fundnature, scm_type_of_fundnature))\
                                                     .filter(AssetClass.Is_Deleted != 1).one_or_none()

                    # Add new Classification if not present in the database
                    # TODO: Does not make sense as we are kicking out rows above if Classification is not available already in the system
                    sql_classification = db_session.query(Classification)\
                                                   .filter(Classification.Classification_Name == classification_standard.get(classification, classification))\
                                                   .filter(Classification.Is_Deleted != 1).one_or_none()

                    if not sql_classification and classification != "" and classification != "0":
                        sql_classification_insert = Classification()
                        sql_classification_insert.Classification_Name = classification
                        sql_classification_insert.Classification_Code = None
                        sql_classification_insert.Is_Deleted = 0
                        sql_classification_insert.Created_By = user_id
                        sql_classification_insert.Created_Date = TODAY
                        sql_classification_insert.Updated_By = None
                        sql_classification_insert.Updated_Date = None
                        db_session.add(sql_classification_insert)
                        db_session.commit()

                    master_classification_id = db_session.query(Classification.Classification_Id)\
                                                         .filter(Classification.Classification_Name == classification_standard.get(classification, classification))\
                                                         .filter(Classification.Is_Deleted != 1).one_or_none()

                    # Check the scheme status
                    master_status_id = db_session.query(Status.Status_Id).filter(Status.Status_Name == scm_status).filter(Status.Is_Deleted != 1).one_or_none()

                    sql_MFSecurity_id = db_session.query(MFSecurity).filter(MFSecurity.MF_Security_Code == fund_id).filter(MFSecurity.Is_Deleted != 1).all()

                    if scm_type_of_fundnature != "" and scm_type_of_fundnature != "0" and classification != "" and classification != "0":
                        if sql_MFSecurity_id:
                            for sql_MFSecurity in sql_MFSecurity_id:
                                sql_MFSecurity.MF_Security_Name = scm_scheme_name
                                sql_MFSecurity.Fund_Id = master_fund_id
                                sql_MFSecurity.AssetClass_Id = master_assetclass_id.AssetClass_Id
                                sql_MFSecurity.Status_Id = master_status_id.Status_Id
                                sql_MFSecurity.BenchmarkIndices_Id = master_benchmarkindices_id.BenchmarkIndices_Id
                                sql_MFSecurity.AMC_Id = master_amc_id.AMC_Id
                                sql_MFSecurity.Classification_Id = master_classification_id.Classification_Id
                                sql_MFSecurity.MF_Security_UnitFaceValue = None if scm_unit_face_value == "" else scm_unit_face_value
                                sql_MFSecurity.MF_Security_OpenDate = scm_start_date
                                sql_MFSecurity.MF_Security_CloseDate = scm_end_date
                                sql_MFSecurity.MF_Security_ReopenDate = scm_reopen_date
                                sql_MFSecurity.MF_Security_Investment_Strategy = scheme_invesyment_strategy
                                sql_MFSecurity.INS_SFINCode = sfin_code
                                sql_MFSecurity.INS_EquityMin =None if equity_min == "" else equity_min
                                sql_MFSecurity.INS_EquityMax = None if equity_max == "" else equity_max
                                sql_MFSecurity.INS_DebtMin = None if debt_min == "" else debt_min
                                sql_MFSecurity.INS_DebtMax = None if debt_max == "" else debt_max
                                sql_MFSecurity.INS_CommMin = None if comm_min == "" else comm_min
                                sql_MFSecurity.INS_CommMax = None if comm_max == "" else comm_max
                                sql_MFSecurity.INS_CashMoneyMarketMin = None if cash_money_market_min == "" else cash_money_market_min
                                sql_MFSecurity.INS_CashMoneyMarketMax = None if cash_money_market_max == "" else cash_money_market_max
                                sql_MFSecurity.INS_EquityDerivativesMin = None if equity_derivatives_min == "" else equity_derivatives_min
                                sql_MFSecurity.INS_EquityDerivativesMax = None if equity_derivatives_max == "" else equity_derivatives_max
                                sql_MFSecurity.Updated_By = user_id
                                sql_MFSecurity.Updated_Date = TODAY
                            db_session.commit()
                        else:
                            MFSecurity_insert = MFSecurity()
                            MFSecurity_insert.MF_Security_Name = scm_scheme_name
                            MFSecurity_insert.MF_Security_Code = fund_id
                            MFSecurity_insert.Fund_Id = master_fund_id
                            MFSecurity_insert.AssetClass_Id = master_assetclass_id.AssetClass_Id
                            MFSecurity_insert.Status_Id = master_status_id.Status_Id
                            MFSecurity_insert.FundType_Id = 1
                            MFSecurity_insert.BenchmarkIndices_Id = master_benchmarkindices_id.BenchmarkIndices_Id
                            MFSecurity_insert.AMC_Id = master_amc_id.AMC_Id
                            MFSecurity_insert.Classification_Id = master_classification_id.Classification_Id
                            MFSecurity_insert.MF_Security_UnitFaceValue = None if scm_unit_face_value == "" else scm_unit_face_value
                            MFSecurity_insert.MF_Security_OpenDate = scm_start_date
                            MFSecurity_insert.MF_Security_CloseDate = scm_end_date
                            MFSecurity_insert.MF_Security_ReopenDate = scm_reopen_date
                            MFSecurity_insert.MF_Security_Investment_Strategy = scheme_invesyment_strategy
                            MFSecurity.INS_SFINCode = sfin_code
                            MFSecurity.INS_EquityMin =None if equity_min == "" else equity_min
                            MFSecurity.INS_EquityMax = None if equity_max == "" else equity_max
                            MFSecurity.INS_DebtMin = None if debt_min == "" else debt_min
                            MFSecurity.INS_DebtMax = None if debt_max == "" else debt_max
                            MFSecurity.INS_CommMin = None if comm_min == "" else comm_min
                            MFSecurity.INS_CommMax = None if comm_max == "" else comm_max
                            MFSecurity.INS_CashMoneyMarketMin = None if cash_money_market_min == "" else cash_money_market_min
                            MFSecurity.INS_CashMoneyMarketMax = None if cash_money_market_max == "" else cash_money_market_max
                            MFSecurity.INS_EquityDerivativesMin = None if equity_derivatives_min == "" else equity_derivatives_min
                            MFSecurity.INS_EquityDerivativesMax = None if equity_derivatives_max == "" else equity_derivatives_max 
                            MFSecurity_insert.Is_Deleted = 0
                            MFSecurity_insert.Created_By = user_id
                            MFSecurity_insert.Created_Date = TODAY
                            db_session.add(MFSecurity_insert)
                            db_session.commit()

                        sql_MFSecurity_id = db_session.query(MFSecurity)\
                                                      .filter(MFSecurity.MF_Security_Code == fund_id)\
                                                      .filter(MFSecurity.Is_Deleted != 1).all()

                        sql_plan_id = db_session.query(Plans)\
                                                .filter(Plans.Plan_Code == get_plan_code(fund_id, PRODUCT_ID))\
                                                .filter(Plans.Is_Deleted != 1).order_by(desc(Plans.Plan_Id)).first()

                        if sql_plan_id:
                            sql_plan_id.Plan_Name = scm_scheme_name
                            sql_plan_id.Plan_Code = get_plan_code(fund_id, PRODUCT_ID)
                            sql_plan_id.MF_Security_Id = sql_MFSecurity_id[0].MF_Security_Id
                            sql_plan_id.PlanType_Id = 1
                            sql_plan_id.Option_Id = 2
                            sql_plan_id.Is_Deleted = 0
                            sql_plan_id.Updated_By = user_id
                            sql_plan_id.Updated_Date = TODAY
                            db_session.commit()
                            Remarks = "Strategy Updated Successfully."
                        else:
                            Plans_insert = Plans()
                            Plans_insert.Plan_Name = scm_scheme_name
                            Plans_insert.Plan_Code = get_plan_code(fund_id, PRODUCT_ID)
                            Plans_insert.MF_Security_Id = sql_MFSecurity_id[0].MF_Security_Id
                            Plans_insert.PlanType_Id = 1
                            Plans_insert.Option_Id = 2
                            Plans_insert.SwitchAllowed_Id = None
                            Plans_insert.Is_Deleted = 0
                            Plans_insert.Created_By = user_id
                            Plans_insert.Created_Date = TODAY
                            db_session.add(Plans_insert)
                            db_session.commit()

                            sql_plan_id = db_session.query(Plans.Plan_Id)\
                                                    .filter(Plans.Plan_Code == get_plan_code(fund_id, PRODUCT_ID))\
                                                    .filter(Plans.Is_Deleted != 1).one_or_none()

                            save_planproductmapping(db_session, sql_plan_id.Plan_Id, PRODUCT_ID, user_id, TODAY)

                            Remarks = "Strategy Uploaded Successfully."
            except Exception as ex:
                ex_record = {}
                ex_record['CMOTS_FundID'] = row['FundID']
                ex_record['CMOTS_SFINCode'] = row['SFINCode']
                ex_record['CMOTS_SchemeName'] = row["SCM_SCHEME_NAME"]
                ex_record['Remarks'] = Remarks
                ex_record['Exception'] = str(ex)
                exceptions_data.append(ex_record)
                print('Exception Recorded!!!') # Remove print statement

                
                db_session.rollback()
                db_session.flush()
                continue

            item = row.copy()
            item["Remarks"] = Remarks
            items.append(item)
            print(Remarks) # Remove print statement

        if exceptions_data:
            exception_file_path = get_rel_path(output_file_path, __file__).lower().replace('schememasterupdate_insurance', 'schememasterupdate_insurance_exception')
            exception_file_path = exception_file_path.replace('/',r'\\')
            # check whether the specified path exists or not
            read_folder = '\\'.join(get_rel_path(output_file_path, __file__).split('/')[:-1])
            isExist = os.path.exists(read_folder)
            if not isExist:
                # create a new directory because it does not exist
                try:
                    os.makedirs(read_folder)
                except Exception as ex:
                    pass # File could not be created. Moving on....

            df_exception = pd.DataFrame(exceptions_data)
            df_exception.to_csv(exception_file_path)

            attachements = list()
            attachements.append(exception_file_path)
            schedule_email_activity(db_session, 'devteam@finalyca.com', '', '', F"ULIP - FundMaster - Exception file{TODAY.strftime('%Y-%b-%d')}", F"ULIP - FundMaster - Exception file{TODAY.strftime('%Y-%b-%d')}", attachements)

    write_csv(output_file_path, header, items, __file__)            
            

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/cmots/31032023_ulip/SchemeMasterUpdate_Insurance.csv"
    READ_PATH = "read/cmots/31032023_ulip/SchemeMasterUpdate_Insurance.csv"
    import_ulip_fund_master_file(FILE_PATH, READ_PATH, USER_ID)