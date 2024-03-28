import csv
from datetime import datetime as dt
from sqlalchemy import and_, func
import os
from bizlogic.importer_helper import save_planproductmapping, get_rel_path, write_csv

from utils import *
from utils.finalyca_store import *
from fin_models import *

def get_schemecode(schemecode, product_id):
    schemecode1 = ""
    if product_id == 4:
        schemecode1 = F"{schemecode}_01"
    else:
        schemecode1 = schemecode
    return schemecode1

def import_pms_fundmaster_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))
    header = [ "SCM_AMC_CD", "SCM_SCHEME_CD", "SCM_SCHEME_NAME", "Classification", "BENCHMARK NAME", "SCM_START_DATE", "SCM_MIN_PURCHASE_AMT", "SCHEME INVESTMENT STRATEGY", "Fee_Structure", "Remarks"]
    items = list()
    PRODUCT_ID = 4
    TODAY = dt.today()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            scm_start_date = None
            master_fund_id = 0
            master_classification_id = 0

            scm_amc_id = row["SCM_AMC_CD"]  
            scm_scheme_cd = row["SCM_SCHEME_CD"]
            scm_scheme_name = row["SCM_SCHEME_NAME"]
            classification = row["Classification"]
            benchmarkname = row["BENCHMARK NAME"]
            scm_start_date1 = row["SCM_START_DATE"]
            if row["SCM_START_DATE"] != "":
                scm_start_date = dt.strptime(row["SCM_START_DATE"], '%d-%b-%y') 

            scm_min_purchase_amt = row["SCM_MIN_PURCHASE_AMT"]
            scheme_investment_strategy = row["SCHEME INVESTMENT STRATEGY"]
            fees_structure = row["Fee_Structure"]
            Remarks = None

            master_amc_id = db_session.query(AMC.AMC_Id).filter(AMC.AMC_Code == scm_amc_id).filter(AMC.Is_Deleted != 1).first()

            if not master_amc_id:
                Remarks = "AMC not available, please add AMC first and upload again."

            sql_benchmarkindices_Id = db_session.query(BenchmarkIndices.BenchmarkIndices_Id).filter(BenchmarkIndices.BenchmarkIndices_Name == benchmarkname).filter(BenchmarkIndices.Is_Deleted != 1).first()

            if not sql_benchmarkindices_Id:
                Remarks = "Benchmark Index is not available, please input Benchmark properly and upload again."

            if Remarks == None:
                sql_fund_id = db_session.query(Fund).filter(Fund.Fund_Code == scm_scheme_cd).filter(Fund.Is_Deleted != 1).order_by(Fund.Fund_Id.desc()).one_or_none()

                if sql_fund_id:
                    master_fund_id = sql_fund_id.Fund_Id
                    
                    sql_fund_id.Fund_Name = scm_scheme_name
                    sql_fund_id.Updated_By = user_id
                    sql_fund_id.Updated_Date = TODAY
                    
                    db_session.commit()
                else:
                    sql_fund_insert = Fund()
                    sql_fund_insert.Fund_Name = scm_scheme_cd
                    sql_fund_insert.Fund_Code = scm_scheme_cd
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
                    sql_fund1 = db_session.query(Fund).filter(Fund.Fund_Code == scm_scheme_cd).filter(Fund.Is_Deleted != 1).one_or_none()
                    master_fund_id = sql_fund1.Fund_Id

                sql_classification = db_session.query(Classification).filter(Classification.Classification_Name == classification).filter(Classification.Is_Deleted != 1).one_or_none()

                if not sql_classification:
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

                master_classification_id = db_session.query(Classification.Classification_Id).filter(Classification.Classification_Name == classification).filter(Classification.Is_Deleted != 1).one_or_none()

                sql_MFSecurity_id = db_session.query(MFSecurity).filter(MFSecurity.MF_Security_Code == scm_scheme_cd).filter(MFSecurity.Is_Deleted != 1).all()

                if sql_MFSecurity_id:
                    for sql_MFSecurity in sql_MFSecurity_id:
                        sql_MFSecurity.MF_Security_Name = scm_scheme_name
                        sql_MFSecurity.Fund_Id = master_fund_id
                        sql_MFSecurity.AssetClass_Id = 1
                        sql_MFSecurity.Status_Id = 1
                        sql_MFSecurity.BenchmarkIndices_Id = sql_benchmarkindices_Id.BenchmarkIndices_Id
                        sql_MFSecurity.AMC_Id = master_amc_id.AMC_Id
                        sql_MFSecurity.Classification_Id = master_classification_id.Classification_Id
                        sql_MFSecurity.MF_Security_OpenDate = scm_start_date
                        sql_MFSecurity.MF_Security_Min_Purchase_Amount = None if scm_min_purchase_amt == "" else scm_min_purchase_amt
                        sql_MFSecurity.MF_Security_Investment_Strategy = scheme_investment_strategy
                        sql_MFSecurity.Fees_Structure = fees_structure
                        sql_MFSecurity.Updated_By = user_id
                        sql_MFSecurity.Updated_Date = TODAY
                    
                    db_session.commit()
                else:
                    MFSecurity_insert = MFSecurity()
                    MFSecurity_insert.MF_Security_Name = scm_scheme_name
                    MFSecurity_insert.MF_Security_Code = scm_scheme_cd
                    MFSecurity_insert.Fund_Id = master_fund_id
                    MFSecurity_insert.AssetClass_Id = 1
                    MFSecurity_insert.Status_Id = 1
                    MFSecurity_insert.FundType_Id = 1
                    MFSecurity_insert.BenchmarkIndices_Id = sql_benchmarkindices_Id.BenchmarkIndices_Id
                    MFSecurity_insert.AMC_Id = master_amc_id.AMC_Id
                    MFSecurity_insert.Classification_Id = master_classification_id.Classification_Id                
                    MFSecurity_insert.MF_Security_OpenDate = scm_start_date
                    MFSecurity_insert.MF_Security_Min_Purchase_Amount = None if scm_min_purchase_amt == "" else scm_min_purchase_amt
                    MFSecurity_insert.MF_Security_Investment_Strategy = scheme_investment_strategy
                    MFSecurity_insert.Is_Deleted = 0
                    MFSecurity_insert.Fees_Structure = fees_structure
                    MFSecurity_insert.Created_By = user_id
                    MFSecurity_insert.Created_Date = TODAY

                    db_session.add(MFSecurity_insert)
                    db_session.commit()

                    sql_MFSecurity_id = db_session.query(MFSecurity.MF_Security_Id).filter(MFSecurity.MF_Security_Code == scm_scheme_cd).filter(MFSecurity.Is_Deleted != 1).one_or_none()

                sql_plan_id = db_session.query(Plans).filter(Plans.Plan_Code == get_schemecode(scm_scheme_cd,PRODUCT_ID)).filter(Plans.Is_Deleted != 1).one_or_none()

                if sql_plan_id:
                
                    sql_plan_id.Plan_Name = scm_scheme_name,
                    sql_plan_id.Plan_Code = get_schemecode(scm_scheme_cd, PRODUCT_ID),
                    sql_plan_id.MF_Security_Id = sql_MFSecurity_id.MF_Security_Id,
                    sql_plan_id.PlanType_Id = 1,
                    sql_plan_id.Option_Id = 2,
                    sql_plan_id.SwitchAllowed_Id = None,                    
                    sql_plan_id.Updated_By = user_id,
                    sql_plan_id.Updated_Date = TODAY
                    
                    db_session.commit()
                    Remarks = "Strategy Updated successfully."

                else:
                    Plans_insert = Plans()
                    Plans_insert.Plan_Name = scm_scheme_name
                    Plans_insert.Plan_Code = get_schemecode(scm_scheme_cd, PRODUCT_ID)
                    Plans_insert.MF_Security_Id = sql_MFSecurity_id.MF_Security_Id
                    Plans_insert.PlanType_Id = 1
                    Plans_insert.Option_Id = 2
                    Plans_insert.Is_Deleted = 0
                    Plans_insert.Created_By = user_id
                    Plans_insert.Created_Date = TODAY

                    db_session.add(Plans_insert)
                    db_session.commit()

                    sql_plan_id = db_session.query(Plans.Plan_Id).filter(Plans.Plan_Code == get_schemecode(scm_scheme_cd, PRODUCT_ID)).filter(Plans.Is_Deleted != 1).one_or_none()
                    
                    save_planproductmapping(db_session, sql_plan_id.Plan_Id, PRODUCT_ID, user_id, TODAY)
                    
                    Remarks = "Strategy uploaded successfully."
            item = row.copy()
            item["Remarks"] = Remarks
            items.append(item)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/PMS - Fund Master_300.csv"
    READ_PATH = "read/PMS - Fund Master_300.csv"
    import_pms_fundmaster_file(FILE_PATH, READ_PATH, USER_ID)