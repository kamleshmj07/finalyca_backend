import csv
from datetime import datetime as dt
import os
import pandas as pd
from bizlogic.importer_helper import save_assetclass, save_classification, get_rel_path, write_csv

from utils import *
from utils.finalyca_store import *
from fin_models import *
from bizlogic.common_helper import schedule_email_activity

def import_mf_fundmaster_file(input_file_path, output_file_path, user_id):
    header = [ "SCM_AMC_CD", "FundID", "SchemeCode", "SCM_SCHEME_CD","SCM_SCHEME_NAME", "SCM_UNIT_FACE_VALUE", "SCM_TYPE_OF_FUND/NATURE", "SCM_SCHEME_OPTION", "SCM_DIV_REINV_OPTION", "SCM_START_DATE", "SCM_END_DATE", "SCH_REOPEN_DATE", "SCM_PUR_AVAILABLE", "SCM_RED_AVAILABLE", "SCM_SIP_AVAILABLE", "SCM_MIN_PURCHASE_AMT", "SCM_PUR_MULTIPLES_AMT", "SCM_ADD_MIN_PURCHASE_AMT", "SCM_ADD_PUR_MULTIPLES_AMT", "SCM_MIN_REDEEM_AMT", "SCM_MIN_REDEEM_UNITS", "SCM_TRXN_CUT_OFF_TIME", "SCM_SIP_FREQ", "SCM_SIP_DATES", "SCM_SIP_MIN_AMT", "SCM_SIP_MIN_AGG_AMT", "SCM_SWITCH IN_AVAILABLE", "SCM_SWITCH OUT_AVAILABLE", "SCM_OFFER_LINK", "SCM_STATUS", "SCM_CLOSE_OPEN_ENDED", "SCM_ELSS", "BENCHMARK_NAME", "OLD_SCHEME_NAME", "AMFI_CODE", "MATURITY_DATE", "RTA_CODE", "RTA_NAME", "AMFI SCHEME NAMES", "MINIMUM_SCM_BALANCE_UNIT", "MATURITY_PERIOD", "MINIMUM_LOCKIN_PERIOD", "EXTERNAL_MAP_CODE", "MutualFund", "FundName", "Classification", "ISIN1", "ETF_Flag", "SCHEME_INVESTMENT_STRATEGY", "FUND_MANAGER_NAME", "BRIEF_OF_FUND_MANAGER", "3x3_RISK_RETURN_MATRIX", "SIP_MIN_Installment", "SCM_STP_AVAILABLE", "SCM_STP_FREQ", "SCM_STP_MIN_INSTALL", "SCM_STP_DATES", "SCM_STP_MIN_AMT", "SCM_SWP_AVAILABLE", "SCM_SWP_FREQ", "SCM_SWP_MIN_INSTALL", "SCM_SWP_DATES", "SCM_SWP_MIN_AMT", "Demat", "Plan_Type", "RTAAMCCode", "ISIN2", "BnchmarkIndex_Co_Code", "ExitLoad", "RiskGrade", "SchemeType", "Remarks"]
    items = list()
    exceptions_data = []

    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))
    
    PRODUCT_ID = 1
    TODAY = dt.today()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f, delimiter="|")
        for row in csvreader:
            try:
                row = {key.replace(',', ''):val for key, val in row.items()} #remove comma from dict key
                mf_security_startdate = None
                mf_security_enddate = None
                mf_security_reopendate = None
                mf_security_maturitydate = None

                scm_amc_id = row["SCM_AMC_CD"]  
                fund_id = row["FundID"]
                schemecode =  row["SchemeCode"]
                if schemecode == "":
                    raise Exception("SchemeCode is empty.")

                scm_scheme_cd = row["SCM_SCHEME_CD"]
                scm_scheme_name = row["SCM_SCHEME_NAME"]
                scm_unit_face_value = row["SCM_UNIT_FACE_VALUE"]
                scm_type_of_fund_nature = row["SCM_TYPE_OF_FUND/NATURE"].replace('/', ':').replace(' Fund', '') if row["SCM_TYPE_OF_FUND/NATURE"] != None else "" # In CMOTS version, this provides the classification for Master.Classification
                scm_scheme_option = row["SCM_SCHEME_OPTION"]
                scm_div_reinv_option = row["SCM_DIV_REINV_OPTION"]
                if row["SCM_START_DATE"] != "":
                    mf_security_startdate = dt.strptime(row["SCM_START_DATE"].replace("/","-"), '%d-%m-%Y')

                if row["SCM_END_DATE"] != "":
                    mf_security_enddate = dt.strptime(row["SCM_END_DATE"].replace("/","-"), '%d-%m-%Y')

                if row["SCH_REOPEN_DATE"] != "":
                    mf_security_reopendate = dt.strptime(row["SCH_REOPEN_DATE"].replace("/","-"), '%d-%m-%Y')

                if row["MATURITY_DATE"] != "":
                    mf_security_maturitydate = dt.strptime(row["MATURITY_DATE"].replace("/","-"), '%d-%m-%Y')
                
                scm_pur_available = row["SCM_PUR_AVAILABLE"]  
                scm_red_available = row["SCM_RED_AVAILABLE"]  
                scm_sip_available = row["SCM_SIP_AVAILABLE"]  
                scm_min_purchase_amt = row["SCM_MIN_PURCHASE_AMT"]  
                scm_pur_multiples_amt = row["SCM_PUR_MULTIPLES_AMT"]
                scm_add_min_purchase_amt = row["SCM_ADD_MIN_PURCHASE_AMT"]
                scm_add_pur_multiples_amt = row["SCM_ADD_PUR_MULTIPLES_AMT"]
                scm_min_redeem_amt = row["SCM_MIN_REDEEM_AMT"]  
                scm__min_redeem_units = row["SCM_MIN_REDEEM_UNITS"]
                scm_trxn_cut_off_time = row["SCM_TRXN_CUT_OFF_TIME"]
                scm_sip_freq = row["SCM_SIP_FREQ"]
                scm_sip_dates = row["SCM_SIP_DATES"]
                scm_sip_min_amt = row["SCM_SIP_MIN_AMT"]
                scm_sip_min_agg_amt = row["SCM_SIP_MIN_AGG_AMT"]
                scm_offer_link = row["SCM_OFFER_LINK"]
                scm_status = 'Active' # row["SCM_STATUS"]
                scm_benchmark_name = row["BENCHMARK_NAME"]
                amfi_code = row["AMFI_CODE"]
                rta_code = row["RTA_CODE"]
                rta_name = row["RTA_NAME"]
                amfi_scheme_names = row["AMFI SCHEME NAMES"]
                minimum_scm_balance_unit = row["MINIMUM_SCM_BALANCE_UNIT"]
                maturity_period = row["MATURITY_PERIOD"]
                minimum_lockin_period = row["MINIMUM_LOCKIN_PERIOD"]
                external_map_code = row["EXTERNAL_MAP_CODE"]
                fund_name = row["FundName"]
                classification = row["Classification"] # In CMOTS version, this provides the assetclass for Master.AssetClass
                isin1 = row["ISIN1"]
                scheme_investment_strategy = row["SCHEME_INVESTMENT_STRATEGY"]
                # TODO Add logic to capture the fund manager names
                fund_manager_name = row["FUND_MANAGER_NAME"]
                brief_of_fund_manager = row["BRIEF_OF_FUND_MANAGER"]
                sip_min_installment = row["SIP_MIN_Installment"]
                scm_stp_available = row["SCM_STP_AVAILABLE"]
                scm_stp_freq = row["SCM_STP_FREQ"]
                scm_stp_min_install = row["SCM_STP_MIN_INSTALL"]
                scm_stp_dates = row["SCM_STP_DATES"]
                scm_stp_min_amt = row["SCM_STP_MIN_AMT"]
                scm_swp_available = row["SCM_SWP_AVAILABLE"]
                scm_swp_freq = row["SCM_SWP_FREQ"]
                scm_swp_min_install = row["SCM_SWP_MIN_INSTALL"]
                scm_swp_dates = row["SCM_SWP_DATES"]
                scm_swp_min_amt = row["SCM_SWP_MIN_AMT"]
                demat = row["Demat"]
                plan_type = row["Plan_Type"]
                rta_amc_code = row["RTAAMCCode"]
                isin2 = row["ISIN2"]
                benchmarkindex_co_code = row["BnchmarkIndex_Co_Code"] if row["BnchmarkIndex_Co_Code"] != '0' else None
                schemetype = 'Open Ended' if row["SchemeType"] == 'OPEN' else 'Close Ended' if row["SchemeType"] == 'CLOSE' else 'Interval'
                exit_load = F'Exid Load: {row["ExitLoad"]}' if row["ExitLoad"] else None #in case of MF we will use fees_structure column to store exit load as MF will not have fees structure
                risk_grade = row["RiskGrade"] if row["RiskGrade"] else None
                Remarks = None

                master_fund_id = 0
                master_assetclass_id = 0
                master_classification_id = 0
                master_benchmarkindices_id = 0
                master_plantype_id = 0
                master_options_id = 0
                master_amc = 0

                master_amc = db_session.query(AMC).filter(AMC.AMC_Code == scm_amc_id).filter(AMC.Is_Deleted != 1).one_or_none()

                if not master_amc:
                    Remarks = "AMC not available, please add AMC first and upload again."

                if Remarks == None:
                    sql_fund = db_session.query(Fund).filter(Fund.Fund_Code == fund_id).filter(Fund.Is_Deleted != 1).one_or_none()

                    if sql_fund:
                        master_fund_id = sql_fund.Fund_Id           
                        sql_fund.Fund_Name = fund_name
                        sql_fund.Fund_OfferLink = scm_offer_link
                        sql_fund.Updated_By = user_id
                        sql_fund.Updated_Date = TODAY
                        sql_fund.Fund_manager = fund_manager_name
                    
                        db_session.commit()
                    else:
                        sql_fund_insert = Fund()
                        sql_fund_insert.Fund_Name = fund_name
                        sql_fund_insert.Fund_Code = fund_id
                        sql_fund_insert.Fund_Description = None
                        sql_fund_insert.Fund_OfferLink = scm_offer_link
                        sql_fund_insert.Fund_OldName = None
                        sql_fund_insert.Is_Deleted = 0
                        sql_fund_insert.Top_Holding_ToBeShown = None
                        sql_fund_insert.Fund_manager = fund_manager_name
                        sql_fund_insert.Created_Date = user_id
                        sql_fund_insert.Created_Date = TODAY

                        db_session.add(sql_fund_insert)
                        db_session.commit()

                    if master_fund_id != 0:
                        sql_fund1 = db_session.query(Fund).filter(Fund.Fund_Code == fund_id).filter(Fund.Is_Deleted != 1).one_or_none()
                        master_fund_id = sql_fund1.Fund_Id
                    
                    save_assetclass(db_session, classification, None, user_id, TODAY)

                    master_assetclass_id = db_session.query(AssetClass.AssetClass_Id).filter(AssetClass.AssetClass_Name == classification).filter(AssetClass.Is_Deleted != 1).scalar()

                    save_classification(db_session, scm_type_of_fund_nature, None, user_id, TODAY, master_assetclass_id)

                    master_classification_id = db_session.query(Classification.Classification_Id).filter(Classification.Classification_Name == scm_type_of_fund_nature).filter(Classification.Is_Deleted != 1).scalar()
                    
                    if benchmarkindex_co_code:
                        sql_benchmarkindices = db_session.query(BenchmarkIndices).filter(BenchmarkIndices.Co_Code == benchmarkindex_co_code).filter(BenchmarkIndices.Is_Deleted != 1).first()
                    else:
                        sql_benchmarkindices = db_session.query(BenchmarkIndices).filter(BenchmarkIndices.BenchmarkIndices_Name == scm_benchmark_name).filter(BenchmarkIndices.Is_Deleted != 1).first()
                        

                    if not sql_benchmarkindices and scm_benchmark_name != "0":
                        sql_benchmarkindices_insert = BenchmarkIndices()
                        sql_benchmarkindices_insert.BenchmarkIndices_Name = scm_benchmark_name
                        sql_benchmarkindices_insert.BenchmarkIndices_Code = None
                        sql_benchmarkindices_insert.BenchmarkIndices_Description = scm_benchmark_name
                        sql_benchmarkindices_insert.Co_Code = benchmarkindex_co_code
                        sql_benchmarkindices_insert.Is_Deleted = 0
                        sql_benchmarkindices_insert.Created_By = user_id
                        sql_benchmarkindices_insert.Created_Date = TODAY
                        sql_benchmarkindices_insert.Updated_By = None
                        sql_benchmarkindices_insert.Updated_Date = None

                        db_session.add(sql_benchmarkindices_insert)
                        db_session.commit()

                    if benchmarkindex_co_code:
                        master_benchmarkindices_id = db_session.query(BenchmarkIndices.BenchmarkIndices_Id).filter(BenchmarkIndices.Co_Code == benchmarkindex_co_code).filter(BenchmarkIndices.Is_Deleted != 1).scalar()
                    else:
                        master_benchmarkindices_id = db_session.query(BenchmarkIndices.BenchmarkIndices_Id).filter(BenchmarkIndices.BenchmarkIndices_Name == scm_benchmark_name).filter(BenchmarkIndices.Is_Deleted != 1).scalar()
                    
                    if not master_benchmarkindices_id:
                        raise Exception('Benchmark Co-Code and/or Benchmark Name is missing for the scheme.')

                    sql_plantype = db_session.query(PlanType).filter(PlanType.PlanType_Name == plan_type).filter(PlanType.Is_Deleted != 1).one_or_none()

                    if not sql_plantype:
                        if plan_type != '0' and plan_type != None:
                            sql_plantype_insert = PlanType()
                            sql_plantype_insert.PlanType_Name = plan_type
                            sql_plantype_insert.PlanType_Code = None
                            sql_plantype_insert.Is_Deleted = 0

                            db_session.add(sql_plantype_insert)
                            db_session.commit()

                            master_plantype_id = db_session.query(PlanType.PlanType_Id).filter(PlanType.PlanType_Name == plan_type).filter(PlanType.Is_Deleted != 1).scalar()

                            if master_plantype_id == 0 or master_plantype_id == None:
                                raise Exception("Plan_Type is none or empty. Please review and re-upload.")
                    else:
                        master_plantype_id = sql_plantype.PlanType_Id

                    if not (scm_scheme_option == "" or scm_scheme_option == None):
                        sql_options = db_session.query(Options).filter(Options.Option_Name == scm_scheme_option).filter(Options.Is_Deleted != 1).one_or_none()
                
                        if not sql_options:
                            sql_options_insert = Options()
                            sql_options_insert.Option_Name = scm_scheme_option
                            sql_options_insert.Option_Code = scm_scheme_option
                            sql_options_insert.Is_Deleted = 0
                            sql_options_insert.Created_By = user_id
                            sql_options_insert.Created_Date = TODAY
                            sql_options_insert.Updated_By = None
                            sql_options_insert.Updated_Date = None

                            db_session.add(sql_options_insert)
                            db_session.commit()
                    else:
                        raise Exception("SCM_SCHEME_OPTION is none or empty. Please review and re-upload.")
		    
                    master_options_id = db_session.query(Options.Option_Id).filter(Options.Option_Name == scm_scheme_option).filter(Options.Is_Deleted != 1).scalar()

                    master_status_id = db_session.query(Status.Status_Id).filter(Status.Status_Name == scm_status).filter(Status.Is_Deleted != 1).one_or_none()

                    sql_MFSecurity_id = db_session.query(MFSecurity).filter(MFSecurity.MF_Security_Code == schemecode).filter(MFSecurity.Is_Deleted != 1).all()

                    master_fundtype_id = db_session.query(FundType.FundType_Id).filter(FundType.FundType_Name == schemetype).filter(FundType.Is_Deleted != 1).scalar()

                    if sql_MFSecurity_id:
                        for sql_MFSecurity in sql_MFSecurity_id:
                            sql_MFSecurity.MF_Security_Name = fund_name
                            sql_MFSecurity.Fund_Id = master_fund_id
                            sql_MFSecurity.AssetClass_Id = master_assetclass_id
                            sql_MFSecurity.Status_Id = master_status_id.Status_Id
                            sql_MFSecurity.BenchmarkIndices_Id = master_benchmarkindices_id
                            sql_MFSecurity.AMC_Id = master_amc.AMC_Id
                            sql_MFSecurity.Classification_Id = master_classification_id
                            sql_MFSecurity.MF_Security_UnitFaceValue = None if scm_unit_face_value == "" else scm_unit_face_value
                            sql_MFSecurity.MF_Security_OpenDate = mf_security_startdate
                            sql_MFSecurity.MF_Security_CloseDate = mf_security_enddate
                            sql_MFSecurity.MF_Security_ReopenDate = mf_security_reopendate
                            sql_MFSecurity.MF_Security_PurchaseAvailable = 1 if scm_pur_available == "Yes" else 0
                            sql_MFSecurity.MF_Security_Redemption_Available =  1 if scm_red_available == "Yes" else 0
                            sql_MFSecurity.MF_Security_SIP_Available = 1 if scm_sip_available == "Yes" else 0
                            sql_MFSecurity.MF_Security_Min_Purchase_Amount = None if scm_min_purchase_amt == "" else scm_min_purchase_amt
                            sql_MFSecurity.MF_Security_Purchase_Multiplies_Amount = None if scm_pur_multiples_amt == "" else scm_pur_multiples_amt
                            sql_MFSecurity.MF_Security_Add_Min_Purchase_Amount = None if scm_add_min_purchase_amt == "" else scm_add_min_purchase_amt
                            sql_MFSecurity.MF_Security_Add_Purchase_Multiplies_Amount = None if scm_add_pur_multiples_amt == "" else scm_add_pur_multiples_amt
                            sql_MFSecurity.MF_Security_Min_Redeem_Amount = None if scm_min_redeem_amt == "" else scm_min_redeem_amt
                            sql_MFSecurity.MF_Security_Min_Redeem_Units = None if scm__min_redeem_units == "" else scm__min_redeem_units
                            sql_MFSecurity.MF_Security_Trxn_Cut_Off_Time = scm_trxn_cut_off_time[0:8]
                            sql_MFSecurity.MF_Security_SIP_Frequency = scm_sip_freq
                            sql_MFSecurity.MF_SIP_Dates = scm_sip_dates
                            sql_MFSecurity.MF_Security_SIP_Min_Amount = scm_sip_min_amt 
                            sql_MFSecurity.MF_Security_SIP_Min_Agg_Amount = None if scm_sip_min_agg_amt == "" else scm_sip_min_agg_amt
                            sql_MFSecurity.MF_Security_Maturity_Date = mf_security_maturitydate
                            sql_MFSecurity.MF_Security_Min_Balance_Unit = None if minimum_scm_balance_unit == "" else minimum_scm_balance_unit
                            sql_MFSecurity.MF_Security_Maturity_Period = maturity_period
                            sql_MFSecurity.MF_Security_Min_Lockin_Period = minimum_lockin_period
                            sql_MFSecurity.MF_Security_Investment_Strategy = scheme_investment_strategy
                            sql_MFSecurity.MF_Security_SIP_Min_Installment = sip_min_installment
                            sql_MFSecurity.MF_Security_STP_Available = 1 if scm_stp_available == "Yes" else 0
                            sql_MFSecurity.MF_Security_STP_Frequency = scm_stp_freq                    
                            sql_MFSecurity.MF_Security_STP_Min_Install = scm_stp_min_install
                            sql_MFSecurity.MF_Security_STP_Dates = scm_stp_dates
                            sql_MFSecurity.MF_Security_STP_Min_Amount = scm_stp_min_amt
                            sql_MFSecurity.MF_Security_SWP_Available = 1 if scm_swp_available == "Yes" else 0
                            sql_MFSecurity.MF_Security_SWP_Frequency = scm_swp_freq
                            sql_MFSecurity.MF_Security_SWP_Min_Install = scm_swp_min_install
                            sql_MFSecurity.MF_Security_SWP_Dates = scm_swp_dates
                            sql_MFSecurity.MF_Security_SWP_Min_Amount = scm_swp_min_amt
                            sql_MFSecurity.FundType_Id = master_fundtype_id
                            sql_MFSecurity.Fees_Structure = exit_load
                            sql_MFSecurity.Risk_Grade = risk_grade
                            sql_MFSecurity.Updated_By = user_id
                            sql_MFSecurity.Updated_Date = TODAY
                        
                        db_session.commit()

                    else:
                        MFSecurity_insert = MFSecurity()
                        MFSecurity_insert.MF_Security_Name = fund_name
                        MFSecurity_insert.MF_Security_Code = schemecode
                        MFSecurity_insert.Fund_Id = master_fund_id
                        MFSecurity_insert.AssetClass_Id = master_assetclass_id
                        MFSecurity_insert.Status_Id = master_status_id.Status_Id
                        MFSecurity_insert.FundType_Id = master_fundtype_id
                        MFSecurity_insert.BenchmarkIndices_Id = master_benchmarkindices_id
                        MFSecurity_insert.AMC_Id = master_amc.AMC_Id
                        MFSecurity_insert.Classification_Id = master_classification_id
                        MFSecurity_insert.MF_Security_UnitFaceValue = None if scm_unit_face_value == "" else scm_unit_face_value
                        MFSecurity_insert.MF_Security_OpenDate = mf_security_startdate
                        MFSecurity_insert.MF_Security_CloseDate = mf_security_enddate
                        MFSecurity_insert.MF_Security_ReopenDate = mf_security_reopendate
                        MFSecurity_insert.MF_Security_PurchaseAvailable = 1 if scm_pur_available == "Yes" else 0
                        MFSecurity_insert.MF_Security_Redemption_Available = 1 if scm_red_available == "Yes" else 0
                        MFSecurity_insert.MF_Security_SIP_Available = 1 if scm_sip_available == "Yes" else 0
                        MFSecurity_insert.MF_Security_Min_Purchase_Amount = None if scm_min_purchase_amt == "" else scm_min_purchase_amt
                        MFSecurity_insert.MF_Security_Purchase_Multiplies_Amount = None if scm_pur_multiples_amt == "" else scm_pur_multiples_amt
                        MFSecurity_insert.MF_Security_Add_Min_Purchase_Amount = None if scm_add_min_purchase_amt == "" else scm_add_min_purchase_amt
                        MFSecurity_insert.MF_Security_Add_Purchase_Multiplies_Amount = None if scm_add_pur_multiples_amt == "" else scm_add_pur_multiples_amt
                        MFSecurity_insert.MF_Security_Min_Redeem_Amount = None if scm_min_redeem_amt == "" else scm_min_redeem_amt
                        MFSecurity_insert.MF_Security_Min_Redeem_Units = None if scm__min_redeem_units == "" else scm__min_redeem_units
                        MFSecurity_insert.MF_Security_Trxn_Cut_Off_Time = scm_trxn_cut_off_time[0:8]
                        MFSecurity_insert.MF_Security_SIP_Frequency = scm_sip_freq
                        MFSecurity_insert.MF_SIP_Dates = scm_sip_dates
                        MFSecurity_insert.MF_Security_SIP_Min_Amount = scm_sip_min_amt 
                        MFSecurity_insert.MF_Security_SIP_Min_Agg_Amount = None if scm_sip_min_agg_amt == "" else scm_sip_min_agg_amt
                        MFSecurity_insert.MF_Security_Maturity_Date = mf_security_maturitydate
                        MFSecurity_insert.MF_Security_Min_Balance_Unit = None if minimum_scm_balance_unit == "" else minimum_scm_balance_unit
                        MFSecurity_insert.MF_Security_Maturity_Period = maturity_period
                        MFSecurity_insert.MF_Security_Min_Lockin_Period = minimum_lockin_period
                        MFSecurity_insert.MF_Security_Investment_Strategy = scheme_investment_strategy
                        MFSecurity_insert.MF_Security_SIP_Min_Installment = sip_min_installment
                        MFSecurity_insert.MF_Security_STP_Available = 1 if scm_stp_available == "Yes" else 0
                        MFSecurity_insert.MF_Security_STP_Frequency = scm_stp_freq
                        MFSecurity_insert.MF_Security_STP_Min_Install = scm_stp_min_install
                        MFSecurity_insert.MF_Security_STP_Dates = scm_stp_dates
                        MFSecurity_insert.MF_Security_STP_Min_Amount = scm_stp_min_amt
                        MFSecurity_insert.MF_Security_SWP_Available = 1 if scm_swp_available == "Yes" else 0
                        MFSecurity_insert.MF_Security_SWP_Frequency = scm_swp_freq
                        MFSecurity_insert.MF_Security_SWP_Min_Install = scm_swp_min_install
                        MFSecurity_insert.MF_Security_SWP_Dates = scm_swp_dates
                        MFSecurity_insert.MF_Security_SWP_Min_Amount = scm_swp_min_amt
                        MFSecurity_insert.Is_Deleted = 0
                        MFSecurity_insert.Fees_Structure = exit_load
                        MFSecurity_insert.Risk_Grade = risk_grade
                        MFSecurity_insert.Created_By = user_id
                        MFSecurity_insert.Created_Date = TODAY
                        # db_session.print_query(MFSecurity_insert)
                        db_session.add(MFSecurity_insert)
                        db_session.commit()

                        sql_MFSecurity_id = db_session.query(MFSecurity).filter(MFSecurity.MF_Security_Code == schemecode).filter(MFSecurity.Is_Deleted != 1).all()

                    sql_plan_id = db_session.query(Plans).filter(Plans.Plan_Code == scm_scheme_cd).filter(Plans.Is_Deleted != 1).all()

                    if sql_plan_id:
                        for sql_plan in sql_plan_id:
                            sql_plan.Plan_Name = scm_scheme_name
                            sql_plan.Plan_Code = scm_scheme_cd
                            sql_plan.MF_Security_Id = sql_MFSecurity_id[0].MF_Security_Id
                            sql_plan.PlanType_Id = master_plantype_id
                            sql_plan.Option_Id = master_options_id
                            sql_plan.SwitchAllowed_Id = None
                            sql_plan.Plan_DivReinvOption = 1 if scm_div_reinv_option == "Y" else 0
                            sql_plan.Plan_External_Map_Code = external_map_code
                            sql_plan.Plan_Demat = 0 if demat == "FALSE" else 1
                            sql_plan.Plan_RTA_AMC_Code = rta_amc_code
                            sql_plan.ISIN = isin1
                            sql_plan.ISIN2 = isin2
                            sql_plan.RTA_Code = rta_code
                            sql_plan.RTA_Name = rta_name
                            sql_plan.AMFI_Code = amfi_code
                            sql_plan.AMFI_Name = amfi_scheme_names
                            sql_plan.Updated_By = user_id
                            sql_plan.Updated_Date = TODAY
                        
                        db_session.commit()
                        Remarks = "Strategy Updated successfully."

                    else:
                        Plans_insert = Plans()
                        Plans_insert.Plan_Name = scm_scheme_name
                        Plans_insert.Plan_Code = scm_scheme_cd
                        Plans_insert.MF_Security_Id = sql_MFSecurity_id[0].MF_Security_Id
                        Plans_insert.PlanType_Id = master_plantype_id
                        Plans_insert.Option_Id = master_options_id
                        Plans_insert.SwitchAllowed_Id = None
                        Plans_insert.Plan_DivReinvOption = 1 if scm_div_reinv_option == "Y" else 0
                        Plans_insert.Plan_External_Map_Code = external_map_code
                        Plans_insert.Plan_Demat = 0 if demat == "FALSE" else 1
                        Plans_insert.Plan_RTA_AMC_Code = rta_amc_code
                        Plans_insert.ISIN = isin1
                        Plans_insert.ISIN2 = isin2
                        Plans_insert.RTA_Code = rta_code
                        Plans_insert.RTA_Name = rta_name
                        Plans_insert.AMFI_Code = amfi_code
                        Plans_insert.AMFI_Name = amfi_scheme_names
                        Plans_insert.Is_Deleted = 0
                        Plans_insert.Created_By = user_id
                        Plans_insert.Created_Date = TODAY

                        db_session.add(Plans_insert)
                        db_session.commit()

                        sql_plan_id = db_session.query(Plans.Plan_Id).filter(Plans.Plan_Code == scm_scheme_cd).filter(Plans.Is_Deleted != 1).one_or_none()

                        PlanProductMapping_insert = PlanProductMapping()
                        PlanProductMapping_insert.Plan_Id = sql_plan_id.Plan_Id # TODO Bug Identified >> Multiple rows were found when one or none was required
                        PlanProductMapping_insert.Product_Id = PRODUCT_ID
                        PlanProductMapping_insert.Is_Deleted = 0

                        db_session.add(PlanProductMapping_insert)
                        db_session.commit()
                        Remarks = "Strategy uploaded successfully."
            except Exception as ex:
                ex_record = {}
                ex_record['CMOTS_SchemeCode'] = row['SchemeCode']
                ex_record['CMOTS_ISIN'] = row['ISIN1']
                ex_record['CMOTS_ISIN2'] = row['ISIN2']
                ex_record['CMOTS_AMFISchemeName'] = row["AMFI SCHEME NAMES"]
                ex_record['CMOTS_AMFICode'] = row['AMFI_CODE']
                ex_record['Remarks'] = Remarks
                ex_record['Exception'] = str(ex)
                exceptions_data.append(ex_record)
                
                db_session.rollback()
                db_session.flush()
                continue

            item = row.copy()
            item["Remarks"] = Remarks
            items.append(item)
        if exceptions_data:
            exception_file_path = get_rel_path(output_file_path, __file__).lower().replace('schememasterupdate', 'schememasterupdate_exception')
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
            schedule_email_activity(db_session, 'devteam@finalyca.com', '', '', F"MF - FundMaster - Exception file{TODAY.strftime('%Y-%b-%d')}", F"MF - FundMaster - Exception file{TODAY.strftime('%Y-%b-%d')}", attachements)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/SchemeMasterUpdate_25-07-2023-04-13-57.csv"
    READ_PATH = "read/SchemeMasterUpdate_25-07-2023-04-13-57.csv"
    import_mf_fundmaster_file(FILE_PATH, READ_PATH, USER_ID)
