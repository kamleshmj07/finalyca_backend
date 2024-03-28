import csv
from datetime import datetime as dt
from datetime import timedelta
import logging
from operator import or_
from unittest.loader import VALID_MODULE_NAME
from sqlalchemy import and_, desc, func, text
import os
import math

from utils import *
from utils.finalyca_store import *
from fin_models import *
from bizlogic.importer_helper import save_planproductmapping, get_rel_path, write_csv, get_schemecode_factsheet
from bizlogic.analytics import get_risk_ratios
from bizlogic.fund_portfolio_analysis import generate_portfolio_analysis, prepare_fund_screener

def import_aif_factsheet_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))
    engine = get_unsafe_db_engine(config)
    connection = engine.raw_connection()
    cursor_obj = connection.cursor()

    header = ["AMCCode", "SchemeCode", "SchemeName", "Date", "Equity", "Debt", "Cash", "SCHEME RETURNS-1 MONTH", "SCHEME RETURNS-3 MONTH", "SCHEME RETURNS- 6 MONTH", "SCHEME RETURNS-1 YEAR", "SCHEME RETURNS-2 YEAR", "SCHEME RETURNS-3 YEAR", "SCHEME RETURNS- 5 YEAR", "SCHEME RETURNS- 10 YEAR", "SCHEME RETURNS since inception", "SCHEME BENCHMARK RETURNS-1 MONTH", "SCHEME BENCHMARK RETURNS-3 MONTH", "SCHEME BENCHMARK RETURNS-6 MONTH", "SCHEME BENCHMARK RETURNS-1 YEAR", "SCHEME BENCHMARK RETURNS-2 YEAR", "SCHEME BENCHMARK RETURNS-3 YEAR", "SCHEME BENCHMARK RETURNS-5 YEAR", "SCHEME BENCHMARK RETURNS-10 YEAR", "SCHEME BENCHMARK RETURNS-SI", "Exit Load", "Net Assets (Rs Cr)", "Avg Mkt Cap (Rs Cr)", "Portfolio Divident Yield", "Total Stocks", "Portfolio P/B Ratio", "Portfolio P/E Ratio", "AIF_COMMITED CAPITAL (Rs Cr)","AIF_DRAWDOWN CAPITAL (Rs Cr)","AIF_CAPITAL RETURNED (Rs Cr)","AIF_INITIAL CLOSURE DATE","AIF_FUND CLOSURE DATE", "AIF_ALLOTMENT DATE", "AIF_DOLLAR_NAV", "AIF_FUND_RATING", "Remarks"]

    try:        
        items = list()
        PRODUCT_ID = 5
        TODAY = dt.today()
        logging.warning(F"aif upload - start")
        with open(get_rel_path(input_file_path, __file__), 'r') as f:
            csvreader = csv.DictReader(f)
            logging.warning(F"aif upload - after reader")
            for row in csvreader:
                autocalculate = None
                logging.warning(F"aif upload - row")
                schemecode = get_schemecode_factsheet(row["SchemeCode"], PRODUCT_ID) 
                date = dt.strptime(row["Date"], '%d-%m-%Y')
                logging.warning(F"aif upload - row1")
                equity = None if row["Equity"].strip() == "NA" else row["Equity"].replace("%", "")
                debt = None if row["Debt"].strip() == "NA" else row["Debt"].replace("%", "") 
                cash = None if row["Cash"].strip() == "NA" else row["Cash"].replace("%", "") 
                schemereturn1month = None if row["SCHEME RETURNS-1 MONTH"].strip() == "NA" else row["SCHEME RETURNS-1 MONTH"].replace("%", "")
                schemereturn3month = None if row["SCHEME RETURNS-3 MONTH"].strip() == "NA" else row["SCHEME RETURNS-3 MONTH"].replace("%", "")
                schemereturn6month = None if row["SCHEME RETURNS- 6 MONTH"].strip() == "NA" else row["SCHEME RETURNS- 6 MONTH"].replace("%", "")
                schemereturn1year = None if row["SCHEME RETURNS-1 YEAR"].strip() == "NA" else row["SCHEME RETURNS-1 YEAR"].replace("%", "")
                schemereturn2year = None if row["SCHEME RETURNS-2 YEAR"].strip() == "NA" else row["SCHEME RETURNS-2 YEAR"].replace("%", "")
                schemereturn3year = None if row["SCHEME RETURNS-3 YEAR"].strip() == "NA" else row["SCHEME RETURNS-3 YEAR"].replace("%", "")
                schemereturn5year = None if row["SCHEME RETURNS- 5 YEAR"].strip() == "NA" else row["SCHEME RETURNS- 5 YEAR"].replace("%", "")
                schemereturn10year = None if row["SCHEME RETURNS- 10 YEAR"].strip() == "NA" else row["SCHEME RETURNS- 10 YEAR"].replace("%", "")
                schemereturnsi = None if row["SCHEME RETURNS since inception"].strip() == "NA" else row["SCHEME RETURNS since inception"].replace("%", "")
                schemebenchmarkreturn1month = None if row["SCHEME BENCHMARK RETURNS-1 MONTH"].strip() == "NA" else row["SCHEME BENCHMARK RETURNS-1 MONTH"].replace("%", "")
                schemebenchmarkreturn3month = None if row["SCHEME BENCHMARK RETURNS-3 MONTH"].strip() == "NA" else row["SCHEME BENCHMARK RETURNS-3 MONTH"].replace("%", "")
                schemebenchmarkreturn6month = None if row["SCHEME BENCHMARK RETURNS-6 MONTH"].strip() == "NA" else row["SCHEME BENCHMARK RETURNS-6 MONTH"].replace("%", "")
                schemebenchmarkreturn1year = None if row["SCHEME BENCHMARK RETURNS-1 YEAR"].strip() == "NA" else row["SCHEME BENCHMARK RETURNS-1 YEAR"].replace("%", "")
                schemebenchmarkreturn2year = None if row["SCHEME BENCHMARK RETURNS-2 YEAR"].strip() == "NA" else row["SCHEME BENCHMARK RETURNS-2 YEAR"].replace("%", "")
                schemebenchmarkreturn3year = None if row["SCHEME BENCHMARK RETURNS-3 YEAR"].strip() == "NA" else row["SCHEME BENCHMARK RETURNS-3 YEAR"].replace("%", "")
                schemebenchmarkreturn5year = None if row["SCHEME BENCHMARK RETURNS-5 YEAR"].strip()== "NA" else row["SCHEME BENCHMARK RETURNS-5 YEAR"].replace("%", "")
                schemebenchmarkreturn10year = None if row["SCHEME BENCHMARK RETURNS-10 YEAR"].strip() == "NA" else row["SCHEME BENCHMARK RETURNS-10 YEAR"].replace("%", "")
                schemebenchmarkreturnsi = None if row["SCHEME BENCHMARK RETURNS-SI"].strip() == "NA" else row["SCHEME BENCHMARK RETURNS-SI"].replace("%", "")  
                exitload = row["Exit Load"] 
                netassetrscr = None if row["Net Assets (Rs Cr)"].strip() == "NA" else row["Net Assets (Rs Cr)"]
                avgmktcaprscr = None if row["Avg Mkt Cap (Rs Cr)"].strip() == "NA" else row["Avg Mkt Cap (Rs Cr)"] 
                portfoliodividendyield = None if row["Portfolio Divident Yield"].strip() == "NA" else row["Portfolio Divident Yield"]
                totalstocks = None if row["Total Stocks"].strip() == "NA" else row["Total Stocks"]
                portfoliopbratio = None if row["Portfolio P/B Ratio"].strip() == "NA" else row["Portfolio P/B Ratio"]
                portfolioperatio = None if row["Portfolio P/E Ratio"].strip() == "NA" else row["Portfolio P/E Ratio"]
                commitedcapital_crs = None if row["AIF_COMMITED CAPITAL (Rs Cr)"].strip() == "NA" else row["AIF_COMMITED CAPITAL (Rs Cr)"]
                dradowncapital_crs = None if row["AIF_DRAWDOWN CAPITAL (Rs Cr)"].strip() == "NA" else row["AIF_DRAWDOWN CAPITAL (Rs Cr)"]
                capitalreturned_crs = None if row["AIF_CAPITAL RETURNED (Rs Cr)"].strip() == "NA" else row["AIF_CAPITAL RETURNED (Rs Cr)"]
                logging.warning(F"aif upload - row2")
                initial_closure_date = None if row["AIF_INITIAL CLOSURE DATE"].strip() == "NA" else dt.strptime(row["AIF_INITIAL CLOSURE DATE"], '%d-%m-%Y')
                logging.warning(F"aif upload - row3")
                fund_closure_date = None if row["AIF_FUND CLOSURE DATE"].strip() == "NA" else dt.strptime(row["AIF_FUND CLOSURE DATE"], '%d-%m-%Y')
                logging.warning(F"aif upload - row4")
                allotment_date = None if row["AIF_ALLOTMENT DATE"].strip() == "NA" else dt.strptime(row["AIF_ALLOTMENT DATE"], '%d-%m-%Y')
                aif_dollar_nav = None if row["AIF_DOLLAR_NAV"].strip() == "NA" else row["AIF_DOLLAR_NAV"]
                aif_fund_rating = None if row["AIF_FUND_RATING"].strip() == "NA" else row["AIF_FUND_RATING"]
                logging.warning(F"aif upload - row5")
                Remarks = None

                sql_plan_id = db_session.query(Plans.Plan_Id, 
                                               MFSecurity.BenchmarkIndices_Id, 
                                               func.datediff(text('Month'),MFSecurity.MF_Security_OpenDate, date.today()))\
                                                .select_from(Plans)\
                                                .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                                .filter(Plans.Plan_Code == schemecode)\
                                                .filter(Plans.Is_Deleted != 1)\
                                                .filter(MFSecurity.Status_Id==1)\
                                                .filter(MFSecurity.Is_Deleted != 1)\
                                                .order_by(desc(Plans.Plan_Id)).first()

                if sql_plan_id:
                    save_planproductmapping(db_session, sql_plan_id.Plan_Id, PRODUCT_ID, user_id, TODAY) 
                    
                else:
                    Remarks = "Plans are not available in system."

                if date > TODAY:
                    Remarks = "Factsheet cannot be uploaded for Future date."
                logging.warning(F"aif upload - row6")
                if Remarks == None:
                    update_values = {
                    FactSheet.Is_Deleted : 1,
                    FactSheet.Updated_By : user_id,
                    FactSheet.Updated_Date : TODAY,
                    }
                    
                    sql_factsheet_del = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id==sql_plan_id.Plan_Id, FactSheet.TransactionDate == date, FactSheet.Is_Deleted != 1)).update(update_values)

                    db_session.commit()
                    Remarks = 'Flagged Deleted and uploaded new records Successfully.'

                    if netassetrscr == None:
                        sql_fact = db_session.query(func.max(FactSheet.TransactionDate)).filter(FactSheet.Plan_Id == sql_plan_id.Plan_Id, FactSheet.NetAssets_Rs_Cr > 0, FactSheet.Is_Deleted != 1, FactSheet.TransactionDate < date).first()

                        if sql_fact:
                            sql_facts = db_session.query(FactSheet.NetAssets_Rs_Cr).filter(FactSheet.Plan_Id == sql_plan_id.Plan_Id, FactSheet.Is_Deleted != 1, FactSheet.TransactionDate == sql_fact[0]).first()

                            if sql_facts != None:
                                netassetrscr = sql_facts.NetAssets_Rs_Cr                  

                    if avgmktcaprscr == None and portfoliodividendyield == None and totalstocks == None and portfoliopbratio == None and portfolioperatio == None:
                        autocalculate = True

                    logging.info(F"PMS - Factsheet insertion started.")
                    factsheet_insert = FactSheet()
                    factsheet_insert.Plan_Id = sql_plan_id.Plan_Id
                    factsheet_insert.TransactionDate = date
                    factsheet_insert.Equity = equity
                    factsheet_insert.Debt = debt
                    factsheet_insert.Cash = cash
                    factsheet_insert.SCHEME_RETURNS_1MONTH = schemereturn1month
                    factsheet_insert.SCHEME_RETURNS_3MONTH = schemereturn3month
                    factsheet_insert.SCHEME_RETURNS_6MONTH = schemereturn6month
                    factsheet_insert.SCHEME_RETURNS_1YEAR = schemereturn1year
                    factsheet_insert.SCHEME_RETURNS_2YEAR = schemereturn2year
                    factsheet_insert.SCHEME_RETURNS_3YEAR = schemereturn3year
                    factsheet_insert.SCHEME_RETURNS_5YEAR = schemereturn5year
                    factsheet_insert.SCHEME_RETURNS_10YEAR = schemereturn10year
                    factsheet_insert.SCHEME_RETURNS_since_inception = schemereturnsi
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_1MONTH = schemebenchmarkreturn1month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_3MONTH = schemebenchmarkreturn3month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_6MONTH = schemebenchmarkreturn6month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_1YEAR = schemebenchmarkreturn1year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_2YEAR = schemebenchmarkreturn2year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_3YEAR = schemebenchmarkreturn3year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_5YEAR = schemebenchmarkreturn5year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_10YEAR = schemebenchmarkreturn10year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_SI = schemebenchmarkreturnsi
                    factsheet_insert.Exit_Load = exitload
                    factsheet_insert.NetAssets_Rs_Cr = netassetrscr
                    factsheet_insert.AvgMktCap_Rs_Cr = avgmktcaprscr
                    factsheet_insert.Portfolio_Dividend_Yield = portfoliodividendyield
                    factsheet_insert.TotalStocks = totalstocks
                    factsheet_insert.PortfolioP_BRatio = portfoliopbratio
                    factsheet_insert.PortfolioP_ERatio = portfolioperatio
                    factsheet_insert.AIF_COMMITEDCAPITAL_Rs_Cr = commitedcapital_crs
                    factsheet_insert.AIF_DRAWDOWNCAPITAL_Rs_Cr = dradowncapital_crs
                    factsheet_insert.AIF_CAPITALRETURNED_Rs_Cr = capitalreturned_crs
                    factsheet_insert.AIF_INITIALCLOSUREDATE = initial_closure_date
                    factsheet_insert.AIF_FUNDCLOSUREDATE = fund_closure_date
                    factsheet_insert.AIF_ALLOTMENTDATE = allotment_date
                    factsheet_insert.SourceFlag = "AIF"
                    factsheet_insert.AIF_DOLLAR_NAV = aif_dollar_nav
                    factsheet_insert.AIF_FUND_RATING = aif_fund_rating
                    factsheet_insert.Is_Deleted = 0
                    factsheet_insert.Created_By = user_id
                    factsheet_insert.Created_Date = TODAY

                    #calculate risk analysis                    
                    sql_nav = db_session.query(NAV)\
                    .filter(NAV.Is_Deleted != 1).filter(NAV.Plan_Id == sql_plan_id.Plan_Id).filter(NAV.NAV_Type=='P').first()

                    sql_bench_nav = db_session.query(NAV)\
                    .filter(NAV.Is_Deleted != 1).filter(NAV.Plan_Id == sql_plan_id.BenchmarkIndices_Id).filter(NAV.NAV_Type=='I').first()

                    if sql_nav and sql_bench_nav:
                        #calculate risk analysis                    
                        sql_rate_max_date = db_session.query(func.max(RiskFreeIndexRate.Date)).filter(RiskFreeIndexRate.Date <= date).scalar()
                        sql_rate = db_session.query(RiskFreeIndexRate).filter(RiskFreeIndexRate.Date == sql_rate_max_date).one_or_none()

                        risk_free_index_date = sql_rate.Date
                        risk_free_index_rate = float(sql_rate.Rate)
                        
                        if sql_plan_id[2] >= 36:
                            risk_ratio_36 = get_risk_ratios(db_session, sql_plan_id.Plan_Id, sql_plan_id.BenchmarkIndices_Id, date, 36, risk_free_index_rate)

                            if risk_ratio_36:
                                factsheet_insert.StandardDeviation = risk_ratio_36["StandardDeviation"] if not math.isnan(risk_ratio_36["StandardDeviation"]) else None
                                factsheet_insert.SharpeRatio = risk_ratio_36["SharpeRatio"] if not math.isnan(risk_ratio_36["SharpeRatio"]) else None
                                factsheet_insert.Alpha = risk_ratio_36["Alpha"] if not math.isnan(risk_ratio_36["Alpha"]) else None
                                factsheet_insert.R_Squared = risk_ratio_36["R_Squared"] if not math.isnan(risk_ratio_36["R_Squared"]) else None
                                factsheet_insert.Beta = risk_ratio_36["Beta"] if not math.isnan(risk_ratio_36["Beta"]) else None
                                factsheet_insert.Mean = risk_ratio_36["Mean"] if not math.isnan(risk_ratio_36["Mean"]) else None
                                factsheet_insert.Sortino = risk_ratio_36["Sortino"] if not math.isnan(risk_ratio_36["Sortino"]) else None
                                factsheet_insert.Treynor_Ratio = risk_ratio_36["TreynorRatio"] if not math.isnan(risk_ratio_36["TreynorRatio"]) else None
                        
                        if sql_plan_id[2] >= 12:
                            risk_ratio_12 = get_risk_ratios(db_session, sql_plan_id.Plan_Id, sql_plan_id.BenchmarkIndices_Id, date, 12, risk_free_index_rate)

                            if risk_ratio_12:
                                                            
                                factsheet_insert.StandardDeviation_1Yr = risk_ratio_12["StandardDeviation"] if not math.isnan(risk_ratio_12["StandardDeviation"]) else None                            
                                factsheet_insert.SharpeRatio_1Yr = risk_ratio_12["SharpeRatio"] if not math.isnan(risk_ratio_12["SharpeRatio"]) else None
                                factsheet_insert.Alpha_1Yr = risk_ratio_12["Alpha"] if not math.isnan(risk_ratio_12["Alpha"]) else None
                                factsheet_insert.R_Squared_1Yr = risk_ratio_12["R_Squared"] if not math.isnan(risk_ratio_12["R_Squared"]) else None
                                factsheet_insert.Beta_1Yr = risk_ratio_12["Beta"] if not math.isnan(risk_ratio_12["Beta"]) else None
                                factsheet_insert.Mean_1Yr = risk_ratio_12["Mean"] if not math.isnan(risk_ratio_12["Mean"]) else None
                                factsheet_insert.Sortino_1Yr = risk_ratio_12["Sortino"] if not math.isnan(risk_ratio_12["Sortino"]) else None
                                factsheet_insert.Treynor_Ratio_1Yr = risk_ratio_12["TreynorRatio"] if not math.isnan(risk_ratio_12["TreynorRatio"]) else None
                        
                    db_session.add(factsheet_insert)
                    db_session.flush()

                    inserted_factsheetid = factsheet_insert.FactSheet_Id
                    db_session.commit()
                    
                    sql_logics_plan = db_session.query(Fund.AutoPopulate)\
                                                    .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                    .filter(Plans.Is_Deleted != 1)\
                                                    .filter(MFSecurity.Is_Deleted != 1)\
                                                    .filter(Fund.Is_Deleted != 1)\
                                                    .filter(Plans.Plan_Id == sql_plan_id.Plan_Id).one_or_none()

                    if sql_logics_plan.AutoPopulate:
                        generate_portfolio_analysis(db_session, inserted_factsheetid,user_id, False, True)

                        #Update IsPortfolioProcessed flag
                        update_values = {
                        FactSheet.IsPortfolioProcessed : 1
                        }
                        sql_factsheet_del = db_session.query(FactSheet).filter(FactSheet.FactSheet_Id == inserted_factsheetid).update(update_values)
                        db_session.commit()
                    
                    if autocalculate and sql_logics_plan.AutoPopulate:
                        sql_hol_count = 0

                        sql_hol_count = db_session.query(func.count(Plans.Plan_Code))\
                                                        .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                                        .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                                        .join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id)\
                                                        .filter(UnderlyingHoldings.Portfolio_Date == date)\
                                                        .filter(Plans.Plan_Id == sql_plan_id.Plan_Id)\
                                                        .filter(Plans.Is_Deleted != 1)\
                                                        .filter(MFSecurity.Is_Deleted != 1)\
                                                        .filter(Fund.Is_Deleted != 1)\
                                                        .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                                        .group_by(UnderlyingHoldings.Portfolio_Date, Plans.Plan_Id).all()

                        if sql_hol_count:
                            if sql_hol_count[0][0] > 0:
                                totalstocks = sql_hol_count[0][0]
                                update_values = {
                                    FactSheet.TotalStocks : totalstocks
                                }
                                sql_factstockupdate = db_session.query(FactSheet)\
                                                                        .filter(FactSheet.Plan_Id == sql_plan_id.Plan_Id)\
                                                                        .filter(FactSheet.TransactionDate == date, FactSheet.Is_Deleted != 1).update(update_values)
                                db_session.commit()
                            
                                date_1 = date - timedelta(1)
                                date_2 = date - timedelta(2)
                                
                                Temp_PC = db_session.query(Plans.Plan_Id,
                                                           Plans.Plan_Code, 
                                                           Plans.Plan_Name,
                                                           Fund.Fund_Id,
                                                           Fund.Fund_Code, 
                                                           Fund.Fund_Name,
                                                           UnderlyingHoldings.Portfolio_Date, 
                                                           UnderlyingHoldings.ISIN_Code, 
                                                           UnderlyingHoldings.HoldingSecurity_Id,
                                                           HoldingSecurity.HoldingSecurity_Name,
                                                           HoldingSecurity.Co_Code, 
                                                           UnderlyingHoldings.Percentage_to_AUM, 
                                                           Fundamentals.DivYield, 
                                                           Fundamentals.EPS, 
                                                           Fundamentals.PE, 
                                                           Fundamentals.PBV, 
                                                           Fundamentals.mcap)\
                                                            .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                                            .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                                            .join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id)\
                                                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                            .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                            .join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id , isouter=True)\
                                                            .join(Fundamentals, Fundamentals.CO_CODE == HoldingSecurity.Co_Code)\
                                                            .filter(or_(or_(Fundamentals.PriceDate == date, Fundamentals.PriceDate == date_1), Fundamentals.PriceDate == date_2))\
                                                            .filter(Product.Product_Id == PRODUCT_ID)\
                                                            .filter(UnderlyingHoldings.Portfolio_Date == date)\
                                                            .filter(Plans.Plan_Id == sql_plan_id.Plan_Id)\
                                                            .filter(Plans.Is_Deleted != 1)\
                                                            .filter(MFSecurity.Is_Deleted != 1)\
                                                            .filter(Fund.Is_Deleted != 1)\
                                                            .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                                            .group_by(UnderlyingHoldings.Portfolio_Date, 
                                                                      Plans.Plan_Id,
                                                                      Plans.Plan_Code, 
                                                                      Plans.Plan_Name, 
                                                                      Fund.Fund_Id, 
                                                                      Fund.Fund_Code, 
                                                                      Fund.Fund_Name, 
                                                                      UnderlyingHoldings.ISIN_Code, 
                                                                      UnderlyingHoldings.HoldingSecurity_Id, 
                                                                      HoldingSecurity.HoldingSecurity_Name, 
                                                                      HoldingSecurity.Co_Code, 
                                                                      UnderlyingHoldings.Percentage_to_AUM, 
                                                                      Fundamentals.DivYield, 
                                                                      Fundamentals.EPS, 
                                                                      Fundamentals.PE, 
                                                                      Fundamentals.mcap, 
                                                                      Fundamentals.PBV).all()

                                sum_Percentage_to_AUM = 0
                                Temp_PC_items = list()
                                for sql_obj in Temp_PC:
                                    json_obj = dict()
                                    json_obj["Plan_Id"] = sql_obj['Plan_Id']
                                    json_obj["Plan_Code"] = sql_obj['Plan_Code']
                                    json_obj["Plan_Name"] = sql_obj['Plan_Name']
                                    json_obj["Fund_Id"] = sql_obj['Fund_Id']
                                    json_obj["Fund_Code"] = sql_obj['Fund_Code']
                                    json_obj["Fund_Name"] = sql_obj['Fund_Name']
                                    json_obj["Portfolio_Date"] = sql_obj['Portfolio_Date']
                                    json_obj["ISIN_Code"] = sql_obj['ISIN_Code']
                                    json_obj["HoldingSecurity_Id"] = sql_obj['HoldingSecurity_Id']
                                    json_obj["HoldingSecurity_Name"] = sql_obj['HoldingSecurity_Name']
                                    json_obj["Co_Code"] = sql_obj['Co_Code']
                                    json_obj["Percentage_to_AUM"] = sql_obj['Percentage_to_AUM']
                                    json_obj["EPS"] = sql_obj['EPS']
                                    json_obj["PE"] = sql_obj['PE']
                                    json_obj["PBV"] = sql_obj['PBV']
                                    json_obj["mcap"] = sql_obj['mcap']
                                    json_obj["DivYield"] = sql_obj['DivYield']
                                    json_obj["PE_Rebased"] = 0.0
                                    json_obj["PE_Per"] = 0.0
                                    json_obj["PE_Ratio"] = 0.0
                                    json_obj["PB_Rebased"] = 0.0                  
                                    json_obj["PB_Per"] = 0.0
                                    json_obj["PB_Ratio"] = 0.0
                                    json_obj["MCAP_Rebased"] = 0.0
                                    json_obj["MCAP_Per"] = 0.0
                                    json_obj["MCAP_Ratio"] = 0.0
                                    json_obj["DY_Rebased"] = 0.0
                                    json_obj["DY_Per"] = 0.0
                                    json_obj["DY_Ratio"] = 0.0
                                    Temp_PC_items.append(json_obj)

                                    sum_Percentage_to_AUM += sql_obj['Percentage_to_AUM']

                                Summary_PC = db_session.query(Plans.Plan_Id,
                                                              Plans.Plan_Code, 
                                                              Plans.Plan_Name,
                                                              UnderlyingHoldings.Portfolio_Date, 
                                                              func.sum(UnderlyingHoldings.Percentage_to_AUM).label('Percentage_to_AUM'))\
                                                                .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                                                .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                                                .join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id)\
                                                                .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                                .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                                .join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id , isouter=True)\
                                                                .join(Fundamentals, Fundamentals.CO_CODE == HoldingSecurity.Co_Code)\
                                                                .filter(or_(or_(Fundamentals.PriceDate == date, Fundamentals.PriceDate == date_1), Fundamentals.PriceDate == date_2))\
                                                                .filter(UnderlyingHoldings.Portfolio_Date == date)\
                                                                .filter(Plans.Plan_Id == sql_plan_id.Plan_Id)\
                                                                .filter(Plans.Is_Deleted != 1)\
                                                                .filter(MFSecurity.Is_Deleted != 1)\
                                                                .filter(Fund.Is_Deleted != 1)\
                                                                .filter(UnderlyingHoldings.Is_Deleted != 1)\
                                                                .group_by(UnderlyingHoldings.Portfolio_Date, 
                                                                          Plans.Plan_Id,
                                                                          Plans.Plan_Code, 
                                                                          Plans.Plan_Name).all()

                                for T in Temp_PC_items:
                                    if T['PE'] != 0:    
                                        T["PE_Rebased"] = ((float(T['Percentage_to_AUM']) / float(sum_Percentage_to_AUM)) * 100.00)

                                        T["PE_Per"] = (1 / float(T['PE']))

                                        T["PE_Ratio"] = ((((float(T['Percentage_to_AUM']) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 / float(T['PE'])))


                                    if T['PBV'] != 0:    
                                        T["PB_Rebased"] = (((float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)

                                        T["PB_Per"] = (1 /  float(T['PBV']))

                                        T["PB_Ratio"] = (((((float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 /  float(T['PBV'])))
                                        

                                    if T['mcap'] != 0:
                                        T["MCAP_Rebased"] = ((( float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)

                                        T["MCAP_Per"] = (1 /  float(T['mcap']))

                                        T["MCAP_Ratio"] = ((((( float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 /  float(T['mcap'])))

                                    if T['DivYield'] != 0:
                                        T["DY_Rebased"] = ((( float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)

                                        T["DY_Ratio"] =  (1 /  float(T['DivYield']))

                                        T["DY_Ratio"] = ((((( float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 /  float(T['DivYield'])))

                                for T in Summary_PC:#loop through each distinct plan id name date
                                    sum_mcap = 0
                                    sum_PE_Ratio = 0
                                    sum_PB_Ratio = 0
                                    sum_DY_Ratio = 0
                                    count_mcap = 0

                                    for d in Temp_PC_items:
                                        if d['Plan_Id'] == T['Plan_Id'] and d['Plan_Name'] == T['Plan_Name'] and d['Portfolio_Date'] == T['Portfolio_Date']:
                                            sum_mcap += float(d['mcap'])
                                            count_mcap += 1
                                            sum_PE_Ratio += float(d['PE_Ratio'])
                                            sum_PB_Ratio += float(d['PB_Ratio'])
                                            sum_DY_Ratio += float(d['DY_Ratio'])

                                    avg_mcap = float(sum_mcap / count_mcap)
                                    if sum_PE_Ratio != 0:
                                        sum_PE_Ratio = (1/sum_PE_Ratio) 
                                    if sum_PB_Ratio != 0:   
                                        sum_PB_Ratio = (1/sum_PB_Ratio) 
                                    if sum_DY_Ratio != 0:
                                        sum_DY_Ratio = (1/sum_DY_Ratio) 

                                    update_values = {
                                    FactSheet.PortfolioP_BRatio : sum_PB_Ratio,
                                    FactSheet.PortfolioP_ERatio : sum_PE_Ratio,
                                    FactSheet.AvgMktCap_Rs_Cr : avg_mcap,
                                    FactSheet.Portfolio_Dividend_Yield : sum_DY_Ratio,
                                    FactSheet.Updated_By : user_id,
                                    FactSheet.Updated_Date : TODAY,
                                    }

                                    sql_h = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id==T['Plan_Id'], FactSheet.TransactionDate == date) , FactSheet.Is_Deleted != 1).update(update_values)
                                    db_session.commit()

                                    sql_factsheetdata = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id==T['Plan_Id'], FactSheet.TransactionDate == date) , FactSheet.Is_Deleted != 1).one_or_none()

                                    sql_allholdings = db_session.query(UnderlyingHoldings).\
                                                                        join(Fund, Fund.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                                                        .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                                                        .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                                                        .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                                                        .join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id , isouter=True)\
                                                                        .filter(Product.Product_Id == PRODUCT_ID)\
                                                                        .filter(UnderlyingHoldings.Portfolio_Date == date)\
                                                                        .filter(Plans.Plan_Id == sql_plan_id.Plan_Id)\
                                                                        .filter(Plans.Is_Deleted != 1)\
                                                                        .filter(MFSecurity.Is_Deleted != 1)\
                                                                        .filter(Fund.Is_Deleted != 1)\
                                                                        .filter(UnderlyingHoldings.Is_Deleted != 1).all()

                                    for uh in sql_allholdings:
                                        perc_aum = uh.Percentage_to_AUM
                                        netasset = 0
                                        if sql_factsheetdata.NetAssets_Rs_Cr:
                                            netasset = sql_factsheetdata.NetAssets_Rs_Cr
                                        value_in_inr = (perc_aum / 100) * netasset * 10000000

                                        uh.Value_in_INR = value_in_inr
                                        uh.Updated_By = user_id
                                        uh.Updated_Date = TODAY
                                    
                                    db_session.commit()
                
                item = row.copy()
                item["Remarks"] = Remarks
                items.append(item)  

    finally:            
        cursor_obj.close()
        connection.commit()
        connection.close()

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/factsheet_to_upload.csv"
    READ_PATH = "read/factsheet_to_upload.csv"
    import_aif_factsheet_file(FILE_PATH, READ_PATH, USER_ID)
