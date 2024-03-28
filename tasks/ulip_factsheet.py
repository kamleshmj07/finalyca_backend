import csv
from datetime import datetime as dt
import pandas as pd
from sqlalchemy import and_, desc, func
import os
from bizlogic.importer_helper import save_planproductmapping, get_rel_path, write_csv,get_schemecode_factsheet
from utils import *
from utils.finalyca_store import *
from fin_models import *
from bizlogic.fund_portfolio_analysis import generate_portfolio_analysis
from datetime import date as date
from bizlogic.common_helper import schedule_email_activity

def import_ulip_factsheet_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
     
    db_session = get_finalyca_scoped_session(is_production_config(config))
    items = list()

    PRODUCT_ID = 2
    TODAY = dt.today()

    header = ["AMCCode","SchemeCode","SchemeName","Date","NetAssets","52WeekHigh","52WeekLow","TotalStocks","AvgMktCap","PortfolioPBRatio",
              "PortfolioPERatio","StandardDeviation","SharpeRatio","Beta","RSquared","Alpha","Mean","Equity","Debt","Cash",
              "RANKING_RANK_1MONTH","COUNT_1MONTH","RANKING_RANK_3MONTH","COUNT_3MONTH","RANKING_RANK_6MONTH","COUNT_6MONTH",
              "RANKING_RANK_1YEAR","COUNT_1YEAR","RANKING_RANK_3YEAR","COUNT_3YEAR","RANKING_RANK_5YEAR","COUNT_5YEAR",
              "SCHEME_RETURNS_1MONTH","SCHEME_RETURNS_3MONTH","SCHEME_RETURNS_6MONTH","SCHEME_RETURNS_1YEAR","SCHEME_RETURNS_3YEAR",
              "SCHEME_RETURNS_5YEAR","SCHEME_RETURNS_SinceInception","SCHEME_BENCHMARK_RETURNS_1MONTH","SCHEME_BENCHMARK_RETURNS_3MONTH",
              "SCHEME_BENCHMARK_RETURNS_6MONTH","SCHEME_BENCHMARK_RETURNS_1YEAR","SCHEME_BENCHMARK_RETURNS_3YEAR",
              "SCHEME_BENCHMARK_RETURNS_5YEAR","SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH","SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH",
              "SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH","SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR","SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR",
              "SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR","BillRediscounting","CashEquivalent","TermDeposit","BondsDebentures",
              "CashAnd_CashEquivalent","CPCD","GOISecurities","MutualFundsDebt","SecuritisedDebt","ShortTermDebt","TermDeposits",
              "TreasuryBills","SFINCode","1YrStandard_Deviation","1YrSharpe_Ratio","1YrBeta","1YrRSquared","1YrAlpha","1YrMean","Remarks", "RiskGrade"]
    exceptions_data = []

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f, delimiter='|')
        for row in csvreader:
            try:
                amccode = row['AMCCode']
                schemecode = get_schemecode_factsheet(row['SchemeCode'], PRODUCT_ID)
                schemename = row['SchemeName']
                date = None if row["Date"].strip() =="" else dt.strptime(row["Date"].replace("/","-"), '%d-%m-%Y')   # we assume that if no date is provided then the data is as of latest date for ULIP
                net_assets_rscr = None if row['NetAssets'].strip() == '' else row['NetAssets'].strip()
                week_highrs52 = None if row['52WeekHigh'].strip() == '' else row['52WeekHigh'].strip()
                week_lowrs52 = None if row['52WeekLow'].strip() == '' else row['52WeekLow'].strip()
                total_stocks = None if row['TotalStocks'].strip() == '' else row['TotalStocks'].strip()
                avg_mkt_cap_rscr = None if row['AvgMktCap'].strip() == '' else row['AvgMktCap'].strip()
                portfolio_pb_ratio = None if row['PortfolioPBRatio'].strip() == '' else row['PortfolioPBRatio'].strip()
                portfolio_pe_ratio = None if row['PortfolioPERatio'].strip() == '' else row['PortfolioPERatio'].strip()
                standard_deviation = None if row['StandardDeviation'].strip() == '' else row['StandardDeviation'].strip()
                sharpe_ratio = None if row['SharpeRatio'].strip() == '' else row['SharpeRatio'].strip()
                beta = None if row['Beta'].strip() == '' else row['Beta']
                r_squared = None if row['RSquared'].strip() == '' else row['RSquared'].strip()
                alpha = None if row['Alpha'].strip() == '' else row['Alpha'].strip()
                mean = None if row['Mean'].strip() == '' else row['Mean'].strip()
                equity = None if row['Equity'].strip() == '' else row['Equity'].strip()
                debt = None if row['Debt'].strip() == '' else row['Debt'].strip()
                cash = None if row['Cash'].strip() == '' else row['Cash'].strip()

                ranking_rank1month = None if row['RANKING_RANK_1MONTH'].strip() == '' else row['RANKING_RANK_1MONTH'].strip()
                count1month = None if row['COUNT_1MONTH'].strip() == '' else row['COUNT_1MONTH'].strip()
                ranking_rank3month = None if row['RANKING_RANK_3MONTH'].strip() == '' else row['RANKING_RANK_3MONTH'].strip()
                count3month = None if row['COUNT_3MONTH'].strip() == '' else row['COUNT_3MONTH'].strip()
                ranking_rank6month = None if row['RANKING_RANK_6MONTH'].strip() == '' else row['RANKING_RANK_6MONTH'].strip()
                count6month = None if row['COUNT_6MONTH'].strip() == '' else row['COUNT_6MONTH'].strip()
                ranking_rank1year = None if row['RANKING_RANK_1YEAR'].strip() == '' else row['RANKING_RANK_1YEAR'].strip()
                count1year = None if row['COUNT_1YEAR'].strip() == '' else row['COUNT_1YEAR'].strip()
                ranking_rank3year = None if row['RANKING_RANK_3YEAR'].strip() == '' else row['RANKING_RANK_3YEAR'].strip()
                count3year = None if row['COUNT_3YEAR'].strip() == '' else row['COUNT_3YEAR'].strip()
                ranking_rank5year = None if row['RANKING_RANK_5YEAR'].strip() == '' else row['RANKING_RANK_5YEAR'].strip()
                count5year = None if row['COUNT_5YEAR'].strip() == '' else row['COUNT_5YEAR'].strip()
                
                scheme_returns1month = None if row['SCHEME_RETURNS_1MONTH'].strip() == '' else row['SCHEME_RETURNS_1MONTH'].strip()
                scheme_returns3month = None if row['SCHEME_RETURNS_3MONTH'].strip() == '' else row['SCHEME_RETURNS_3MONTH'].strip()
                scheme_returns6month = None if row['SCHEME_RETURNS_6MONTH'].strip() == '' else row['SCHEME_RETURNS_6MONTH'].strip()
                scheme_returns1year = None if row['SCHEME_RETURNS_1YEAR'].strip() == '' else row['SCHEME_RETURNS_1YEAR'].strip()
                scheme_returns3year = None if row['SCHEME_RETURNS_3YEAR'].strip() == '' else row['SCHEME_RETURNS_3YEAR'].strip()
                scheme_returns5year = None if row['SCHEME_RETURNS_5YEAR'].strip() == '' else row['SCHEME_RETURNS_5YEAR'].strip()
                scheme_returns_since_inception = None if row['SCHEME_RETURNS_SinceInception'].strip() == '' else row['SCHEME_RETURNS_SinceInception'].strip()

                scheme_benchmark_returns1month = None if row['SCHEME_BENCHMARK_RETURNS_1MONTH'].strip() == '' else row['SCHEME_BENCHMARK_RETURNS_1MONTH'].strip()
                scheme_benchmark_returns3month = None if row['SCHEME_BENCHMARK_RETURNS_3MONTH'].strip() == '' else row['SCHEME_BENCHMARK_RETURNS_3MONTH'].strip()
                scheme_benchmark_returns6month = None if row['SCHEME_BENCHMARK_RETURNS_6MONTH'].strip() == '' else row['SCHEME_BENCHMARK_RETURNS_6MONTH'].strip()
                scheme_benchmark_returns1year = None if row['SCHEME_BENCHMARK_RETURNS_1YEAR'].strip() == '' else row['SCHEME_BENCHMARK_RETURNS_1YEAR'].strip()
                scheme_benchmark_returns3year = None if row['SCHEME_BENCHMARK_RETURNS_3YEAR'].strip() == '' else row['SCHEME_BENCHMARK_RETURNS_3YEAR'].strip()
                scheme_benchmark_returns5year = None if row['SCHEME_BENCHMARK_RETURNS_5YEAR'].strip() == '' else row['SCHEME_BENCHMARK_RETURNS_5YEAR'].strip()

                scheme_category_average_returns1month = None if row['SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH'].strip() == '' else row['SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH'].strip()
                scheme_category_average_returns3month = None if row['SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH'].strip() == '' else row['SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH'].strip()
                scheme_category_average_returns6month = None if row['SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH'].strip() == '' else row['SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH'].strip()
                scheme_category_average_returns1year = None if row['SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR'].strip() == '' else row['SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR'].strip()
                scheme_category_average_returns3year = None if row['SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR'].strip() == '' else row['SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR'].strip()
                scheme_category_average_returns5year = None if row['SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR'].strip() == '' else row['SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR'].strip()

                bill_rediscounting = None if row['BillRediscounting'].strip() == '' else row['BillRediscounting'].strip()
                cash_equivalent = None if row['CashEquivalent'].strip() == '' else row['CashEquivalent'].strip()
                term_deposit = None if row['TermDeposit'].strip() == '' else row['TermDeposit'].strip()
                bonds_debentures = None if row['BondsDebentures'].strip() == '' else row['BondsDebentures'].strip()
                cashandcash_equivalent = None if row['CashAnd_CashEquivalent'].strip() == '' else row['CashAnd_CashEquivalent'].strip()
                cpcd = None if row['CPCD'].strip() == '' else row['CPCD'].strip()
                goi_securities = None if row['GOISecurities'].strip() == '' else row['GOISecurities'].strip()
                mutual_funds_debt = None if row['MutualFundsDebt'].strip() == '' else row['MutualFundsDebt'].strip()
                securitised_debt = None if row['SecuritisedDebt'].strip() == '' else row['SecuritisedDebt'].strip()
                short_term_debt = None if row['ShortTermDebt'].strip() == '' else row['ShortTermDebt'].strip()
                term_deposits = None if row['TermDeposits'].strip() == '' else row['TermDeposits'].strip()
                treasury_bills = None if row['TreasuryBills'].strip() == '' else row['TreasuryBills'].strip()
                sfin_code = row['SFINCode']
                yr_standard_deviation1 = None if row['1YrStandard_Deviation'].strip() == '' else row['1YrStandard_Deviation'].strip()
                yrsharperatio1 = None if row['1YrSharpe_Ratio'].strip() == '' else row['1YrSharpe_Ratio'].strip()
                yr_beta1 = None if row['1YrBeta'].strip() == '' else row['1YrBeta'].strip()
                yr_rsquared1 = None if row['1YrRSquared'].strip() == '' else row['1YrRSquared'].strip()
                yr_alpha1 = None if row['1YrAlpha'].strip() == '' else row['1YrAlpha'].strip()
                yr_mean1 = None if row['1YrMean'].strip() == '' else row['1YrMean'].strip()
                riskgrade = row["RiskGrade"]

                Remarks = None

                sql_plan_id = db_session.query(Plans.Plan_Id).filter(Plans.Plan_Code == schemecode).filter(Plans.Is_Deleted != 1).order_by(desc(Plans.Plan_Id)).first()
                if sql_plan_id:
                    save_planproductmapping(db_session, sql_plan_id.Plan_Id, PRODUCT_ID, user_id, TODAY)            
                else:
                    Remarks = "Plans are not available in system."

                if date > TODAY:
                    Remarks = "Factsheet cannot be uploaded for Future date."
                
                if not date:
                    Remarks = "Factsheet transaction date not available."

                if Remarks == None:
                    update_values = {
                    FactSheet.Is_Deleted : 1,
                    FactSheet.Updated_By : user_id,
                    FactSheet.Updated_Date : TODAY,
                    }
                    sql_factsheet_del = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id==sql_plan_id.Plan_Id, FactSheet.TransactionDate == date, FactSheet.Is_Deleted != 1)).update(update_values)
                    db_session.commit()
                    remark = 'Flagged Deleted and uploaded new records Successfully.'

                    if net_assets_rscr == None:
                        sql_fact = db_session.query(func.max(FactSheet.TransactionDate)).filter(FactSheet.Plan_Id == sql_plan_id.Plan_Id, FactSheet.NetAssets_Rs_Cr > 0, FactSheet.Is_Deleted != 1, FactSheet.TransactionDate < date).first()

                        if sql_fact:
                            sql_facts = db_session.query(FactSheet.NetAssets_Rs_Cr).filter(FactSheet.Plan_Id == sql_plan_id.Plan_Id, FactSheet.Is_Deleted != 1, FactSheet.TransactionDate == sql_fact[0]).first()

                            if sql_facts != None:
                                net_assets_rscr = sql_facts.NetAssets_Rs_Cr

                    factsheet_insert = FactSheet()
                    factsheet_insert.Plan_Id = sql_plan_id.Plan_Id
                    factsheet_insert.TransactionDate = date
                    factsheet_insert.WeekHigh_52_Rs = week_highrs52
                    factsheet_insert.WeekLow_52_Rs = week_lowrs52
                    factsheet_insert.TotalStocks = total_stocks
                    factsheet_insert.PortfolioP_BRatio = portfolio_pb_ratio
                    factsheet_insert.PortfolioP_ERatio = portfolio_pe_ratio
                    factsheet_insert.StandardDeviation = standard_deviation
                    factsheet_insert.SharpeRatio = sharpe_ratio
                    factsheet_insert.Beta = beta
                    factsheet_insert.R_Squared = r_squared 
                    factsheet_insert.Alpha = alpha
                    factsheet_insert.Mean = mean
                    factsheet_insert.StandardDeviation_1Yr = yr_standard_deviation1
                    factsheet_insert.SharpeRatio_1Yr = yrsharperatio1
                    factsheet_insert.Beta_1Yr = yr_beta1
                    factsheet_insert.R_Squared_1Yr = yr_rsquared1
                    factsheet_insert.Alpha_1Yr = yr_alpha1
                    factsheet_insert.Mean_1Yr = yr_mean1
                    factsheet_insert.Equity = equity
                    factsheet_insert.Debt = debt
                    factsheet_insert.Cash = cash
                    factsheet_insert.RANKING_RANK_1MONTH = ranking_rank1month
                    factsheet_insert.COUNT_1MONTH = count1month
                    factsheet_insert.RANKING_RANK_3MONTH = ranking_rank3month
                    factsheet_insert.COUNT_3MONTH = count3month
                    factsheet_insert.RANKING_RANK_6MONTH = ranking_rank6month
                    factsheet_insert.COUNT_6MONTH = count6month
                    factsheet_insert.RANKING_RANK_1YEAR = ranking_rank1year
                    factsheet_insert.COUNT_1YEAR = count1year
                    factsheet_insert.RANKING_RANK_3YEAR = ranking_rank3year
                    factsheet_insert.COUNT_3YEAR = count3year
                    factsheet_insert.RANKING_RANK_5YEAR = ranking_rank5year
                    factsheet_insert.COUNT_5YEAR = count5year
                    factsheet_insert.SCHEME_RETURNS_1MONTH = scheme_returns1month
                    factsheet_insert.SCHEME_RETURNS_3MONTH = scheme_returns3month
                    factsheet_insert.SCHEME_RETURNS_6MONTH = scheme_returns6month
                    factsheet_insert.SCHEME_RETURNS_1YEAR = scheme_returns1year
                    factsheet_insert.SCHEME_RETURNS_3YEAR = scheme_returns3year
                    factsheet_insert.SCHEME_RETURNS_5YEAR = scheme_returns5year
                    factsheet_insert.SCHEME_RETURNS_since_inception = scheme_returns_since_inception
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_1MONTH = scheme_benchmark_returns1month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_3MONTH = scheme_benchmark_returns3month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_6MONTH = scheme_benchmark_returns6month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_1YEAR = scheme_benchmark_returns1year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_3YEAR = scheme_benchmark_returns3year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_5YEAR = scheme_benchmark_returns5year
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH = scheme_category_average_returns1month
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH = scheme_category_average_returns3month
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH = scheme_category_average_returns6month
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR = scheme_category_average_returns1year
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR = scheme_category_average_returns3year
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR = scheme_category_average_returns5year
                    factsheet_insert.Bill_Rediscounting = bill_rediscounting
                    factsheet_insert.Cash_Equivalent = cash_equivalent
                    factsheet_insert.Term_Deposit = term_deposit
                    factsheet_insert.Bonds_Debentures = bonds_debentures
                    factsheet_insert.Cash_And_Cash_Equivalent = cashandcash_equivalent
                    factsheet_insert.CP_CD = cpcd
                    factsheet_insert.GOI_Securities = goi_securities
                    factsheet_insert.MutualFunds_Debt = mutual_funds_debt
                    factsheet_insert.Securitised_Debt = securitised_debt
                    factsheet_insert.ShortTerm_Debt = short_term_debt
                    factsheet_insert.Term_Deposits = term_deposit
                    factsheet_insert.Treasury_Bills = treasury_bills
                    factsheet_insert.NetAssets_Rs_Cr = net_assets_rscr
                    factsheet_insert.AvgMktCap_Rs_Cr = avg_mkt_cap_rscr
                    factsheet_insert.SourceFlag = "ULIP"
                    factsheet_insert.Is_Deleted = 0
                    factsheet_insert.Portfolio_Dividend_Yield = None
                    factsheet_insert.Churning_Ratio = None
                    factsheet_insert.Portfolio_Sales_Growth_Estimated = None
                    factsheet_insert.Portfolio_PAT_Growth_Estimated = None
                    factsheet_insert.Portfolio_Earning_Growth_Estimated = None
                    factsheet_insert.Portfolio_Forward_PE = None
                    factsheet_insert.Created_By = user_id
                    factsheet_insert.Created_Date = TODAY
                    factsheet_insert.Updated_By = None
                    factsheet_insert.Updated_Date = None
                    factsheet_insert.Risk_Grade = riskgrade

                    db_session.add(factsheet_insert)
                    db_session.flush()

                    inserted_factsheetid = factsheet_insert.FactSheet_Id
                    db_session.commit()

                    #update heartbeat - plans
                    update_plan_heartbeat = {
                        Plans.Heartbeat_Date : TODAY
                    }
                    
                    sql_heartbeat_update = db_session.query(Plans).filter(Plans.Plan_Id == sql_plan_id.Plan_Id).update(update_plan_heartbeat)
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
                        sql_factsheet_upd = db_session.query(FactSheet)\
                                                            .filter(FactSheet.FactSheet_Id == inserted_factsheetid)\
                                                            .update(update_values)
                        db_session.commit()

                    Remarks = "Uploaded Successfully."
            except Exception as ex:
                ex_record = {}
                ex_record['CMOTS_SchemeCode'] = row['SchemeCode']
                ex_record['CMOTS_SFINCode'] = row['SFINCode']
                ex_record['CMOTS_SchemeName'] = row['SchemeName']
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
            exception_file_path = get_rel_path(output_file_path, __file__).lower().replace('factsheetinsurance', 'factsheetinsurance_exception')
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
            schedule_email_activity(db_session, 'devteam@finalyca.com', '', '', F"ULIP - Factsheet - Exception file{TODAY.strftime('%Y-%b-%d')}", F"ULIP - Fatsheet - Exception file{TODAY.strftime('%Y-%b-%d')}", attachements)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/FactSheetInsurance_26-07-2023-05-22-42.csv"
    READ_PATH = "read/FactSheetInsurance_26-07-2023-05-22-42.csv"
    import_ulip_factsheet_file(FILE_PATH, READ_PATH, USER_ID)

