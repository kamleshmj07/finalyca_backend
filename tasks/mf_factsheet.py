import csv
from datetime import datetime as dt
from sqlalchemy import and_, desc, func
import os
import pandas as pd
from bizlogic.importer_helper import get_rel_path, write_csv, save_planproductmapping

from utils import *
from utils.finalyca_store import *
from fin_models import *
from bizlogic.fund_portfolio_analysis import generate_portfolio_analysis
from bizlogic.common_helper import schedule_email_activity

def import_mf_factsheet_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))
    items = list()
   
    PRODUCT_ID = 1
    TODAY = dt.today()

    header = ["AMCCode","SchemeCode","SchemeName","Date","ISIN","52 Week High (Rs)","52 Week Low (Rs)","TotalStocks","PortfolioPBRatio","PortfolioPERatio","3YEarningsGrowth","AvgCreditRating","Modified Duration_Yrs","StandardDeviation","SharpeRatio","Beta","RSquared","Alpha","Mean","Sortino","Equity","Debt","Cash","RANKING_RANK_1MONTH","COUNT_1MONTH","RANKING_RANK_3MONTH","COUNT_3MONTH","RANKING_RANK_6MONTH","COUNT_6MONTH","RANKING_RANK_1YEAR","COUNT_1YEAR","RANKING_RANK_3YEAR","COUNT_3YEAR","RANKING_RANK_5YEAR","COUNT_5YEAR","SIP_RETURNS_1YEAR","SIP_RETURNS_3YEAR","SIP_RETURNS_5YEAR","SIP_RANKINGS_1 YEAR","SIP_RANKINGS_3 YEAR","SIP_RANKINGS_5 YEAR","SCHEME_RETURNS_1MONTH","SCHEME_RETURNS_3MONTH","SCHEME_RETURNS_6MONTH","SCHEME_RETURNS_1YEAR","SCHEME_RETURNS_3YEAR","SCHEME_RETURNS_5YEAR","SCHEME_RETURNS_SinceInception","SCHEME_BENCHMARK_RETURNS_1MONTH","SCHEME_BENCHMARK_RETURNS_3MONTH","SCHEME_BENCHMARK_RETURNS_6MONTH","SCHEME_BENCHMARK_RETURNS_1YEAR","SCHEME_BENCHMARK_RETURNS_3YEAR","SCHEME_BENCHMARK_RETURNS_5YEAR","SCHEME_BENCHMARK_RETURNS_SI","SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH","SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH","SCHEME_CATEGORY_AVERAGE_RETURNS_ 6MONTH","SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR","SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR","SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR","RiskGrade","ReturnGrade","ExitLoad","ExpenseRatio","SOV","AAA","A1+","AA","AandBelow","BillRediscounting","CashEquivalent","TermDeposit","Unrated_Others","Bonds_Debentures","CashAndCashEquivalent","CP_CD","GOISecurities","MutualFundsDebt","SecuritisedDebt","ShortTermDebt","TermDeposits","TreasuryBills","VRRatings","NetAssets_Rs_Cr","AvgMktCap_Rs_Cr","AvgMaturity_Yrs","1YrStandard_Deviation","1YrSharpe_Ratio","1YrBeta","1YrRSquared","1YrAlpha","1YrMean","1YrSortino","SCM_SCHEME_CD", "Macaulay_Duration_Yrs", "Yield_To_Maturity", "Remarks"]
    exceptions_data = []

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f, delimiter="|")
        for row in csvreader:
            try:
                amccode = row["AMCCode"]
                schemecode = row["SchemeCode"] + "_01"
                schemename = row["SchemeName"]
                date = None if row["Date"].strip() =="" else dt.strptime(row["Date"].replace("/","-"), '%d-%m-%Y')
                isin = row["ISIN"] 
                weekhigh_52 = None if row["52 Week High (Rs)"].strip() == "" else row["52 Week High (Rs)"].replace("%", "")
                weeklow_52 = None if row["52 Week Low (Rs)"].strip() == "" else row["52 Week Low (Rs)"].replace("%", "")
                totalstocks = None if row["TotalStocks"].strip() == "" else row["TotalStocks"]
                portfoliopbratio = None if row["PortfolioPBRatio"].strip() == "" else row["PortfolioPBRatio"]
                portfolioperatio = None if row["PortfolioPERatio"].strip() == "" else row["PortfolioPERatio"]
                earning_growth_3y = None if row["3YEarningsGrowth"].strip() == "" else row["3YEarningsGrowth"].replace("%", "")
                avgcreditrating = None if row["AvgCreditRating"].strip() == "" else row["AvgCreditRating"].replace("%", "")
                modified_duration = None if row["Modified Duration_Yrs"].strip() == "" else row["Modified Duration_Yrs"].replace("%", "")
                macaulay_duration_yrs = None if row["Macaulay_Duration_Yrs"].strip() == "" else row["Macaulay_Duration_Yrs"]
                yield_to_maturity = None if row["Yield_To_Maturity"].strip() == "" else row["Yield_To_Maturity"].replace("%", "")
                standard_deviation = None if row["StandardDeviation"].strip() == "" else row["StandardDeviation"].replace("%", "")
                sharpe_ratio = None if row["SharpeRatio"].strip() == "" else row["SharpeRatio"].replace("%", "")
                beta = None if row["Beta"].strip() == "" else row["Beta"].replace("%", "")
                rsquared = None if row["RSquared"].strip() == "" else row["RSquared"].replace("%", "")
                alpha = None if row["Alpha"].strip() == "" else row["Alpha"].replace("%", "")
                mean = None if row["Mean"].strip() == "" else row["Mean"].replace("%", "")
                Sortino = None if row["Sortino"].strip() == "" else row["Sortino"]
                equity = None if row["Equity"].strip() == "" else row["Equity"].replace("%", "")
                debt = None if row["Debt"].strip() == "" else row["Debt"].replace("%", "") 
                cash = None if row["Cash"].strip() == "" else row["Cash"].replace("%", "") 
                ranking_rank1month = None if row["RANKING_RANK_1MONTH"].strip() == "" else row["RANKING_RANK_1MONTH"]
                count1month = None if row["COUNT_1MONTH"].strip() == "" else row["COUNT_1MONTH"]
                ranking_rank3month = None if row["RANKING_RANK_3MONTH"].strip() == "" else row["RANKING_RANK_3MONTH"]
                count3month = None if row["COUNT_3MONTH"].strip() == "" else row["COUNT_3MONTH"]
                ranking_rank6month = None if row["RANKING_RANK_6MONTH"].strip() == "" else row["RANKING_RANK_6MONTH"]
                count6moth = None if row["COUNT_6MONTH"].strip() == "" else row["COUNT_6MONTH"]
                ranking_rank1year = None if row["RANKING_RANK_1YEAR"].strip() == "" else row["RANKING_RANK_1YEAR"]
                count1year = None if row["COUNT_1YEAR"].strip() == "" else row["COUNT_1YEAR"]
                ranking_rank3year = None if row["RANKING_RANK_3YEAR"].strip() == "" else row["RANKING_RANK_3YEAR"]
                count3year = None if row["COUNT_3YEAR"].strip() == "" else row["COUNT_3YEAR"]
                ranking_rank5year = None if row["RANKING_RANK_5YEAR"].strip() == "" else row["RANKING_RANK_5YEAR"]
                count5year = None if row["COUNT_5YEAR"].strip() == "" else row["COUNT_5YEAR"]
                sipreturn1year = None if row["SIP_RETURNS_1YEAR"].strip() == "" else row["SIP_RETURNS_1YEAR"]
                sipreturn3year = None if row["SIP_RETURNS_3YEAR"].strip() == "" else row["SIP_RETURNS_3YEAR"]
                sipreturn5year = None if row["SIP_RETURNS_5YEAR"].strip() == "" else row["SIP_RETURNS_5YEAR"]
                sipranking1year = None if row["SIP_RANKINGS_1 YEAR"].strip() == "" else row["SIP_RANKINGS_1 YEAR"]
                sipranking3year = None if row["SIP_RANKINGS_3 YEAR"].strip() == "" else row["SIP_RANKINGS_3 YEAR"]
                sipranking5year = None if row["SIP_RANKINGS_5 YEAR"].strip() == "" else row["SIP_RANKINGS_5 YEAR"]
                schemereturn1month = None if row["SCHEME_RETURNS_1MONTH"].strip() == "" else row["SCHEME_RETURNS_1MONTH"].replace("%", "")
                schemereturn3month = None if row["SCHEME_RETURNS_3MONTH"].strip() == "" else row["SCHEME_RETURNS_3MONTH"].replace("%", "")
                schemereturn6month = None if row["SCHEME_RETURNS_6MONTH"].strip() == "" else row["SCHEME_RETURNS_6MONTH"].replace("%", "")
                schemereturn1year = None if row["SCHEME_RETURNS_1YEAR"].strip() == "" else row["SCHEME_RETURNS_1YEAR"].replace("%", "")
                schemereturn3year = None if row["SCHEME_RETURNS_3YEAR"].strip() == "" else row["SCHEME_RETURNS_3YEAR"].replace("%", "")
                schemereturn5year = None if row["SCHEME_RETURNS_5YEAR"].strip() == "" else row["SCHEME_RETURNS_5YEAR"].replace("%", "")
                schemereturnsi = None if row["SCHEME_RETURNS_SinceInception"].strip() == "" else row["SCHEME_RETURNS_SinceInception"].replace("%", "")
                
                schemebenchmarkreturn1month = None if row["SCHEME_BENCHMARK_RETURNS_1MONTH"].strip() == "" else row["SCHEME_BENCHMARK_RETURNS_1MONTH"].replace("%", "")
                schemebenchmarkreturn3month = None if row["SCHEME_BENCHMARK_RETURNS_3MONTH"].strip() == "" else row["SCHEME_BENCHMARK_RETURNS_3MONTH"].replace("%", "")
                schemebenchmarkreturn6month = None if row["SCHEME_BENCHMARK_RETURNS_6MONTH"].strip() == "" else row["SCHEME_BENCHMARK_RETURNS_6MONTH"].replace("%", "")
                schemebenchmarkreturn1year = None if row["SCHEME_BENCHMARK_RETURNS_1YEAR"].strip() == "" else row["SCHEME_BENCHMARK_RETURNS_1YEAR"].replace("%", "")
                schemebenchmarkreturn3year = None if row["SCHEME_BENCHMARK_RETURNS_3YEAR"].strip() == "" else row["SCHEME_BENCHMARK_RETURNS_3YEAR"].replace("%", "")
                schemebenchmarkreturn5year = None if row["SCHEME_BENCHMARK_RETURNS_5YEAR"].strip() == "" else row["SCHEME_BENCHMARK_RETURNS_5YEAR"].replace("%", "")
                schemebenchmarkreturnsi = None if row["SCHEME_BENCHMARK_RETURNS_SI"].strip() == "" else row["SCHEME_BENCHMARK_RETURNS_SI"].replace("%", "")

                schemecategoryaveragereturn1month = None if row["SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH"].strip() == "" else row["SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH"].replace("%", "")
                schemecategoryaveragereturn3month = None if row["SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH"].strip() == "" else row["SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH"].replace("%", "")
                schemecategoryaveragereturn6month = None if row["SCHEME_CATEGORY_AVERAGE_RETURNS_ 6MONTH"].strip() == "" else row["SCHEME_CATEGORY_AVERAGE_RETURNS_ 6MONTH"].replace("%", "")
                schemecategoryaveragereturn1year = None if row["SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR"].strip() == "" else row["SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR"].replace("%", "")
                schemecategoryaveragereturn3year = None if row["SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR"].strip() == "" else row["SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR"].replace("%", "")
                schemecategoryaveragereturn5year = None if row["SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR"].strip() == "" else row["SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR"].replace("%", "")
                    
                riskgrade = row["RiskGrade"]
                returngrade = row["ReturnGrade"]
                exitload = row["ExitLoad"]
                expenseratio = None if row["ExpenseRatio"].strip() == "" else row["ExpenseRatio"].replace("%", "")
                sov = None if row["SOV"].strip() == "" else row["SOV"].replace("%", "")
                aaa = None if row["AAA"].strip() == "" else row["AAA"].replace("%", "")
                a1plus = None if row["A1+"].strip() == "" else row["A1+"].replace("%", "")
                aa = None if row["AA"].strip() == "" else row["AA"].replace("%", "")
                aandbelow = None if row["AandBelow"].strip() == "" else row["AandBelow"].replace("%", "")
                billredicounting = None if row["BillRediscounting"].strip() == "" else row["BillRediscounting"].replace("%", "")
                cashequivalent = None if row["CashEquivalent"].strip() == "" else row["CashEquivalent"].replace("%", "")
                termdeposit = None if row["TermDeposit"].strip() == "" else row["TermDeposit"].replace("%", "")
                unratedothers = None if row["Unrated_Others"].strip() == "" else row["Unrated_Others"].replace("%", "")
                bondsdebentures = None if row["Bonds_Debentures"].strip() == "" else row["Bonds_Debentures"].replace("%", "")
                cashandcashequivalent = None if row["CashAndCashEquivalent"].strip() == "" else row["CashAndCashEquivalent"].replace("%", "")
                cpcd = None if row["CP_CD"].strip() == "" else row["CP_CD"].replace("%", "")
                goisecurities = None if row["GOISecurities"].strip() == "" else row["GOISecurities"].replace("%", "")
                mutualfunddebts = None if row["MutualFundsDebt"].strip() == "" else row["MutualFundsDebt"].replace("%", "")
                securitiseddebt = None if row["SecuritisedDebt"].strip() == "" else row["SecuritisedDebt"].replace("%", "")
                shorttermdebt = None if row["ShortTermDebt"].strip() == "" else row["ShortTermDebt"].replace("%", "")
                termdeposit = None if row["TermDeposits"].strip() == "" else row["TermDeposits"].replace("%", "")
                treasurybills = None if row["TreasuryBills"].strip() == "" else row["TreasuryBills"].replace("%", "")
                vrratings = None if row["VRRatings"].strip() == "" else row["VRRatings"].replace("%", "")
                netassetrscr = None if row["NetAssets_Rs_Cr"].strip() == "" else row["NetAssets_Rs_Cr"].replace("%", "")
                avgmktcaprscr = None if row["AvgMktCap_Rs_Cr"].strip() == "" else row["AvgMktCap_Rs_Cr"].replace("%", "")
                avgmaturityyrs = None if row["AvgMaturity_Yrs"].strip() == "" else row["AvgMaturity_Yrs"].replace("%", "")
                standarddeviation1yr = None if row["1YrStandard_Deviation"].strip() == "" else row["1YrStandard_Deviation"].replace("%", "")
                sharperation1yr = None if row["1YrSharpe_Ratio"].strip() == "" else row["1YrSharpe_Ratio"].replace("%", "")
                beta1yr = None if row["1YrBeta"].strip() == "" else row["1YrBeta"].replace("%", "")
                rsquared1yr = None if row["1YrRSquared"].strip() == "" else row["1YrRSquared"].replace("%", "")
                alpha1yr = None if row["1YrAlpha"].strip() == "" else row["1YrAlpha"].replace("%", "")
                mean1yr = None if row["1YrMean"].strip() == "" else row["1YrMean"].replace("%", "")
                sortino1yr = None if row["1YrSortino"].strip() == "" else row["1YrSortino"].replace("%", "")
                
                Remarks = None

                sql_plan_id = db_session.query(Plans.Plan_Id).filter(Plans.Plan_Code == schemecode).filter(Plans.Is_Deleted != 1).order_by(desc(Plans.Plan_Id)).first()

                if sql_plan_id:
                    save_planproductmapping(db_session, sql_plan_id.Plan_Id, PRODUCT_ID, user_id, TODAY)
                else:
                    Remarks = "Plans are not available in system."

                if date and date > TODAY:
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
                    Remarks = 'Flagged Deleted and uploaded new records Successfully.'

                    if netassetrscr == None:
                        sql_fact = db_session.query(func.max(FactSheet.TransactionDate)).filter(FactSheet.Plan_Id == sql_plan_id.Plan_Id, FactSheet.NetAssets_Rs_Cr > 0, FactSheet.Is_Deleted != 1, FactSheet.TransactionDate < date).first()

                        if sql_fact:
                            sql_facts = db_session.query(FactSheet.NetAssets_Rs_Cr).filter(FactSheet.Plan_Id == sql_plan_id.Plan_Id, FactSheet.Is_Deleted != 1, FactSheet.TransactionDate == sql_fact[0]).first()

                            if sql_facts != None:
                                netassetrscr = sql_facts.NetAssets_Rs_Cr

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
                    factsheet_insert.SCHEME_RETURNS_3YEAR = schemereturn3year
                    factsheet_insert.SCHEME_RETURNS_5YEAR = schemereturn5year
                    factsheet_insert.SCHEME_RETURNS_since_inception = schemereturnsi
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_1MONTH = schemebenchmarkreturn1month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_3MONTH = schemebenchmarkreturn3month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_6MONTH = schemebenchmarkreturn6month
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_1YEAR = schemebenchmarkreturn1year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_3YEAR = schemebenchmarkreturn3year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_5YEAR = schemebenchmarkreturn5year
                    factsheet_insert.SCHEME_BENCHMARK_RETURNS_SI = schemebenchmarkreturnsi
                    factsheet_insert.Exit_Load = exitload
                    factsheet_insert.NetAssets_Rs_Cr = netassetrscr
                    factsheet_insert.AvgMktCap_Rs_Cr = avgmktcaprscr
                    factsheet_insert.AvgMaturity_Yrs = avgmaturityyrs
                    factsheet_insert.Portfolio_Dividend_Yield = None
                    factsheet_insert.TotalStocks = totalstocks
                    factsheet_insert.PortfolioP_BRatio = portfoliopbratio
                    factsheet_insert.PortfolioP_ERatio = portfolioperatio
                    factsheet_insert.WeekHigh_52_Rs = weekhigh_52
                    factsheet_insert.WeekLow_52_Rs = weeklow_52
                    factsheet_insert.EarningsGrowth_3Yrs_Percent = earning_growth_3y
                    factsheet_insert.AvgCreditRating = avgcreditrating
                    factsheet_insert.ModifiedDuration_yrs = modified_duration
                    factsheet_insert.Macaulay_Duration_Yrs = macaulay_duration_yrs
                    factsheet_insert.Yield_To_Maturity = yield_to_maturity
                    factsheet_insert.StandardDeviation = standard_deviation
                    factsheet_insert.SharpeRatio = sharpe_ratio
                    factsheet_insert.Beta = beta
                    factsheet_insert.R_Squared = rsquared
                    factsheet_insert.Alpha = alpha
                    factsheet_insert.Mean = mean
                    factsheet_insert.Sortino = Sortino
                    factsheet_insert.StandardDeviation_1Yr = standarddeviation1yr
                    factsheet_insert.SharpeRatio_1Yr = sharperation1yr
                    factsheet_insert.Beta_1Yr = beta1yr
                    factsheet_insert.R_Squared_1Yr = rsquared1yr
                    factsheet_insert.Alpha_1Yr = alpha1yr
                    factsheet_insert.Mean_1Yr = mean1yr
                    factsheet_insert.Sortino_1Yr = sortino1yr
                    factsheet_insert.RANKING_RANK_1MONTH = ranking_rank1month
                    factsheet_insert.COUNT_1MONTH = count1month
                    factsheet_insert.RANKING_RANK_3MONTH = ranking_rank3month
                    factsheet_insert.COUNT_3MONTH = count3month
                    factsheet_insert.RANKING_RANK_6MONTH = ranking_rank6month
                    factsheet_insert.COUNT_6MONTH = count6moth
                    factsheet_insert.RANKING_RANK_1YEAR = ranking_rank1year
                    factsheet_insert.COUNT_1YEAR = count1year
                    factsheet_insert.RANKING_RANK_3YEAR = ranking_rank3year
                    factsheet_insert.COUNT_3YEAR = count3year
                    factsheet_insert.RANKING_RANK_5YEAR = ranking_rank5year
                    factsheet_insert.COUNT_5YEAR = count5year
                    factsheet_insert.SIP_RETURNS_1YEAR = sipreturn1year
                    factsheet_insert.SIP_RETURNS_3YEAR = sipreturn3year
                    factsheet_insert.SIP_RETURNS_5YEAR = sipreturn5year
                    factsheet_insert.SIP_RANKINGS_1YEAR = sipranking1year
                    factsheet_insert.SIP_RANKINGS_3YEAR = sipranking3year
                    factsheet_insert.SIP_RANKINGS_5YEAR = sipranking5year            
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH = schemecategoryaveragereturn1month
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH = schemecategoryaveragereturn3month
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH = schemecategoryaveragereturn6month
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR = schemecategoryaveragereturn1year
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR = schemecategoryaveragereturn3year
                    factsheet_insert.SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR = schemecategoryaveragereturn5year
                    factsheet_insert.Risk_Grade = riskgrade
                    factsheet_insert.Return_Grade = returngrade
                    factsheet_insert.ExpenseRatio = expenseratio
                    factsheet_insert.SOV = sov
                    factsheet_insert.AAA = aaa
                    factsheet_insert.A1_Plus = a1plus
                    factsheet_insert.AA = aa
                    factsheet_insert.A_and_Below = aandbelow
                    factsheet_insert.Bill_Rediscounting = billredicounting
                    factsheet_insert.Cash_Equivalent = cashequivalent
                    factsheet_insert.Term_Deposit = termdeposit
                    factsheet_insert.Unrated_Others = unratedothers
                    factsheet_insert.Bonds_Debentures = bondsdebentures
                    factsheet_insert.Cash_And_Cash_Equivalent = cashandcashequivalent
                    factsheet_insert.CP_CD =  cpcd
                    factsheet_insert.GOI_Securities = goisecurities
                    factsheet_insert.MutualFunds_Debt = mutualfunddebts
                    factsheet_insert.Securitised_Debt = securitiseddebt
                    factsheet_insert.ShortTerm_Debt = shorttermdebt
                    factsheet_insert.Term_Deposits = termdeposit 
                    factsheet_insert.Treasury_Bills = treasurybills
                    factsheet_insert.VRRatings = vrratings
                    factsheet_insert.Churning_Ratio = None 
                    factsheet_insert.Portfolio_Sales_Growth_Estimated = None
                    factsheet_insert.Portfolio_PAT_Growth_Estimated = None
                    factsheet_insert.Portfolio_Earning_Growth_Estimated = None
                    factsheet_insert.Portfolio_Earning_Growth_Estimated = None            
                    factsheet_insert.SourceFlag = "MF"
                    factsheet_insert.Is_Deleted = 0
                    factsheet_insert.Created_By = user_id
                    factsheet_insert.Created_Date = TODAY

                    db_session.add(factsheet_insert)
                    db_session.flush()

                    inserted_factsheetid = factsheet_insert.FactSheet_Id
                    db_session.commit()

                    #update heartbeat - plans
                    update_plan_heartbeat = {
                        Plans.Heartbeat_Date : TODAY
                    }
                    
                    sql_heartbeat_update = db_session.query(Plans)\
                                                        .filter(Plans.Plan_Id == sql_plan_id.Plan_Id)\
                                                        .update(update_plan_heartbeat)
                    db_session.commit()

                    sql_logics_plan = db_session.query(Fund.AutoPopulate).join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(Plans.Plan_Id == sql_plan_id.Plan_Id).one_or_none()

                    if sql_logics_plan.AutoPopulate:
                        generate_portfolio_analysis(db_session, inserted_factsheetid,user_id, False, True)
                        
                        #Update IsPortfolioProcessed flag
                        update_values = {
                        FactSheet.IsPortfolioProcessed : 1
                        }
                        sql_factsheet_upd = db_session.query(FactSheet).filter(FactSheet.FactSheet_Id == inserted_factsheetid).update(update_values)
                        db_session.commit()

                    Remarks = "Uploaded Successfully."
            except Exception as ex:
                ex_record = {}
                ex_record['CMOTS_SchemeCode'] = schemecode
                ex_record['CMOTS_ISIN'] = row['ISIN']
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
            exception_file_path = get_rel_path(output_file_path, __file__).lower().replace('factsheet', 'factsheet_exception')
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
            schedule_email_activity(db_session, 'devteam@finalyca.com', '', '', F"MF - Factsheet - Exception file{TODAY.strftime('%Y-%b-%d')}", F"MF - Factsheet - Exception file{TODAY.strftime('%Y-%b-%d')}", attachements)

    write_csv(output_file_path, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/Factsheet_19-06-2023-04-00-18.csv"
    READ_PATH = "read/Factsheet_19-06-2023-04-00-18.csv"
    import_mf_factsheet_file(FILE_PATH, READ_PATH, USER_ID)