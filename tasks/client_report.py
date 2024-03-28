from datetime import date, timedelta
from datetime import datetime as dt
import os
from utils.finalyca_store import *
from sqlalchemy import func, case, and_, desc, or_
import pandas as pd
from dateutil import parser
from werkzeug.exceptions import BadRequest
from bizlogic.importer_helper import get_sectorweightsdata, get_marketcap_composition, get_portfolio_date, get_portfolio_characteristics, get_fundriskratio_data, get_scheme_details, get_performancetrend_data, getfundmanager, get_pms_aif_aum_fundwise, get_mf_ulip_aum_fundwise, get_pms_aif_nav_fundwise, get_newly_onboarded_plans, get_plans_fo_which_data_not_received, check_if_fund_contains_nav_and_factsheet_by_date, get_plans_list_product_wise
from bizlogic.common_helper import get_fund_historic_performance, get_plan_meta_info, get_last_transactiondate, get_detailed_fund_holdings


today = dt.today()
first = today.replace(day=1)
current_month = first - timedelta(days=1)

first_current = current_month.replace(day=1)
previous_month = first_current - timedelta(days=1)

def generate_fund_wise_report(db_session, plans=[], show_fund_details_section = True, transaction_date=None, report_name=''):
    today = dt.today()
    
    df_fund_details = pd.DataFrame()
    df_sector = pd.DataFrame()
    df_fund_name = pd.DataFrame()
    df_marketcap = pd.DataFrame()
    df_top = pd.DataFrame()
    df_valuation = pd.DataFrame()
    df_portfolio_characteristics = pd.DataFrame()
    df_risk_analysis = pd.DataFrame()
    df_historic_performance = pd.DataFrame()
    df_performancetrend = pd.DataFrame()
    fund_details_list = list()
    

    for plan_id in plans:
        last_transaction_date = get_last_transactiondate( db_session, plan_id)
        if transaction_date != last_transaction_date.date():
            continue

        portfolio_date = get_portfolio_date(db_session, plan_id, transaction_date)

        portfolio_characteristics = get_portfolio_characteristics(db_session, plan_id, transaction_date)
        performancetrend_data = get_performancetrend_data(db_session, plan_id, transaction_date)
        fundmanager_data = getfundmanager(db_session,plan_id)
        fund_manager_name = ''

        for fm in fundmanager_data:
            fund_manager_name = fund_manager_name + fm['fund_manager_name'] + ', '

        if show_fund_details_section:
            #get scheme details
            scheme_details = get_scheme_details(db_session, plan_id, transaction_date)
            if scheme_details:
                mydict = dict()
                mydict["PMS Scheme"] = scheme_details["plan_name"]
                mydict["Inception Date"] = scheme_details["inception_date"].strftime('%d-%b-%Y')
                mydict["AUM (crs)"] = float(round(scheme_details["aum"],2))
                mydict["Overview/ Fund Strategy"] = scheme_details["investment_strategy"]
                mydict["No of stocks"] = float(portfolio_characteristics["total_stocks"]) if portfolio_characteristics["total_stocks"] else 'NA' if portfolio_characteristics else 'NA'
                mydict["1 yr return"] = float(performancetrend_data["scheme_ret_1y"]) if performancetrend_data["scheme_ret_1y"] else 'NA' if performancetrend_data else 'NA'
                mydict["3 yrs return"] = float(performancetrend_data["scheme_ret_3y"]) if performancetrend_data["scheme_ret_3y"] else 'NA' if performancetrend_data else 'NA'
                mydict["5 yrs return"] = float(performancetrend_data["scheme_ret_5y"]) if performancetrend_data["scheme_ret_5y"] else 'NA' if performancetrend_data else 'NA'
                mydict["Since Inception"] = float(performancetrend_data["scheme_ret_ince"]) if performancetrend_data["scheme_ret_ince"] else 'NA' if performancetrend_data else 'NA'
                mydict["Benchmark"] = scheme_details["benchmark_indices_name"]
                mydict["Fund Flow (Inflow)"] = ''
                mydict["Fund Flow (Outflow)"] = ''
                mydict["Net Flow"] = ''
                mydict["Fund Flow_FYTD (Inflow)"] = ''
                mydict["Fund Flow_FYTD (Outflow)"] = ''
                mydict["Net Flow_FYTD"] = ''
                mydict["Fund Manager"] = fund_manager_name
                mydict["Fees Structure"] = scheme_details["fees_structure"]
                mydict["Minimum Investment"] = round(scheme_details["min_purchase_amount"])

                fund_details_list.append(mydict)


        #get sector weight
        resp = get_sectorweightsdata(db_session, plan_id, transaction_date, composition_for = 'fund_level')
        sectorweights_data = list()
        if resp:
            for res in resp:
                mydict = dict()
                mydict["Sectors"] = res["sector_name"]
                mydict["% holding"] = float(res["sector_weight"]) if res["sector_weight"] else 'NA'
                sectorweights_data.append(mydict)

        sec_data = pd.DataFrame(sectorweights_data)

        sec_data[' ']= ' '

        if df_sector.empty:
            df_sector = sec_data
        else:
            df_sector = pd.concat([df_sector, sec_data], axis=1, join="outer")

        #Get market cap
        resp = get_marketcap_composition(db_session, plan_id, transaction_date, 'fund_level')
        marketcap_data = list()

        for res in resp:
            mydict = dict()
            mydict["Market cap"] = 'Large Cap'
            mydict["% holding"] = float(res['large_cap']) if res["large_cap"] != 'NA' and res["large_cap"] else 'NA'
            marketcap_data.append(mydict)

            mydict = dict()
            mydict["Market cap"] = 'Mid Cap'
            mydict["% holding"] = float(res['mid_cap']) if res["mid_cap"] != 'NA' and res["mid_cap"] else 'NA'
            marketcap_data.append(mydict)

            mydict = dict()
            mydict["Market cap"] = 'Small Cap'
            mydict["% holding"] = float(res['small_cap']) if res["small_cap"] != 'NA' and res["small_cap"] else 'NA'
            marketcap_data.append(mydict)

            mydict = dict()
            mydict["Market cap"] = 'Unlisted'
            mydict["% holding"] = float(res['unlisted']) if res["unlisted"] != 'NA' and res["unlisted"] else 'NA'
            marketcap_data.append(mydict)

        marcap_data = pd.DataFrame(marketcap_data)

        marcap_data[' ']= ' '

        if df_marketcap.empty:
            df_marketcap = marcap_data
        else:
            df_marketcap = pd.concat([df_marketcap, marcap_data], axis=1, join="outer")

        
        #get Top 10 & 5 holdings
        top5_perc = None
        top10_perc = None

        consolidated_holdings = get_detailed_fund_holdings(db_session, plan_id, None, portfolio_date, True)
        df_consolidated_holdings = pd.DataFrame(consolidated_holdings)
        if not df_consolidated_holdings.empty:
            df_consolidated_holdings.sort_values(by=['percentage_to_aum'], ascending=False, inplace=True)

            top5_perc = df_consolidated_holdings.head(5)['percentage_to_aum'].sum()
            top10_perc = df_consolidated_holdings.head(10)['percentage_to_aum'].sum()

        top_percent_list = list()
        mydict = dict()
        mydict["Concentration"] = 'Top 5'
        mydict["% holding"] = float(top5_perc) if top5_perc else 'NA'
        top_percent_list.append(mydict)

        mydict = dict()
        mydict["Concentration"] = 'Top 10'
        mydict["% holding"] = float(top10_perc) if top10_perc != top5_perc else 'NA' if top10_perc else 'NA'
        top_percent_list.append(mydict)

        df_topdata = pd.DataFrame(top_percent_list)
        df_topdata[' '] = ''

        if df_top.empty:
            df_top = df_topdata
        else:
            df_top = pd.concat([df_top, df_topdata], axis=1, join="outer")


        #get plan meta info
        plan_meta_details = get_plan_meta_info(db_session, [plan_id])

        if plan_meta_details:
            mydict = dict()
            mydict[""] = plan_meta_details[str(plan_id)]["plan_name"]
            fund_name_df = pd.DataFrame(mydict, index=[0])
            fund_name_df[' ']= ' '
            fund_name_df['  ']= ' '

            if df_fund_name.empty:
                df_fund_name = fund_name_df
            else:
                df_fund_name = pd.concat([df_fund_name, fund_name_df], axis=1, join="outer")


        # get portfolio characteristics
        valuation_data = list()
        portfolio_charact_data = list()

        
        mydict = dict()
        mydict['Valuations'] = 'PE'
        mydict['%'] = float(portfolio_characteristics["p_e"]) if portfolio_characteristics["p_e"] else 'NA' if portfolio_characteristics else 'NA'
        valuation_data.append(mydict)

        mydict = dict()
        mydict['Valuations'] = 'PB'
        mydict['%'] = float(portfolio_characteristics["p_b"])  if portfolio_characteristics["p_b"] else 'NA' if portfolio_characteristics else 'NA'
        valuation_data.append(mydict)

        df_valuat = pd.DataFrame(valuation_data)
        df_valuat[' ']= ' '
        

        if df_valuation.empty:
            df_valuation = df_valuat
        else:
            df_valuation = pd.concat([df_valuation, df_valuat], axis=1, join="outer")

        #Performance trend
        performancetrend_list = list()
        if performancetrend_data:
            mydict = dict()
            mydict['Returns'] = '1 MONTH'
            mydict['%'] = float(performancetrend_data["scheme_ret_1m"]) if performancetrend_data["scheme_ret_1m"] else 'NA' if performancetrend_data else 'NA'
            performancetrend_list.append(mydict)    

            mydict = dict()
            mydict['Returns'] = '3 MONTH'
            mydict['%'] = float(performancetrend_data["scheme_ret_3m"]) if performancetrend_data["scheme_ret_3m"] else 'NA' if performancetrend_data else 'NA'
            performancetrend_list.append(mydict) 

            mydict = dict()
            mydict['Returns'] = '6 MONTH'
            mydict['%'] = float(performancetrend_data["scheme_ret_6m"]) if performancetrend_data["scheme_ret_6m"] else 'NA' if performancetrend_data else 'NA'
            performancetrend_list.append(mydict)    

            mydict = dict()
            mydict['Returns'] = '1 YEAR'
            mydict['%'] = float(performancetrend_data["scheme_ret_1y"]) if performancetrend_data["scheme_ret_1y"] else 'NA' if performancetrend_data else 'NA'
            performancetrend_list.append(mydict)

            mydict = dict()
            mydict['Returns'] = '2 YEAR'
            mydict['%'] = float(performancetrend_data["scheme_ret_2y"]) if performancetrend_data["scheme_ret_2y"] else 'NA' if performancetrend_data else 'NA'
            performancetrend_list.append(mydict)

            mydict = dict()
            mydict['Returns'] = '3 YEAR'
            mydict['%'] = float(performancetrend_data["scheme_ret_3y"]) if performancetrend_data["scheme_ret_3y"] else 'NA' if performancetrend_data else 'NA'
            performancetrend_list.append(mydict)

            mydict = dict()
            mydict['Returns'] = '5 YEAR'
            mydict['%'] = float(performancetrend_data["scheme_ret_5y"]) if performancetrend_data["scheme_ret_5y"] else 'NA' if performancetrend_data else 'NA'
            performancetrend_list.append(mydict)

            mydict = dict()
            mydict['Returns'] = 'Since Inception'
            mydict['%'] = float(performancetrend_data["scheme_ret_ince"]) if performancetrend_data["scheme_ret_ince"] else 'NA' if performancetrend_data else 'NA'
            performancetrend_list.append(mydict) 

            performancetrend_df = pd.DataFrame(performancetrend_list)
            performancetrend_df[' '] = ''
            
            if df_performancetrend.empty:
                df_performancetrend = performancetrend_df
            else:
                df_performancetrend = pd.concat([df_performancetrend, performancetrend_df], axis=1, join="outer")


        if portfolio_characteristics:
            mydict = dict()
            mydict['Portfolio Characteristics'] = 'Total Stocks'
            mydict[''] = float(portfolio_characteristics["total_stocks"]) if portfolio_characteristics["total_stocks"] else 'NA' if portfolio_characteristics else 'NA'
            portfolio_charact_data.append(mydict)

            mydict = dict()
            mydict['Portfolio Characteristics'] = 'Avg Mkt Cap(Rs Cr)'
            mydict[''] = float(portfolio_characteristics["avg_mkt_cap"]) if portfolio_characteristics["avg_mkt_cap"] else 'NA' if portfolio_characteristics else 'NA'
            portfolio_charact_data.append(mydict)

            mydict = dict()
            mydict['Portfolio Characteristics'] = 'Portfolio P/E Ratio'
            mydict[''] = float(portfolio_characteristics["p_e"]) if portfolio_characteristics['p_e'] else 'NA'  if portfolio_characteristics else 'NA'
            portfolio_charact_data.append(mydict)
            
            mydict = dict()
            mydict['Portfolio Characteristics'] = 'Portfolio Dividend Yield'
            mydict[''] = float(portfolio_characteristics["dividend_yield"]) if portfolio_characteristics["dividend_yield"] else 'NA'  if portfolio_characteristics else 'NA'
            portfolio_charact_data.append(mydict)
            
            df_port_charct = pd.DataFrame(portfolio_charact_data)
            df_port_charct[' ']= ' '
            
            
            if df_portfolio_characteristics.empty:
                df_portfolio_characteristics = df_port_charct
            else:
                df_portfolio_characteristics = pd.concat([df_portfolio_characteristics, df_port_charct], axis=1, join="outer")


        #get Risk analysis
        riskanalysis_data = list()
        risk_analysis = get_fundriskratio_data(db_session, plan_id, transaction_date)
        if risk_analysis:
            mydict = dict()
            mydict['Risk Analysis'] = 'StandardDeviation 1Yr'
            mydict['%'] = float(risk_analysis["standard_deviation_1_y"]) if risk_analysis["standard_deviation_1_y"] else 'NA' if risk_analysis else 'NA'
            riskanalysis_data.append(mydict)

            mydict = dict()
            mydict['Risk Analysis'] = 'StandardDeviation 3Yr'
            mydict['%'] = float(risk_analysis["standard_deviation_3_y"]) if risk_analysis["standard_deviation_3_y"] else 'NA' if risk_analysis else 'NA'
            riskanalysis_data.append(mydict)

            mydict = dict()
            mydict['Risk Analysis'] = 'SharpeRatio 1Yr'
            mydict['%'] = float(risk_analysis["sharpe_ratio_1_y"]) if risk_analysis["sharpe_ratio_1_y"] else 'NA' if risk_analysis else 'NA'
            riskanalysis_data.append(mydict)

            mydict = dict()
            mydict['Risk Analysis'] = 'SharpeRatio 3Yr'
            mydict['%'] = float(risk_analysis["sharpe_ratio_3_y"]) if risk_analysis["sharpe_ratio_3_y"] else 'NA' if risk_analysis else 'NA'
            riskanalysis_data.append(mydict)

            mydict = dict()
            mydict['Risk Analysis'] = 'Beta 1Yr'
            mydict['%'] = float(risk_analysis["beta_1_y"]) if risk_analysis["beta_1_y"] else 'NA' if risk_analysis else 'NA'
            riskanalysis_data.append(mydict)

            mydict = dict()
            mydict['Risk Analysis'] = 'Beta 3Yr'
            mydict['%'] = float(risk_analysis["beta_3_y"]) if risk_analysis["beta_3_y"] else 'NA' if risk_analysis else 'NA'
            riskanalysis_data.append(mydict)

            mydict = dict()
            mydict['Risk Analysis'] = 'R_Squared 1Yr'
            mydict['%'] = float(risk_analysis["r_square_1_y"]) if risk_analysis["r_square_1_y"] else 'NA' if risk_analysis else 'NA'
            riskanalysis_data.append(mydict)

            mydict = dict()
            mydict['Risk Analysis'] = 'R_Squared 3Yr'
            mydict['%'] = float(risk_analysis["r_square_3_y"]) if risk_analysis["r_square_3_y"] else 'NA' if risk_analysis else 'NA'
            riskanalysis_data.append(mydict)

            mydict = dict()
            mydict['Risk Analysis'] = 'Modified Duration'
            mydict['%'] = 'NA'
            riskanalysis_data.append(mydict)

            df_risk_anal = pd.DataFrame(riskanalysis_data)
            df_risk_anal[' '] = ''
            
            if df_risk_analysis.empty:
                df_risk_analysis = df_risk_anal
            else:
                df_risk_analysis = pd.concat([df_risk_analysis, df_risk_anal], axis=1, join="outer")

        #get historic performance
        fund_historic_performance_list = list()
        transaction_date_python = transaction_date
        fund_historic_performance = get_fund_historic_performance(db_session,plan_id, None, None, 'CY', transaction_date_python)
        
        for perf in fund_historic_performance:
            mydict = dict()
            mydict["Calendar Year - Performance"] = perf['period']
            mydict["%"] = float(perf['performance'])
            fund_historic_performance_list.append(mydict)
        
        df_historic_perf = pd.DataFrame(fund_historic_performance_list)
        df_historic_perf[' '] = ''
        
        if df_historic_performance.empty:
            df_historic_performance = df_historic_perf
        else:
            df_historic_performance = pd.concat([df_historic_performance, df_historic_perf], axis=1, join="outer")
    
    df_fund_details = pd.DataFrame(fund_details_list)
       

    list_df = [df_fund_details, df_fund_name, df_sector, df_top, df_marketcap, df_valuation, df_performancetrend, df_risk_analysis, df_portfolio_characteristics, df_historic_performance]

    attachment = write_excel(list_df, report_name, transaction_date)

    return attachment
    


    

    

def write_excel(list_df=[], report_name='', report_date=None):
    export_dir_path = "E:\\Finalyca\\api\\Reports\\"
    attachements = list()
    output = F"{report_name}_{today.year}-{today.month}-{today.day}.xlsx"
    file_data_path = os.path.join(export_dir_path, output)
    attachements.append(file_data_path)
    sr = 1

    with pd.ExcelWriter(file_data_path) as writer:
        for df in list_df:

            df.to_excel(writer, startrow=sr, sheet_name=F"Data as on {report_date.strftime('%B %d, %Y')}", index=False, float_format="%.2f", header=False)
            workbook  = writer.book
            worksheet = writer.sheets[F"Data as on {report_date.strftime('%B %d, %Y')}"]

            header_format = workbook.add_format(
                {
                    "bold": True,
                    "text_wrap": True,
                    "valign": "top",
                    "fg_color": "#e3eaeb",
                    "border": 1,
                    'align': 'center',
                }
            )
            if not df.empty:
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(sr-1, col_num, value, header_format)

            format1 = workbook.add_format({'bg_color': '#FFC7CE',
                               'font_color': '#9C0006'})

            fmt_number = workbook.add_format({
            'num_format' : '0'
            })
            # Adding currency format
            fmt_currency = workbook.add_format({
            'num_format' : '$#,##0.00' ,'bold' :False
            })
            # Adding percentage format.
            fmt_rate = workbook.add_format({
            'num_format' : '%0.0' , 'bold' : False
            })
            
            # Adding formats for header row.
            fmt_header = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#215967',
            'font_color': '#FFFFFF',
            'border': 1})

            #Setting the zoom            
            worksheet.set_zoom(70)
            worksheet.set_column("A:ZZ", 12)
            
            
            # if sr==2:
              
                # title = F"Data as on {report_date.strftime('%B %d, %Y')}"
                # worksheet.write(0, 0, title, fmt_header)   #row,column
                # # worksheet.merge_range("A0:A1", "Merged Range", fmt_header)
                # worksheet.merge_range(0, 0, 0, 1, title , fmt_header)

            sr += (df.shape[0] + (2 if sr!=0 else 1))

    return attachements
            
def get_yesbank_report(db_session):
    plans = [9,6,47017,54,44571,1,45514,44289]
    attachment = generate_fund_wise_report(db_session, plans, True, current_month.date(), 'yesbank_report')
    return attachment

def get_motilaloswal_report(db_session):
    plans = [44931, 44932, 6, 7, 44296, 45002, 9, 10, 45558, 44287, 44293, 44518, 44608, 44927, 44928]
    attachment = generate_fund_wise_report(db_session, plans, False, current_month.date(), 'motilaloswal_report')
    return attachment


def axis_write_excel(report_df, pms_aum_df , report_name='', report_date=None, pms_aum=0, aif_aum=0, mf_aum=0, ulip_aum=0):
    export_dir_path = "E:\\Finalyca\\api\\Reports\\"
    attachements = list()
    output = F"{report_name}_{today.year}-{today.month}-{today.day}.xlsx"
    file_data_path = os.path.join(export_dir_path, output)
    attachements.append(file_data_path)
    sr = 4

    with pd.ExcelWriter(file_data_path) as writer:    
        report_df.to_excel(writer, startrow=sr, sheet_name='Report', index=False, float_format="%.2f", header=False)
        workbook  = writer.book
        worksheet = writer.sheets['Report']
        format1 = workbook.add_format({'bg_color': '#FFC7CE',
                                        'font_color': '#9C0006'})

        fmt_number = workbook.add_format({
        'num_format' : '#,##0.00'
        })
        # Adding currency format
        fmt_currency = workbook.add_format({
        'num_format' : '$#,##0.00' ,'bold' :False
        })
        # Adding percentage format.
        fmt_rate = workbook.add_format({
        'num_format' : '%0.0' , 'bold' : False
        })

        # Adding formats for header row.
        fmt_header = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top'})            

        #Setting the zoom            
        worksheet.set_zoom(70)
        worksheet.set_column("A:A", 12)
        worksheet.set_column("B:B", 40)

        worksheet.set_column("C:C", 12, fmt_number)
        worksheet.set_column("E:G", 12, fmt_number)
        worksheet.set_column("I:K", 12, fmt_number)
        worksheet.set_column("M:O", 12, fmt_number)
        worksheet.set_column("Q:S", 12, fmt_number)
        
        
        if sr==4: 
            title = F"Data as on {report_date.strftime('%B %d, %Y')}"
            worksheet.write(0, 0, title, fmt_header)   #row,column
            #row1
            worksheet.merge_range(0, 0, 0, 1, title , fmt_header) #start_row, start_column, end_row, end_column

            #row2
            worksheet.merge_range(1, 0, 1, 2, 'Data for Market Cap and AUM is in Rs crores', fmt_header)

            fmt_header = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'align': 'center',
            'border': 1})

            fmt_header1 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'align': 'center',
            'fg_color': '#f2f2f2',
            'border': 1})

            fmt_header_red = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'align': 'center',
            'font_color': 'red',
            'border': 1,
            'num_format' : '#,##0.00'
            })  

            worksheet.merge_range(1, 3, 1, 5, 'Total PMS AUM on Finalyca - ', fmt_header1)
            worksheet.write(1, 6, pms_aum, fmt_header_red)

            worksheet.merge_range(1, 7, 1, 9, 'Total MF Exposure to Equity on Finalyca - ', fmt_header1)
            worksheet.write(1, 10, mf_aum, fmt_header_red)

            worksheet.merge_range(1, 11, 1, 13, 'Total ULIP Exposure to Equity on Finalyca - ', fmt_header1)
            worksheet.write(1, 14, ulip_aum, fmt_header_red)

            worksheet.merge_range(1, 15, 1, 17, 'Total AIF  - Equity AUM on Finalyca - ', fmt_header1)
            worksheet.write(1, 18, aif_aum, fmt_header_red)
            
            #row3
            worksheet.write(2, 0, 'ISIN Code', fmt_header1)
            worksheet.write(2, 1, 'Equity Name', fmt_header1)
            worksheet.write(2, 2, 'Market Cap', fmt_header1)

            fmt_blue = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#9bc2e6',
            'font_color': '#0b0d0e',
            'align': 'center',
            'border': 1})
            worksheet.merge_range(2, 3, 2, 6, 'PMS' , fmt_blue)

            fmt_yellow = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#ffd966',
            'font_color': '#0b0d0e',
            'align': 'center',
            'border': 1})
            worksheet.merge_range(2, 7, 2, 10, 'MF' , fmt_yellow)

            fmt_green = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#a9d08e',
            'font_color': '#0b0d0e',
            'align': 'center',
            'border': 1})
            worksheet.merge_range(2, 11, 2, 14, 'ULIP' , fmt_green)

            fmt_orange = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#f4b084',
            'font_color': '#0b0d0e',
            'align': 'center',
            'border': 1})
            worksheet.merge_range(2, 15, 2, 18, 'AIF' , fmt_orange)

            #row 4
            worksheet.merge_range(3, 0, 3, 2, '')
            worksheet.write(3, 3, 'Funds Count', fmt_header1)
            worksheet.write(3, 4, 'AUM', fmt_header1)
            worksheet.write(3, 5, '% of total Product AUM', fmt_header1)
            worksheet.write(3, 6, '% of Equity M Cap', fmt_header1)
            worksheet.write(3, 7, 'Funds Count', fmt_header1)
            worksheet.write(3, 8, 'AUM', fmt_header1)
            worksheet.write(3, 9, '% of total Product AUM', fmt_header1)
            worksheet.write(3, 10, '% of Equity M Cap', fmt_header1)
            worksheet.write(3, 11, 'Funds Count', fmt_header1)
            worksheet.write(3, 12, 'AUM', fmt_header1)
            worksheet.write(3, 13, '% of total Product AUM', fmt_header1)
            worksheet.write(3, 14, '% of Equity M Cap', fmt_header1)
            worksheet.write(3, 15, 'Funds Count', fmt_header1)
            worksheet.write(3, 16, 'AUM', fmt_header1)
            worksheet.write(3, 17, '% of total Product AUM', fmt_header1)
            worksheet.write(3, 18, '% of Equity M Cap', fmt_header1)
            
        sr += (report_df.shape[0] + (2 if sr!=0 else 1))

        pms_aum_df.to_excel(writer, startrow=1, sheet_name='PMS AUM', index=False, float_format="%.2f", header=False )
        workbook1  = writer.book
        worksheet1 = writer.sheets['PMS AUM']

         #Setting the zoom            
        worksheet1.set_zoom(70)

        worksheet1.set_column("A:A", 30)
        worksheet1.set_column("B:B", 10)
        worksheet1.set_column("C:C", 12)

        worksheet1.write(0, 0, 'PMS Name', fmt_header1)
        worksheet1.write(0, 1, 'AUM', fmt_header1)
        worksheet1.write(0, 2, 'Inception Date', fmt_header1)
        worksheet1.write(0, 3, 'Date', fmt_header1)
        # worksheet1.set_column("C:S", 12, fmt_number)

    return attachements
    
    

def get_equity_analysis_overview(db_session, transaction_date, pms_aum, aif_aum, mf_aum, ulip_aum):    
    
    fundstock_subquery = db_session.query(
        HoldingSecurity.ISIN_Code.label('isin_code'), 
        FundStocks.HoldingSecurity_Name.label('holdingsecurity_name'), 
        
        (func.sum(case((FundStocks.Product_Id == 4, 1), else_=0)) - func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.ExitStockForFund == 1), 1), else_=0))).label('pms_count'), 
        (func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.ExitStockForFund != 1), FundStocks.Value_In_Inr), else_=0))/10000000).label('pms_aum'),

        (func.sum(case((FundStocks.Product_Id == 1, 1), else_=0)) - func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.ExitStockForFund == 1), 1), else_=0))).label('mf_count'),
        (func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.ExitStockForFund != 1), FundStocks.Value_In_Inr), else_=0))/10000000).label('mf_aum'),

        (func.sum(case((FundStocks.Product_Id == 2, 1), else_=0)) - func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.ExitStockForFund == 1), 1), else_=0))).label('ulip_count'),
        (func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.ExitStockForFund != 1), FundStocks.Value_In_Inr), else_=0))/10000000).label('ulip_aum'),

        (func.sum(case((FundStocks.Product_Id == 5, 1), else_=0)) - func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.ExitStockForFund == 1), 1), else_=0))).label('aif_count'), 
        (func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.ExitStockForFund != 1), FundStocks.Value_In_Inr), else_=0))/10000000).label('aif_aum'),


    ).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == FundStocks.HoldingSecurity_Id)\
    .group_by(
        HoldingSecurity.ISIN_Code, 
        FundStocks.HoldingSecurity_Name, 
        FundStocks.HoldingSecurity_Id,
        
    )\
    .filter(HoldingSecurity.ISIN_Code.like("INE%")).filter(FundStocks.InstrumentType == 'Equity').filter(HoldingSecurity.active==1).subquery()

    sql_fundamental_subquery = db_session.query(func.max(Fundamentals.PriceDate).label('max_pricedate'), Fundamentals.CO_CODE).filter(Fundamentals.Is_Deleted != 1, Fundamentals.PriceDate <= transaction_date).group_by(Fundamentals.CO_CODE).subquery()
    
    equities = db_session.query(fundstock_subquery.c.isin_code, 
                                fundstock_subquery.c.holdingsecurity_name, 
                                Fundamentals.mcap, 
                                fundstock_subquery.c.pms_count, 
                                fundstock_subquery.c.pms_aum, 
                                (fundstock_subquery.c.pms_aum * 100 / pms_aum).label('pms_total_product_aum'),
                                (fundstock_subquery.c.pms_aum * 100 / Fundamentals.mcap).label('pms_equity_to_mcap'),
                                fundstock_subquery.c.mf_count, 
                                fundstock_subquery.c.mf_aum, 
                                (fundstock_subquery.c.mf_aum * 100 / mf_aum).label('mf_total_product_aum'),
                                (fundstock_subquery.c.mf_aum * 100 / Fundamentals.mcap).label('mf_equity_to_mcap'),
                                fundstock_subquery.c.ulip_count, 
                                fundstock_subquery.c.ulip_aum, 
                                (fundstock_subquery.c.ulip_aum * 100 / ulip_aum).label('ulip_total_product_aum'),
                                (fundstock_subquery.c.ulip_aum * 100 / Fundamentals.mcap).label('ulip_equity_to_mcap'),
                                fundstock_subquery.c.aif_count, 
                                fundstock_subquery.c.aif_aum,
                                (fundstock_subquery.c.aif_aum * 100 / aif_aum).label('aif_total_product_aum'),
                                (fundstock_subquery.c.aif_aum * 100 / Fundamentals.mcap).label('aif_equity_to_mcap'))\
                .select_from(HoldingSecurity)\
                .join(fundstock_subquery, fundstock_subquery.c.isin_code == HoldingSecurity.ISIN_Code)\
                .join(sql_fundamental_subquery, sql_fundamental_subquery.c.CO_CODE == HoldingSecurity.Co_Code)\
                .join(Fundamentals, and_(Fundamentals.PriceDate == sql_fundamental_subquery.c.max_pricedate, Fundamentals.CO_CODE == sql_fundamental_subquery.c.CO_CODE)).filter(HoldingSecurity.active==1)\
            .order_by(desc(Fundamentals.mcap)).all()
    
        
    return equities


def get_axis_bank_report(db_session):
    # report_date = dt.strptime('2023-04-30', '%Y-%m-%d')
    report_date = current_month.date()
    
    pms_aum = get_pms_aif_aum_fundwise(db_session, 4, report_date, False)
    aif_aum = get_pms_aif_aum_fundwise(db_session, 5, report_date, False)
    mf_aum = get_mf_ulip_aum_fundwise(db_session, 1)
    ulip_aum = get_mf_ulip_aum_fundwise(db_session, 2)

    pms_fundwise_aum = get_pms_aif_aum_fundwise(db_session, 4, report_date, True)
    pms_fundwise_aum_df = pd.DataFrame(pms_fundwise_aum)
    pms_fundwise_aum_df = pms_fundwise_aum_df.drop('Plan_Id', axis=1)
    pms_fundwise_aum_df = pms_fundwise_aum_df.rename(columns={'Plan_Name': 'Fund Name', 'NetAssets_Rs_Cr': 'AUM', 'TransactionDate': 'Date'})
    pms_fundwise_aum_df['AUM'] = pd.to_numeric(pms_fundwise_aum_df['AUM'])
    pms_fundwise_aum_df['Inception Date'] = pd.to_datetime(pms_fundwise_aum_df['Inception Date'])
    pms_fundwise_aum_df['Date'] = pd.to_datetime(pms_fundwise_aum_df['Date'])
    pms_fundwise_aum_df['Date'] = pms_fundwise_aum_df['Date'].dt.date

    equities = get_equity_analysis_overview(db_session, report_date, pms_aum, aif_aum, mf_aum, ulip_aum)

    resp = list()
    for obj in equities:
        resp.append(obj._asdict())

    df_fundstock = pd.DataFrame(resp)
    df_fundstock.fillna(0)

    df_fundstock['mcap'] = pd.to_numeric(df_fundstock['mcap'])
    df_fundstock['pms_aum'] = pd.to_numeric(df_fundstock['pms_aum'])
    df_fundstock['pms_total_product_aum'] = pd.to_numeric(df_fundstock['pms_total_product_aum'])
    df_fundstock['pms_equity_to_mcap'] = pd.to_numeric(df_fundstock['pms_equity_to_mcap'])
    df_fundstock['mf_aum'] = pd.to_numeric(df_fundstock['mf_aum'])
    df_fundstock['mf_total_product_aum'] = pd.to_numeric(df_fundstock['mf_total_product_aum'])
    df_fundstock['mf_equity_to_mcap'] = pd.to_numeric(df_fundstock['mf_equity_to_mcap'])
    df_fundstock['ulip_aum'] = pd.to_numeric(df_fundstock['ulip_aum'])
    df_fundstock['ulip_total_product_aum'] = pd.to_numeric(df_fundstock['ulip_total_product_aum'])
    df_fundstock['ulip_equity_to_mcap'] = pd.to_numeric(df_fundstock['ulip_equity_to_mcap'])
    df_fundstock['aif_aum'] = pd.to_numeric(df_fundstock['aif_aum'])
    df_fundstock['aif_total_product_aum'] = pd.to_numeric(df_fundstock['aif_total_product_aum'])
    df_fundstock['aif_equity_to_mcap'] = pd.to_numeric(df_fundstock['aif_equity_to_mcap'])
    

    attachement = axis_write_excel(df_fundstock, pms_fundwise_aum_df, 'axis bank', report_date, pms_aum, aif_aum, mf_aum, ulip_aum)
    return attachement


def get_detailed_nav_aum_report(db_session):
    report_date = current_month.date()
    
    

    #get list of plans newly added and add them to list
    plans_newly_added_list = get_newly_onboarded_plans(db_session, 4)
    for plans in plans_newly_added_list:
        #check if it is having data for current month
        having_data = check_if_fund_contains_nav_and_factsheet_by_date(db_session, plans, report_date)
        if having_data:
            report_plans_status = Report_Plans_status()
            report_plans_status.Plan_Id = plans
            report_plans_status.Shared_In_Last_Month_Report = 1
            report_plans_status.Shared_In_Current_Month = 1
            report_plans_status.Last_Month_Date = report_date.strftime('%Y-%m-%d')
            report_plans_status.Current_Month_Date = report_date.strftime('%Y-%m-%d')
            report_plans_status.added_on_portal = report_date.strftime('%Y-%m-%d')
            db_session.add(report_plans_status)
            db_session.commit()
    
    plans_newly_added = get_plans_newly_added_in_report(db_session, report_date)

    #get list of plans - data issue
    data_issue_list = get_plans_having_data_issue(db_session, report_date)
    for dat in data_issue_list:
        status_query = db_session.query(Report_Plans_status).filter(Report_Plans_status.Plan_Id == dat).one_or_none()
        status_query.Shared_In_Current_Month = 0
        status_query.Current_Month_Date = None

        status_query.Shared_In_Last_Month_Report = 0
        status_query.Last_Month_Date = None

        db_session.commit()

    #get list of plans where issue fixed
    fixed_data_issue_list = get_fixed_data_issues(db_session, report_date, data_issue_list)
    #sachin hefre add list of plans for which we recvd data after a gap
    for dat in fixed_data_issue_list:
        status_query = db_session.query(Report_Plans_status).filter(Report_Plans_status.Plan_Id == dat).one_or_none()
        if status_query:            
            status_query.Shared_In_Current_Month = 1
            status_query.Current_Month_Date = report_date.strftime('%Y-%m-%d')

            status_query.Shared_In_Last_Month_Report = 1
            status_query.Last_Month_Date = report_date.strftime('%Y-%m-%d')

            db_session.commit()

    

    #get funds for which data not recvd
    plans_data_not_recvd = get_plans_fo_which_data_not_received(db_session, 4, report_date)
    for dat in plans_data_not_recvd:
        status_query = db_session.query(Report_Plans_status).filter(Report_Plans_status.Plan_Id == dat).one_or_none()
        if status_query:            
            status_query.Shared_In_Current_Month = 0
            status_query.Current_Month_Date = None

            status_query.Shared_In_Last_Month_Report = 0
            status_query.Last_Month_Date = None

            db_session.commit()


    excluded_list = (data_issue_list if data_issue_list else []) + (plans_newly_added if plans_newly_added else []) + (plans_data_not_recvd if plans_data_not_recvd else []) + (fixed_data_issue_list if fixed_data_issue_list else []) #exclude from incremental data


    respons = get_detailed_incremental_nav_aum_report(db_session, report_date, excluded_list, fixed_data_issue_list)
    file_data_path = respons['file_path']
    fixed_data_issue_list = respons['fixed_data_issue_list']

    file_data_path1 = get_detailed_changes_nav_aum_report(db_session, report_date, data_issue_list, fixed_data_issue_list, plans_newly_added, plans_data_not_recvd)

    attachements = list()
    attachements.append(file_data_path)
    attachements.append(file_data_path1)

    return attachements

def get_detailed_changes_nav_aum_report(db_session, report_date, data_issue_list, fixed_data_issue_list, plans_newly_added, plans_data_not_recvd):
    not_recvd_plan_meta_info = get_plan_meta_info(db_session, plans_data_not_recvd)
    not_recvd_plan_meta_data_list = list()
    for data in not_recvd_plan_meta_info:
        plan_meta_inf1 = dict()
        plan_meta_inf1['Fund Name'] = not_recvd_plan_meta_info[str(data)]['plan_name']
        plan_meta_inf1['Remarks'] = 'Data not Received'
        
        not_recvd_plan_meta_data_list.append(plan_meta_inf1)
    
    not_recvd_plan_meta_info_df = pd.DataFrame(not_recvd_plan_meta_data_list)
    not_recvd_plan_meta_info_df = not_recvd_plan_meta_info_df.sort_values(by=['Fund Name'])

    #New added
    #AUM
    new_fundwise_aum = get_pms_aif_aum_fundwise(db_session, 4, report_date, True, plans_newly_added, False)
    new_fundwise_aum_df = pd.DataFrame(new_fundwise_aum)
    new_fundwise_aum_df = new_fundwise_aum_df.drop('Plan_Id', axis=1)
    new_fundwise_aum_df = new_fundwise_aum_df.rename(columns={'Plan_Name': 'Fund Name', 'NetAssets_Rs_Cr': 'AUM', 'TransactionDate': 'Date'})
    new_fundwise_aum_df['AUM'] = pd.to_numeric(new_fundwise_aum_df['AUM'])
    new_fundwise_aum_df['Inception Date'] = pd.to_datetime(new_fundwise_aum_df['Inception Date'])
    new_fundwise_aum_df['Inception Date'] = new_fundwise_aum_df['Inception Date'].dt.date
    new_fundwise_aum_df['Date'] = pd.to_datetime(new_fundwise_aum_df['Date'])
    new_fundwise_aum_df['Date'] = new_fundwise_aum_df['Date'].dt.date

    #NAV
    new_fundwise_nav = get_pms_aif_nav_fundwise(db_session, 4, None, True, plans_newly_added, False)
    new_fundwise_nav_df = pd.DataFrame(new_fundwise_nav)
    new_fundwise_nav_df['NAV'] = pd.to_numeric(new_fundwise_nav_df['NAV'])
    new_fundwise_nav_df['Date'] = pd.to_datetime(new_fundwise_nav_df['Date'])
    new_fundwise_nav_df['Date'] = new_fundwise_nav_df['Date'].dt.date


    plan_meta_info = get_plan_meta_info(db_session, plans_newly_added)
    plan_meta_data_list = list()
    for data in plan_meta_info:
        plan_meta_inf = dict()
        plan_meta_inf['Fund Name'] = plan_meta_info[str(data)]['plan_name']
        plan_meta_inf['AMC Name'] = plan_meta_info[str(data)]['amc_name']
        plan_meta_inf['Classification'] = plan_meta_info[str(data)]['classification']
        plan_meta_inf['Benchmark'] = plan_meta_info[str(data)]['benchmark_name']
        
        plan_meta_data_list.append(plan_meta_inf)
    
    plan_meta_info_df = pd.DataFrame(plan_meta_data_list)


    #having data issue
    #AUM
    issue_fundwise_aum = get_pms_aif_aum_fundwise(db_session, 4, report_date, True, data_issue_list, False)
    issue_fundwise_aum_df = pd.DataFrame(issue_fundwise_aum)
    if not issue_fundwise_aum_df.empty:
        issue_fundwise_aum_df = issue_fundwise_aum_df.drop('Plan_Id', axis=1)
        issue_fundwise_aum_df = issue_fundwise_aum_df.rename(columns={'Plan_Name': 'Fund Name', 'NetAssets_Rs_Cr': 'AUM', 'TransactionDate': 'Date'})
        issue_fundwise_aum_df['AUM'] = pd.to_numeric(issue_fundwise_aum_df['AUM'])
        issue_fundwise_aum_df['Inception Date'] = pd.to_datetime(issue_fundwise_aum_df['Inception Date'])
        issue_fundwise_aum_df['Inception Date'] = issue_fundwise_aum_df['Inception Date'].dt.date
        issue_fundwise_aum_df['Date'] = pd.to_datetime(issue_fundwise_aum_df['Date'])
        issue_fundwise_aum_df['Date'] = issue_fundwise_aum_df['Date'].dt.date

    #NAV
    issue_fundwise_nav = get_pms_aif_nav_fundwise(db_session, 4, report_date, True, data_issue_list, False)
    issue_fundwise_nav_df = pd.DataFrame(issue_fundwise_nav)
    if not  issue_fundwise_nav_df.empty:
        issue_fundwise_nav_df['NAV'] = pd.to_numeric(issue_fundwise_nav_df['NAV'])
        issue_fundwise_nav_df['Date'] = pd.to_datetime(issue_fundwise_nav_df['Date'])
        issue_fundwise_nav_df['Date'] = issue_fundwise_nav_df['Date'].dt.date


    #fixed data issue
    #AUM
    fixed_fundwise_aum = get_pms_aif_aum_fundwise(db_session, 4, report_date, True, fixed_data_issue_list, False)
    fixed_fundwise_aum_df = pd.DataFrame(fixed_fundwise_aum)
    if not  fixed_fundwise_aum_df.empty:
        fixed_fundwise_aum_df = fixed_fundwise_aum_df.drop('Plan_Id', axis=1)
        fixed_fundwise_aum_df = fixed_fundwise_aum_df.rename(columns={'Plan_Name': 'Fund Name', 'NetAssets_Rs_Cr': 'AUM', 'TransactionDate': 'Date'})
        fixed_fundwise_aum_df['AUM'] = pd.to_numeric(fixed_fundwise_aum_df['AUM'])
        fixed_fundwise_aum_df['Inception Date'] = pd.to_datetime(fixed_fundwise_aum_df['Inception Date'])
        fixed_fundwise_aum_df['Inception Date'] = fixed_fundwise_aum_df['Inception Date'].dt.date
        fixed_fundwise_aum_df['Date'] = pd.to_datetime(fixed_fundwise_aum_df['Date'])
        fixed_fundwise_aum_df['Date'] = fixed_fundwise_aum_df['Date'].dt.date

    #NAV
    fixed_fundwise_nav = get_pms_aif_nav_fundwise(db_session, 4, None, True, fixed_data_issue_list, False)
    fixed_fundwise_nav_df = pd.DataFrame(fixed_fundwise_nav)
    if not  fixed_fundwise_nav_df.empty:
        fixed_fundwise_nav_df['NAV'] = pd.to_numeric(fixed_fundwise_nav_df['NAV'])
        fixed_fundwise_nav_df['Date'] = pd.to_datetime(fixed_fundwise_nav_df['Date'])
        fixed_fundwise_nav_df['Date'] = fixed_fundwise_nav_df['Date'].dt.date



    # TODO: make a standard directory
    export_dir_path = "E:\\Finalyca\\api\\Reports\\"

    output = F"Changes_report_{report_date.strftime('%B %d, %Y')}.xlsx"
    file_data_path = os.path.join(export_dir_path, output)
    

    with pd.ExcelWriter(file_data_path) as writer:
        format_worksheet(new_fundwise_aum_df, 'New Additions with AUM', writer)
        format_worksheet(new_fundwise_nav_df, 'NAVs of Additions', writer)
        format_worksheet(plan_meta_info_df, 'Masters of New Additions', writer)
        format_worksheet(issue_fundwise_aum_df, 'AUM-PMSes - having some issues', writer)
        format_worksheet(issue_fundwise_nav_df, 'NAV-PMSes - having some issues', writer)
        format_worksheet(fixed_fundwise_aum_df, 'Corrected PMSes Hist - AUM', writer)
        format_worksheet(fixed_fundwise_nav_df, 'Corrected PMSes Hist - NAV', writer)
        format_worksheet(not_recvd_plan_meta_info_df, 'Other Updates', writer)
        

    return file_data_path
    
   
    


def get_plans_newly_added_in_report(db_session, report_date):
    newlyadded_list = db_session.query(Report_Plans_status.Plan_Id).filter(Report_Plans_status.added_on_portal == report_date, Report_Plans_status.Plan_Id.not_in([46163,44193]))
    resp = [u._asdict() for u in newlyadded_list.all()]

    res = list()
    for sub in resp:        
        res.append(sub['Plan_Id'])
    return res    

def get_plans_having_data_issue(db_session, report_date):
    data_issue_list = db_session.query(Report_Data_Issues.Plan_Id).filter(Report_Data_Issues.Is_Fixed != 1, Report_Data_Issues.Plan_Id.not_in([46163,44193]))
    resp = [u._asdict() for u in data_issue_list.all()]

    res = list()
    for sub in resp:        
        res.append(sub['Plan_Id'])
    return res

def get_fixed_data_issues(db_session, report_date, data_issue_list=[]):
    fixed_data_issue_list = db_session.query(Report_Data_Issues.Plan_Id).filter(Report_Data_Issues.Is_Fixed == 1, Report_Data_Issues.Plan_Id.not_in(data_issue_list), Report_Data_Issues.Plan_Id.not_in([46163,44193]))
    resp = [u._asdict() for u in fixed_data_issue_list.all()]

    res = list()
    for sub in resp:
        res.append(sub['Plan_Id'])

    return res
# sachin
# def get_plan_list_missing_last_month(db_session, report_date, data_issue_list=[]):
#     missing_list = db_session.query(Report_Plans_status.Plan_Id).filter(Report_Data_Issues.Is_Fixed == 1, Report_Data_Issues.Plan_Id.not_in(data_issue_list), Report_Data_Issues.Plan_Id.not_in([46163,44193]))
#     resp = [u._asdict() for u in fixed_data_issue_list.all()]

#     res = list()
#     for sub in resp:
#         res.append(sub['Plan_Id'])

#     return res


def get_detailed_incremental_nav_aum_report(db_session, report_date, excluded_list, fixed_data_issue_list):
    
    resp = dict()
    #AUM
    pms_fundwise_aum = get_pms_aif_aum_fundwise(db_session, 4, report_date, True, excluded_list, True)
    #update Masters.Report_Plans_status table as recvd data 
    for dat in pms_fundwise_aum:
        status_query = db_session.query(Report_Plans_status).filter(Report_Plans_status.Plan_Id == dat['Plan_Id']).one_or_none()
        if status_query: 
            if status_query.Current_Month_Date:                
                if status_query.Current_Month_Date == previous_month.date():                    
                    status_query.Last_Month_Date = previous_month.strftime('%Y-%m-%d')
                    
                elif status_query.Current_Month_Date.date() < previous_month.date():
                    status_query.Last_Month_Date = status_query.Current_Month_Date                

                    fixed_data_issue_list.append(dat['Plan_Id'])#add fund as we have to share nav of missing last month
            else:
                fixed_data_issue_list.append(dat['Plan_Id'])
                
                status_query.Last_Month_Date = status_query.Current_Month_Date
                # raise BadRequest(F'incremental report having item(plan_id - {dat["Plan_Id"]}) which was not shared earlier.')
                # sachin add in the list saying recvd data after a gap return list to fixed

            status_query.Shared_In_Current_Month = 1            
            status_query.Shared_In_Last_Month_Report = 1
            status_query.Current_Month_Date = report_date.strftime('%Y-%m-%d')
            db_session.commit()

        else:
            raise BadRequest(F'incremental report having item(plan_id - {dat["Plan_Id"]}) which does not exists in the Masters.Report_Plans_status table.')
    excluded_list = excluded_list + fixed_data_issue_list
    pms_fundwise_aum = get_pms_aif_aum_fundwise(db_session, 4, report_date, True, excluded_list, True)
    pms_fundwise_aum_df = pd.DataFrame(pms_fundwise_aum)
    pms_fundwise_aum_df = pms_fundwise_aum_df.drop('Plan_Id', axis=1)
    pms_fundwise_aum_df = pms_fundwise_aum_df.rename(columns={'Plan_Name': 'PMS Name', 'NetAssets_Rs_Cr': 'AUM', 'TransactionDate': 'Date'})
    pms_fundwise_aum_df['AUM'] = pd.to_numeric(pms_fundwise_aum_df['AUM'])
    pms_fundwise_aum_df['Inception Date'] = pd.to_datetime(pms_fundwise_aum_df['Inception Date'])
    pms_fundwise_aum_df['Inception Date'] = pms_fundwise_aum_df['Inception Date'].dt.date
    pms_fundwise_aum_df['Date'] = pd.to_datetime(pms_fundwise_aum_df['Date'])
    pms_fundwise_aum_df['Date'] = pms_fundwise_aum_df['Date'].dt.date

    #NAV
    pms_fundwise_nav = get_pms_aif_nav_fundwise(db_session, 4, report_date, True, excluded_list, True)
    pms_fundwise_nav_df = pd.DataFrame(pms_fundwise_nav)
    pms_fundwise_nav_df['NAV'] = pd.to_numeric(pms_fundwise_nav_df['NAV'])
    pms_fundwise_nav_df['Date'] = pd.to_datetime(pms_fundwise_nav_df['Date'])
    pms_fundwise_nav_df['Date'] = pms_fundwise_nav_df['Date'].dt.date

    # TODO: make a standard directory
    export_dir_path = "E:\\Finalyca\\api\\Reports\\"

    
    output = F"Incremental_report_{report_date.strftime('%B %d, %Y')}.xlsx"
    file_data_path = os.path.join(export_dir_path, output)
    resp['file_path'] = file_data_path
    resp['fixed_data_issue_list'] = fixed_data_issue_list

    with pd.ExcelWriter(file_data_path) as writer:
        format_worksheet(pms_fundwise_nav_df, 'NAV', writer)
        format_worksheet(pms_fundwise_aum_df, 'AUM', writer) 

    return resp


def format_worksheet(df, sheetname, writer):
    df.to_excel(writer, sheet_name=sheetname, float_format="%.2f", index=False)
    workbook  = writer.book
    worksheet = writer.sheets[sheetname]
    worksheet.set_zoom(80)
    if not df.empty:
        worksheet.autofit()

    header_format = workbook.add_format({
                                        'bold': True,
                                        'fg_color': '#AED6F1',
                                        'border': 1
                                        })

    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)

        column_len = df[value].astype(str).str.len().max()
        column_len = max(column_len, len(value)) + 2
        worksheet.set_column(col_num, col_num, column_len)

    return worksheet

def generate_jio_financial_report(db_session):
    today = dt.today()
    
    df_fund_details = pd.DataFrame()
    fund_details_list = list()
    
    mf_plans_data = get_plans_list_product_wise(db_session, 1, current_month, previous_month)
    # count = 0
    for scheme_details in mf_plans_data:
        # count = count + 1
        age_of_fund = calculate_age(scheme_details["MF_Security_OpenDate"], current_month, True)
        # print(count)

        # if count> 500:
        #     break
        mydict = dict()
        mydict["AMC Name"] = scheme_details["AMC_Name"]
        mydict["Scheme Name"] = scheme_details["Plan_Name"]
        mydict["ISIN"] = scheme_details["ISIN"]
        mydict["ISIN 2"] = scheme_details["ISIN2"]
        mydict["Benchmark Name"] = scheme_details["BenchmarkIndices_Name"]
        mydict["Factsheet Date"] = scheme_details["TransactionDate"].strftime('%d-%b-%Y')
        mydict["Asset Class"] = scheme_details["AssetClass_Name"]
        mydict["Fund Type"] = scheme_details["FundType_Name"]
        mydict["Classification"] = scheme_details["Classification_Name"]
        mydict["Expense Ratio"] = float(round(scheme_details["ExpenseRatio"],4)) if scheme_details["ExpenseRatio"] else ''
        mydict["AUM (crs)"] = float(round(scheme_details["NetAssets_Rs_Cr"],4)) if scheme_details["NetAssets_Rs_Cr"] else ''
        
        mydict["1 Yr Volatility(St Dev)"] = float(round(scheme_details["StandardDeviation_1Yr"],2)) if scheme_details["StandardDeviation_1Yr"] and age_of_fund>=12 else None
        mydict["1 Yr Volatility(Beta)"] = float(round(scheme_details["Beta_1Yr"],2)) if scheme_details["Beta_1Yr"] and age_of_fund>=12 else None
        mydict["3 Yr Volatility(St Dev)"] = float(round(scheme_details["StandardDeviation"],2)) if scheme_details["StandardDeviation"] and age_of_fund>=36 else None
        mydict["3 Yr Volatility(Beta)"] = float(round(scheme_details["Beta"],2)) if scheme_details["Beta"] and age_of_fund>=36 else None
        mydict["Nav"] = float(round(scheme_details["NAV"],2)) if scheme_details["NAV"] else ''
        mydict["Inception Date"] = scheme_details["MF_Security_OpenDate"].strftime('%d-%b-%Y') if scheme_details["MF_Security_OpenDate"] else None
        
        mydict["Scheme Age in Months"] = age_of_fund

        scheme_return_1year = scheme_details['SCHEME_RETURNS_1YEAR'] if scheme_details['SCHEME_RETURNS_1YEAR'] else 0
        scheme_return_3year = scheme_details['SCHEME_RETURNS_3YEAR'] if scheme_details['SCHEME_RETURNS_3YEAR'] else 0
        scheme_return_5year = scheme_details['SCHEME_RETURNS_5YEAR'] if scheme_details['SCHEME_RETURNS_5YEAR'] else 0

        benchmark_return_1year = scheme_details['SCHEME_BENCHMARK_RETURNS_1YEAR'] if scheme_details['SCHEME_BENCHMARK_RETURNS_1YEAR'] else 0
        benchmark_return_3year = scheme_details['SCHEME_BENCHMARK_RETURNS_3YEAR'] if scheme_details['SCHEME_BENCHMARK_RETURNS_3YEAR'] else 0
        benchmark_return_5year = scheme_details['SCHEME_BENCHMARK_RETURNS_5YEAR'] if scheme_details['SCHEME_BENCHMARK_RETURNS_5YEAR'] else 0
        
        mydict["1 Yr Alpha over benchmark"] = round(scheme_return_1year - benchmark_return_1year,2) if scheme_return_1year and benchmark_return_1year else ''
        mydict["3 Yr Alpha over benchmark"] = round(scheme_return_3year - benchmark_return_3year,2) if scheme_return_3year and benchmark_return_3year else ''
        mydict["5 Yr Alpha over benchmark"] = round(scheme_return_5year - benchmark_return_5year,2) if scheme_return_5year and benchmark_return_5year else ''

        fundmanager_data = getfundmanager(db_session, scheme_details['Plan_Id'])

        for index, fm in enumerate(fundmanager_data):
            mydict[F'Fund Manager {index+1}'] = fm['fund_manager_name']
            mydict[F'AUM in cr under Fund Manager {index+1}'] = fm['fund_manager_aum_managed']

        fund_details_list.append(mydict)

    df_fund_details = pd.DataFrame(fund_details_list)

    report_date = current_month.date()
    attachment = write_excel([df_fund_details], 'jio', report_date)
    return attachment

# def get_test():
#     db_session = get_finalyca_scoped_session(True)  
#     date = dt.strptime('2023-10-31', '%Y-%m-%d')
    
#     sql_nav = db_session.query(NAV)\
#     .filter(NAV.Is_Deleted != 1).filter(NAV.Plan_Id == 31603).filter(NAV.NAV_Type=='P').first()

#     sql_bench_nav = db_session.query(NAV)\
#     .filter(NAV.Is_Deleted != 1).filter(NAV.Plan_Id == 139).filter(NAV.NAV_Type=='I').first()

#     if sql_nav: #and sql_bench_nav:
#         #calculate risk analysis                    
#         sql_rate_max_date = db_session.query(func.max(RiskFreeIndexRate.Date)).filter(RiskFreeIndexRate.Date <= date).scalar()

#     sql_rate = db_session.query(RiskFreeIndexRate).filter(RiskFreeIndexRate.Date == sql_rate_max_date).one_or_none()

#     risk_free_index_date = sql_rate.Date
#     risk_free_index_rate = float(sql_rate.Rate)

    
#     risk_ratio_12 = get_risk_ratios(db_session, 33448, 635, date, 12, risk_free_index_rate)
#     risk_ratio_36 = get_risk_ratios(db_session, 33448, 635, date, 36, risk_free_index_rate)
#     a= 1

# get_test()

if __name__ == '__main__':
    db_session = get_finalyca_scoped_session(True)  
    generate_jio_financial_report(db_session)
    # get_axis_bank_report(db_session)
    # get_yesbank_report(db_session)
    # get_motilaloswal_report(db_session)
    # get_detailed_nav_aum_report(db_session)