from operator import itemgetter
import json
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from dateutil import parser
from bizlogic.importer_helper import get_scheme_details, getfundmanager, get_performancetrend_data, get_sectorweightsdata,\
                                    get_compositiondata, get_portfolio_characteristics, get_fundriskratio_data, get_fund_holdings,\
                                    get_marketcap_composition, get_riskrating, get_instrumenttype, get_investmentstyle, \
                                    get_fund_portfolio_change, get_fund_nav, get_rollingreturn,\
                                    get_detailed_fund_risk_ratios, get_attributions, get_portfolio_date,\
                                    get_organization_whitelabel, get_fundcomparedata_planwise, get_plan_overlap,\
                                    get_trailing_return_and_riskanalysis, get_holdings_sector_data, get_securedunsecured_data,\
                                    get_portfolio_instrumentrating_data, get_portfolio_instrument_data, get_aum_monthwise,\
                                    investmentstyle_month_wise, marketcapcomposition_month_wise, calculate_portfolio_level_analysis,\
                                    generate_active_rolling_returns
from bizlogic.common_helper import get_benchmarkdetails, get_fund_historic_performance, get_plan_meta_info, get_detailed_fund_holdings
from reports.visuals import get_geo_location_chart, get_trand_analysis_stacked_chart, get_pie_chart, get_barchart, get_linechart, get_line_chart
from reports.utils import get_table_from_html_template, prepare_pdf_from_html
from utils.utils import calculate_age


def get_factsheetpdf(db_session, plan_id, transactiondate, portfolio_date, attribution_from_date, attribution_to_date, organization_id, image_path, whitelabel_dir, report_generation_path, selection_dict, gsquare_url="", attribution_benchmark_id="", is_annualized_return = False):
    template_vars = dict()
    is_debt = False

    #prepare report data points to be shown
    template_vars["page_break_class"] = "breakpage" if selection_dict["page_break"] == True else "donotbreak"
    template_vars["hide_section_1"] = True if not selection_dict["section_1"] else False
    template_vars["hide_section_2"] = True if not selection_dict["section_2"] else False
    template_vars["hide_portfolio_holdings"] = True if not selection_dict["portfolio_holdings"] else False
    template_vars["hide_performance_trend"] = True if not selection_dict["performance_trend"] else False
    template_vars["hide_nav_movement"] = True if not selection_dict["nav_movement"] else False
    template_vars["hide_rolling_return_1"] = True if not selection_dict["rolling_return_1"] else False
    template_vars["hide_rolling_return_3"] = True if not selection_dict["rolling_return_3"] else False
    template_vars["hide_rolling_return_5"] = True if not selection_dict["rolling_return_5"] else False
    template_vars["hide_performance_graph"] = True if not selection_dict["performance_graph"] else False
    template_vars["hide_risk_volatility_measures"] = True if not selection_dict["risk_volatility_measures"] else False
    template_vars["hide_detailed_portfolio"] = True if not selection_dict["detailed_portfolio"] else False
    template_vars["hide_attribution"] = True if not selection_dict["attribution"] else False
    template_vars["hide_trend_analysis"] = False#True if not selection_dict["trend_analysis"] else False
    template_vars["hide_active_rolling_return_1"] = True if not selection_dict["active_rolling_return_1"] else False
    template_vars["hide_active_rolling_return_3"] = True if not selection_dict["active_rolling_return_3"] else False

    #get data for factsheet
    data = get_factsheet_data(db_session, plan_id, transactiondate, portfolio_date, attribution_from_date, attribution_to_date, organization_id, template_vars, gsquare_url, attribution_benchmark_id, is_annualized_return=is_annualized_return)

    #prepare scheme details
    product_id = None
    cal_age = None
    
    organization_whitelabel_data = data["organization_whitelabel_data"]
    if organization_whitelabel_data:
        template_vars["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{organization_whitelabel_data['logo']}"
        template_vars["disclaimer_text"] = organization_whitelabel_data['disclaimer']

    scheme_details = data["scheme_details"]
    if scheme_details:          
        # template_vars["organization_logo_path"] = organization_logo_path
        template_vars["amc_imagepath"] = F"{image_path}/{scheme_details['amc_logo']}"
        template_vars["strategy_name"] = scheme_details['plan_name']
        template_vars["strategy_category"] = scheme_details["product_name"] + " - " + scheme_details["classification_name"] 
        product_id = scheme_details['product_id']
        template_vars["inception_date"] = datetime.strftime(scheme_details["inception_date"], '%d %b %Y') 
        template_vars["benchmark_name"] = scheme_details["benchmark_indices_name"] if scheme_details["benchmark_indices_name"] else "Not Available"
        template_vars["min_investment"] = scheme_details["min_purchase_amount_inwords"] if scheme_details["min_purchase_amount_inwords"] else "Not Available"
        template_vars["exp_ratio"] = scheme_details["expense_ratio"] if scheme_details["expense_ratio"] else "Not Available"
        template_vars["exit_load"] = scheme_details["exit_load"] if scheme_details["exit_load"] else "Not Available"
        template_vars["fees_structure"] = scheme_details["fees_structure"] if scheme_details["fees_structure"] else "Not Available"
        template_vars["investment_strategy"] = scheme_details["investment_strategy"] if scheme_details["investment_strategy"] else "NA"
        # age
        template_vars["transaction_date"] = datetime.strftime(scheme_details["transaction_date"], '%d %b %Y')
        template_vars["aum"] = round(scheme_details["aum"],2) if scheme_details["aum"] else "NA"

        if scheme_details['aif_category'] == 2 and scheme_details['aif_sub_category'] == 'Debt':
            template_vars['is_debt'] = True
            is_debt = True
            template_vars['aif_allotment_date'] = datetime.strftime(scheme_details["aif_allotment_date"], '%d %b %Y') if scheme_details["aif_allotment_date"] else "NA"
            template_vars["aif_capitalreturned_in_cr"] = round(scheme_details["aif_capitalreturned_in_cr"],2) if scheme_details["aif_capitalreturned_in_cr"] else "NA"
            template_vars['aif_category'] = scheme_details["aif_category"] 
            template_vars['aif_sub_category'] = scheme_details["aif_sub_category"] 
            template_vars["aif_commitedcapital_in_cr"] = round(scheme_details["aif_commitedcapital_in_cr"],2) if scheme_details["aif_commitedcapital_in_cr"] else "NA"
            template_vars['aif_class_of_units'] = scheme_details["aif_class_of_units"] if scheme_details["aif_class_of_units"] else "NA"
            template_vars['aif_currency'] = scheme_details["aif_currency"] if scheme_details["aif_currency"] else "NA"
            template_vars['aif_dollar_nav'] = scheme_details["aif_dollar_nav"] if scheme_details["aif_dollar_nav"] else "NA"
            template_vars["aif_drawdowncapital_in_cr"] = round(scheme_details["aif_drawdowncapital_in_cr"],2) if scheme_details["aif_drawdowncapital_in_cr"] else "NA"
            template_vars['aif_fund_rating'] = scheme_details["aif_fund_rating"] if scheme_details["aif_fund_rating"] else "NA"
            template_vars['aif_hurdle_rate'] = scheme_details["aif_hurdle_rate"] if scheme_details["aif_hurdle_rate"] else "NA"
            template_vars["aif_initial_drawdown"] = round(scheme_details["aif_initial_drawdown"],2) if scheme_details["aif_initial_drawdown"] else "NA"
            template_vars['aif_fundclosure_date'] = datetime.strftime(scheme_details["aif_fundclosure_date"], '%d %b %Y') if scheme_details["aif_fundclosure_date"] else "NA"
            template_vars['aif_initialclosure_date'] = datetime.strftime(scheme_details["aif_initialclosure_date"], '%d %b %Y') if scheme_details["aif_initialclosure_date"] else "NA"
            template_vars['aif_investment_style'] = scheme_details["aif_investment_style"] if scheme_details["aif_investment_style"] else "NA"
            template_vars['aif_investment_theme'] = scheme_details["aif_investment_theme"] if scheme_details["aif_investment_theme"] else "NA"
            template_vars["aif_min_investment_amount_in_cr"] = round(scheme_details["aif_min_investment_amount_in_cr"],2) if scheme_details["aif_min_investment_amount_in_cr"] else "NA"
            template_vars['aif_nri_investment_allowed'] = scheme_details["aif_nri_investment_allowed"] if scheme_details["aif_nri_investment_allowed"] else "NA"
            template_vars['aif_performance_fees'] = scheme_details["aif_performance_fees"] if scheme_details["aif_performance_fees"] else "NA"
            template_vars['aif_set_up_fees'] = scheme_details["aif_set_up_fees"] if scheme_details["aif_set_up_fees"] else "NA"
            template_vars["aif_sponsor_commitment_in_cr"] = round(scheme_details["aif_sponsor_commitment_in_cr"],2) if scheme_details["aif_sponsor_commitment_in_cr"] else "NA"
            template_vars['aif_subscription_status'] = scheme_details["aif_subscription_status"] if scheme_details["aif_subscription_status"] else "NA"
            template_vars["aif_target_fund_size_in_cr"] = round(scheme_details["aif_target_fund_size_in_cr"],2) if scheme_details["aif_target_fund_size_in_cr"] else "NA"
            template_vars['aif_tenure_of_fund_in_months'] = scheme_details["aif_tenure_of_fund_in_months"] if scheme_details["aif_tenure_of_fund_in_months"] else "NA"
            template_vars['isin'] = scheme_details["isin"] if scheme_details["isin"] else "NA"
            template_vars['fund_type'] = scheme_details["fund_type"] if scheme_details["fund_type"] else "NA"
        
        #calculate fund age
        tran_date = datetime.strptime(transactiondate, '%Y-%m-%d')
        template_vars["age"] = calculate_age(scheme_details["inception_date"], tran_date)
        
        cal_age = relativedelta(tran_date, scheme_details["inception_date"])
    # AIF debt 
    data_label = []

    for row in data['instrument_rating_1m_data']:
        instrument_dict = dict()
        instrument_dict = {
            'title':row['instrument_rating'],
            'values':row['value']
        }
        data_label.append(instrument_dict)

    template_vars["instrumentrating_data_html"] = get_pie_chart(data_label, "", False, 7, True, ('#228FCF', '#364EB9', '#36B9A5'), False, False, 300, 900)
    
    # AIF debt
    data_label = []

    for row in data['instrument_1m_data']:
        instrument_dict = dict()
        instrument_dict = {
            'title':row['instrument'],
            'values':row['value']
        }
        data_label.append(instrument_dict)

    template_vars["instrument_data_html"] = get_pie_chart(data_label, "", False, 7,
     True, ('#228FCF', '#364EB9', '#36B9A5'), False, False, 300, 900)

    # AIF debt
    data_label = []

    for row in data['secured_unsecured_1m_data']:
        instrument_dict = dict()
        instrument_dict = {
            'title':row['secured_unsecured'],
            'values':row['value']
        }
        data_label.append(instrument_dict)

    template_vars["secured_unsecured_data_html"] = get_pie_chart(data_label, "", False, 7, True, ('#228FCF', '#364EB9', '#36B9A5'), False, False, 300, 900)
    
    df_investmentstyle = data["historical_investmentstyle_data"]
    
    if not df_investmentstyle.empty:
        investmentstyle_data = df_investmentstyle.to_dict(orient='records')
        template_vars["trend_analysis_investmentstyle_historical_data_html"] = get_trand_analysis_stacked_chart(investmentstyle_data, 'style', 12, False, 'aum', True, 400, 1200, 4, 4)
    else:
        template_vars["trend_analysis_investmentstyle_historical_data_html"] = None

        
    template_vars["trend_analysis_marketcap_historical_data_html"] = get_trand_analysis_stacked_chart(data["historical_marketcap_data"], 'market_cap', 12, False, 'value', True, 400, 1200, 4, 4)
    
    template_vars["trend_analysis_sector_historical_data_html"] = get_trand_analysis_stacked_chart(data['historical_holdingsector_data'],'sector_name',12, False, 'value', True, 400, 1200, 4, 4)

    template_vars["trend_analysis_sector_historical_data_html"] = get_trand_analysis_stacked_chart(data['historical_holdingsector_data'],'sector_name',12, False, 'value', True, 400, 1200, 4, 4)

    template_vars["trend_analysis_instrument_historical_data_html"] = get_trand_analysis_stacked_chart(data['historical_instrument_data'],'instrument',12, False, 'value', True, 400, 1200, 4, 4)

    template_vars["trend_analysis_instrumentstyle_historical_data_html"] = get_trand_analysis_stacked_chart(data['historical_instrumentstyle_data'],'instrument_rating',12, False, 'value', True, 400, 1200, 4, 4)

    historical_aum_list = list()
    for dt in data['historical_aum_data']:
        data_list = list()
        data_list.append(dt['asofdate'])
        data_list.append(dt['aum_in_cr'])
        historical_aum_list.append(data_list)

    template_vars["trend_analysis_aum_data_html"] = get_line_chart(historical_aum_list, fontsize=8, in_miliseconds=True, add_range=False, show_legend=False, show_label=True, height=400, width=1100)
       


    #Fund Manager details
    template_vars["fundmanager_details"] = data["fundmanager_details"]

    #Fund Performance data
    template_vars["performance_data"] = data["performance_data"]
    if data["performance_data"]:
        x_labels = ['1 Month','3 Months','6 Months','1 Year','3 Years','5 Years','10 Years','Since Inception']
        data_label = []
        # fund
        fund_perf = []
        fund_perf.append(data["performance_data"]["scheme_ret_1m"])
        fund_perf.append(data["performance_data"]["scheme_ret_3m"])
        fund_perf.append(data["performance_data"]["scheme_ret_6m"])
        fund_perf.append(data["performance_data"]["scheme_ret_1y"])
        fund_perf.append(data["performance_data"]["scheme_ret_3y"])
        fund_perf.append(data["performance_data"]["scheme_ret_5y"])
        fund_perf.append(data["performance_data"]["scheme_ret_10y"])
        fund_perf.append(data["performance_data"]["scheme_ret_ince"])
        fund_dict = {
            'title':data["performance_data"]["plan_name"],
            'values':fund_perf
        }
        data_label.append(fund_dict)

        # benchmarkmark
        bm_perf = []
        bm_perf.append(data["performance_data"]["bm_ret_1m"])
        bm_perf.append(data["performance_data"]["bm_ret_3m"])
        bm_perf.append(data["performance_data"]["bm_ret_6m"])
        bm_perf.append(data["performance_data"]["bm_ret_1y"])
        bm_perf.append(data["performance_data"]["bm_ret_3y"])
        bm_perf.append(data["performance_data"]["bm_ret_5y"])
        bm_perf.append(data["performance_data"]["bm_ret_10y"])
        bm_perf.append(data["performance_data"]["bm_ret_ince"])
        bm_dict = {
            'title':data["performance_data"]["benchmark_name"],
            'values':bm_perf
        }
        data_label.append(bm_dict)

        encoded_string = get_barchart(x_labels,data_label, False, "", 11, 350, False, False, True)
        template_vars["performance_chart_src"] = encoded_string
    
    if not template_vars["hide_section_1"]:
        #Sector data
        template_vars["sector_data"] = data["sector_data"]
        template_vars["topholding_data"] = data["topholding_data"]
        data_list = list()    
        column_list = ["Sector", "Wts(%)"]

        for dt in data["sector_data"]:
            dict_data = dict()
            dict_data["Sector"] = dt["sector_name"]
            dict_data["Wts(%)"] = dt["sector_weight"]
            data_list.append(dict_data)
        
        template_vars["sector_data_html"]= get_table_from_html_template(data_list, column_list, 35)

        #Top Holding data
        template_vars["topholding_data"] = data["topholding_data"]
        data_list = list()    
        column_list = ["Top Holdings" if not is_debt else "Issuer Name", "Wts(%)"]

        for dt in data["topholding_data"]:
            dict_data = dict()
            dict_data["Top Holdings" if not is_debt else "Issuer Name"] = dt["security_name"]
            dict_data["Wts(%)"] = dt["security_weight"]
            data_list.append(dict_data)
        
        template_vars["topholding_data_html"]= get_table_from_html_template(data_list, column_list, 32)


        #composition data
        template_vars["composition_data"] = data["composition_data"]

        data_list = list()    
        column_list = ["Composition", "Wts(%)"]

        dict_data = dict()
        dict_data["Composition"] = "Equity"
        dict_data["Wts(%)"] = data["composition_data"]["equity"]
        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Composition"] = "Debt"
        dict_data["Wts(%)"] = data["composition_data"]["debt"]
        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Composition"] = "Cash"
        dict_data["Wts(%)"] = data["composition_data"]["cash"]
        data_list.append(dict_data)

        template_vars["composition_data_html"]= get_table_from_html_template(data_list, column_list, 30)


        #portfolio characteristics data
        template_vars["portfolio_characteristics_data"] = data["portfolio_characteristics_data"]
        if template_vars["portfolio_characteristics_data"]["scheme_asset_class_name"] == 'Debt':
            data_list = list()    
            column_list = ["Portfolio Characteristics", "Wts(%)"]

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Average Maturity Years"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["avg_maturity_yrs"] if data["portfolio_characteristics_data"]["avg_maturity_yrs"] else 'NA'
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Macauly Duration Years"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["macaulay_duration_yrs"] if data["portfolio_characteristics_data"]["macaulay_duration_yrs"] else 'NA'
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Modified Duration"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["modified_duration_yrs"] if data["portfolio_characteristics_data"]["modified_duration_yrs"] else 'NA'
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Yield To Maturity"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["yield_to_maturity"] if data["portfolio_characteristics_data"]["yield_to_maturity"] else 'NA'
            data_list.append(dict_data)

            template_vars["portfolio_characteristics_data_html"]= get_table_from_html_template(data_list, column_list, 30)
        else:
            data_list = list()    
            column_list = ["Portfolio Characteristics", "Wts(%)"]

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Total Stocks"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["total_stocks"] if data["portfolio_characteristics_data"]["total_stocks"] else 'NA'
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Avg Mkt Cap (Cr)"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["avg_mkt_cap"] if data["portfolio_characteristics_data"]["avg_mkt_cap"] else 'NA'
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Median Mkt Cap (Cr)"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["median_mkt_cap"] if data["portfolio_characteristics_data"]["median_mkt_cap"] else 'NA'
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Portfolio P/E Ratio"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["p_e"] if data["portfolio_characteristics_data"]["p_e"] else 'NA'
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Portfolio P/B Ratio"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["p_b"] if data["portfolio_characteristics_data"]["p_b"] else 'NA'
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Portfolio Characteristics"] = "Portfolio Dividend Yield"
            dict_data["Wts(%)"] = data["portfolio_characteristics_data"]["dividend_yield"] if data["portfolio_characteristics_data"]["dividend_yield"] else 'NA'
            data_list.append(dict_data)

            template_vars["portfolio_characteristics_data_html"]= get_table_from_html_template(data_list, column_list, 30)

        #riskratio data
        template_vars["riskratio_data"] = data["riskratio_data"]    
        data_list = list()    
        column_list = ["Risk Analysis", "1Y", "3Y"]

        dict_data = dict()
        dict_data["Risk Analysis"] = "Standard Deviation"
        dict_data["1Y"] = data["riskratio_data"]["standard_deviation_1_y"]
        dict_data["3Y"] = data["riskratio_data"]["standard_deviation_3_y"]
        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Risk Analysis"] = "Sharpe Ratio"
        dict_data["1Y"] = data["riskratio_data"]["sharpe_ratio_1_y"]
        dict_data["3Y"] = data["riskratio_data"]["sharpe_ratio_3_y"]
        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Risk Analysis"] = "Beta"
        dict_data["1Y"] = data["riskratio_data"]["beta_1_y"]
        dict_data["3Y"] = data["riskratio_data"]["beta_3_y"]
        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Risk Analysis"] = "R-Square"
        dict_data["1Y"] = data["riskratio_data"]["r_square_1_y"]
        dict_data["3Y"] = data["riskratio_data"]["r_square_3_y"]
        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Risk Analysis"] = "Treynor Ratio"
        dict_data["1Y"] = data["riskratio_data"]["treynor_ratio_1_y"]
        dict_data["3Y"] = data["riskratio_data"]["treynor_ratio_3_y"]
        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Risk Analysis"] = "Modified Duration"
        dict_data["1Y"] = None
        dict_data["3Y"] = None
        data_list.append(dict_data)

        template_vars["riskratio_data_html"]= get_table_from_html_template(data_list, column_list, 30)

        data_list = list()    
        column_list = ["Sector", "Wts(%)"]

        for dt in data["holdingsector_data"]:
            dict_data = dict()
            try:
                dict_data["Sector"] = dt["sector_name"]
                dict_data["Wts(%)"] = round(float(dt["sector_weight"]), 2) if dt["sector_weight"] else 'NA'
            except:
                dict_data["Sector"] = dt["sector_name"]
                dict_data["Wts(%)"] = round(float(dt["value"]), 2) if dt["value"] else 'NA'
            data_list.append(dict_data)
        
        template_vars["holdingsector_data_html"]= get_table_from_html_template(data_list, column_list, 35)

    
    if not template_vars["hide_section_2"]:
        #marketcap_composition data
        template_vars["marketcap_composition_data"] = data["marketcap_composition_data"]
        data_list = list()    
        column_list = ["Market Cap Composition", "%"]

        for dt in data["marketcap_composition_data"]:
            dict_data = dict()
            dict_data["Market Cap Composition"] = 'Large Cap'
            dict_data["%"] = dt["large_cap"]
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Market Cap Composition"] = 'Mid Cap'
            dict_data["%"] = dt["mid_cap"]
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Market Cap Composition"] = 'Small Cap'
            dict_data["%"] = dt["small_cap"]
            data_list.append(dict_data)
        
        template_vars["marketcap_composition_data_html"]= get_table_from_html_template(data_list, column_list, 27)

        #Risk rating
        template_vars["riskrating_data"] = data["riskrating_data"]
        data_list = list()    
        column_list = ["Risk Rating", "%"]

        for dt in data["riskrating_data"]:
            dict_data = dict()
            dict_data["Risk Rating"] = dt["risk_category"]
            dict_data["%"] = dt["Percentage_to_AUM"]
            data_list.append(dict_data)
        
        template_vars["riskrating_data_html"]= get_table_from_html_template(data_list, column_list, 18)

        #Instrument Type
        template_vars["instrumenttype_data"] = data["instrumenttype_data"]
        data_list = list()    
        column_list = ["Instrument Type", "%"]

        for dt in data["instrumenttype_data"]:
            dict_data = dict()
            dict_data["Instrument Type"] = dt["instrument_type"]
            dict_data["%"] = dt["Percentage_to_AUM"]
            data_list.append(dict_data)
        
        template_vars["instrumenttype_data_html"]= get_table_from_html_template(data_list, column_list, 20)

        #Investment style
        template_vars["investmentstyle_data"] = data["investmentstyle_data"]
        data_list = list()    
        column_list = ["Investment Style", "Blend", "Growth", "Value"]
        if data["investmentstyle_data"]:
            dict_data = dict()
            dict_data["Investment Style"] = "Large Cap"
            dict_data["Blend"] = data["investmentstyle_data"][0]["large_cap_blend"]
            dict_data["Growth"] = data["investmentstyle_data"][0]["large_cap_growth"]
            dict_data["Value"] = data["investmentstyle_data"][0]["large_cap_value"]
            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Investment Style"] = "Mid Cap"
            dict_data["Blend"] = data["investmentstyle_data"][0]["mid_cap_blend"]
            dict_data["Growth"] = data["investmentstyle_data"][0]["mid_cap_growth"]
            dict_data["Value"] = data["investmentstyle_data"][0]["mid_cap_value"]

            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Investment Style"] = "Small Cap"
            dict_data["Blend"] = data["investmentstyle_data"][0]["small_cap_blend"]
            dict_data["Growth"] = data["investmentstyle_data"][0]["small_cap_growth"]
            dict_data["Value"] = data["investmentstyle_data"][0]["small_cap_value"]

            data_list.append(dict_data)
        
        template_vars["investmentstyle_data_html1"]= get_table_from_html_template(data_list, column_list, 35)

    if not template_vars["hide_portfolio_holdings"]:
        #increase exposure
        template_vars["increaseexposure_data"] = data["increaseexposure_data"]
        
        data_list = list()    
        column_list = ["Security Name", "Increase in Wts(%)", "Current Wts(%)"]

        for dt in data["increaseexposure_data"]:
            dict_data = dict()
            dict_data["Security Name"] = dt["security_name"]
            dict_data["Increase in Wts(%)"] = dt["weight_difference"]
            dict_data["Current Wts(%)"] = dt["security_new_weight"]
            data_list.append(dict_data)
        
        template_vars["increaseexposure_data_html"]= get_table_from_html_template(data_list, column_list, 30)


        #Decrease exposure
        template_vars["decreaseexposure_data"] = data["decreaseexposure_data"]

        data_list = list()    
        column_list = ["Security Name", "Decrease in Wts(%)", "Current Wts(%)"]

        for dt in data["decreaseexposure_data"]:
            dict_data = dict()
            dict_data["Security Name"] = dt["security_name"]
            dict_data["Decrease in Wts(%)"] = dt["weight_difference"]
            dict_data["Current Wts(%)"] = dt["security_new_weight"]
            data_list.append(dict_data)
        
        template_vars["decreaseexposure_data_html"]= get_table_from_html_template(data_list, column_list, 30)


        #New Entrant
        template_vars["newentrants_data"] = data["newentrants_data"]

        data_list = list()    
        column_list = ["Security Name"]

        for dt in data["newentrants_data"]:
            dict_data = dict()
            dict_data["Security Name"] = dt["security_name"]
            data_list.append(dict_data)
        
        template_vars["newentrants_data_html"]= get_table_from_html_template(data_list, column_list, 20)


        #Complete exit
        template_vars["completeexit_data"] = data["completeexit_data"]

        data_list = list()    
        column_list = ["Security Name"]

        for dt in data["completeexit_data"]:
            dict_data = dict()
            dict_data["Security Name"] = dt["security_name"]
            data_list.append(dict_data)
        
        template_vars["completeexit_data_html"]= get_table_from_html_template(data_list, column_list, 20)

    if not template_vars["hide_performance_trend"]:
        #Performance Trend
        data["performance_data"]
        data_list = list()    
        column_list = ['Statistic', '1 Month','3 Month','6 Month','1 Year','3 Year','5 Year','Since Inception']
        
        dict_data = dict()
        dict_data["Statistic"] = "Portfolio Return"
        dict_data["1 Month"] = data["performance_data"]["scheme_ret_1m"]
        dict_data["3 Month"] = data["performance_data"]["scheme_ret_3m"]
        dict_data["6 Month"] = data["performance_data"]["scheme_ret_6m"]
        dict_data["1 Year"] = data["performance_data"]["scheme_ret_1y"]
        dict_data["3 Year"] = data["performance_data"]["scheme_ret_3y"]
        dict_data["5 Year"] = data["performance_data"]["scheme_ret_5y"]
        dict_data["Since Inception"] = data["performance_data"]["scheme_ret_ince"]

        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Statistic"] = "Index Return"
        dict_data["1 Month"] = data["performance_data"]["bm_ret_1m"]
        dict_data["3 Month"] = data["performance_data"]["bm_ret_3m"]
        dict_data["6 Month"] = data["performance_data"]["bm_ret_6m"]
        dict_data["1 Year"] = data["performance_data"]["bm_ret_1y"]
        dict_data["3 Year"] = data["performance_data"]["bm_ret_3y"]
        dict_data["5 Year"] = data["performance_data"]["bm_ret_5y"]
        dict_data["Since Inception"] = data["performance_data"]["bm_ret_ince"]

        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Statistic"] = "Active Return"
        dict_data["1 Month"] = data["performance_data"]["active_ret_1m"]
        dict_data["3 Month"] = data["performance_data"]["active_ret_3m"]
        dict_data["6 Month"] = data["performance_data"]["active_ret_6m"]
        dict_data["1 Year"] = data["performance_data"]["active_ret_1y"]
        dict_data["3 Year"] = data["performance_data"]["active_ret_3y"]
        dict_data["5 Year"] = data["performance_data"]["active_ret_5y"]
        dict_data["Since Inception"] = data["performance_data"]["active_ret_ince"]

        data_list.append(dict_data)

        
        if product_id == 1 or product_id == 2:
            dict_data = dict()
            dict_data["Statistic"] = "Fund Ranking #"
            dict_data["1 Month"] = data["performance_data"]["ranking_rank_1m"]
            dict_data["3 Month"] = data["performance_data"]["ranking_rank_3m"]
            dict_data["6 Month"] = data["performance_data"]["ranking_rank_6m"]
            dict_data["1 Year"] = data["performance_data"]["ranking_rank_1y"]
            dict_data["3 Year"] = data["performance_data"]["ranking_rank_3y"]
            dict_data["5 Year"] = data["performance_data"]["ranking_rank_5y"]
            dict_data["Since Inception"] = ''

            data_list.append(dict_data)

            dict_data = dict()
            dict_data["Statistic"] = "Total No of Funds #"
            dict_data["1 Month"] = data["performance_data"]["count_1m"]
            dict_data["3 Month"] = data["performance_data"]["count_3m"]
            dict_data["6 Month"] = data["performance_data"]["count_6m"]
            dict_data["1 Year"] = data["performance_data"]["count_1y"]
            dict_data["3 Year"] = data["performance_data"]["count_3y"]
            dict_data["5 Year"] = data["performance_data"]["count_5y"]
            dict_data["Since Inception"] = ''

            data_list.append(dict_data)

        template_vars["performancetrend_data_html"]= get_table_from_html_template(data_list, column_list, 100)

    if not template_vars["hide_nav_movement"]:
        #NAV
        nav_encoded_string = get_linechart(x_labels,data["nav_data"])
        template_vars["nav_chart_src"] = nav_encoded_string
    
    #get rolling return 
    rollingdata = list()
    
    if not template_vars["hide_rolling_return_1"]:
        #1 year
        if data["rollingretrun_1yr_data"]:
            if cal_age.years >= 1:
                rollingreturn_data = dict()
                rollingreturn_data["RollingReturn_title"] = 'Rolling Return - 1 Year'
                rollingreturn_data["RollingReturn_data"] = get_rollingreturn_html(data, "rollingretrun_1yr_data")

                rollingdata.append(rollingreturn_data)

    if not template_vars["hide_rolling_return_3"]:
        #3 year
        if data["rollingretrun_3yr_data"]:
            if cal_age.years >= 3:
                rollingreturn_data = dict()
                rollingreturn_data["RollingReturn_title"] = 'Rolling Return - 3 Year`s'
                rollingreturn_data["RollingReturn_data"] = get_rollingreturn_html(data, "rollingretrun_3yr_data")

                rollingdata.append(rollingreturn_data)

    if not template_vars["hide_rolling_return_5"]:
    #5 year
        if data["rollingretrun_5yr_data"]:
            if cal_age.years >= 5:
                rollingreturn_data = dict()
                rollingreturn_data["RollingReturn_title"] = 'Rolling Return - 5 Year`s'
                rollingreturn_data["RollingReturn_data"] = get_rollingreturn_html(data, "rollingretrun_5yr_data")

                rollingdata.append(rollingreturn_data)

    template_vars["rollingreturn_data"] = rollingdata
    template_vars["rollingreturn_type"] = 'Annualized' if is_annualized_return else 'Absolute'

    #get active rolling return 
    active_rollingdata = list()
    
    if not template_vars["hide_active_rolling_return_1"]:
        #1 year
        if data["active_rollingretrun_1yr_data"]:
            
            active_rollingreturn_data = dict()
            active_rollingreturn_data["RollingReturn_title"] = 'Active Rolling Return - 1 Year'
            active_rollingreturn_data["RollingReturn_data"] = get_rollingreturn_html(data, "active_rollingretrun_1yr_data", True)

            active_rollingdata.append(active_rollingreturn_data)
    
    if not template_vars["hide_active_rolling_return_3"]:
        #3 years
        if data["active_rollingretrun_3yr_data"]:
            
            active_rollingreturn_data = dict()
            active_rollingreturn_data["RollingReturn_title"] = 'Active Rolling Return - 3 Year'
            active_rollingreturn_data["RollingReturn_data"] = get_rollingreturn_html(data, "active_rollingretrun_3yr_data", True)

            active_rollingdata.append(active_rollingreturn_data)
    
    template_vars["active_rollingreturn_data"] = active_rollingdata
    

    if not template_vars["hide_performance_graph"]:
        #Performance 
        #MOM
        performance_data = list()
        performance_data.append(get_performance_html(data, 'performance_mom_data', transactiondate, 'Month on Month'))
        performance_data.append(get_performance_html(data, 'performance_fy_data', transactiondate, 'Financial Year'))
        performance_data.append(get_performance_html(data, 'performance_cy_data', transactiondate, 'Calendar Year'))
        template_vars["rollingreturn_performance_data"] = performance_data

    if not template_vars["hide_risk_volatility_measures"]:
        #Risk and Volatility 
        risk_and_volatility = list()
        
        risk_data = dict()
        risk_data["title"] = 'Risk & Volatility Measures - 1 Year'
        risk_data["risk_and_vol_data"] = prepare_risk_volatility_data(data["risk_volatility_data"], '1')

        risk_and_volatility.append(risk_data)

        risk_data = dict()
        risk_data["title"] = 'Risk & Volatility Measures - 3 Year`s'
        risk_data["risk_and_vol_data"] = prepare_risk_volatility_data(data["risk_volatility_data"], '3')

        risk_and_volatility.append(risk_data)

        template_vars["risk_and_volatility_data"] = risk_and_volatility

    if not template_vars["hide_detailed_portfolio"]:
        template_vars["portfolio_holdings_data"] = data["portfolio_holdings_data"]
        #portfolio holdings
        if not is_debt:
            data_list = list()    
            column_list = ["Security Name", "Wts (%)", "Sector", "Market Cap", "Instrument Type", "Risk Rating", "Investment Style", "Held Since"]

            if data["portfolio_holdings_data"]:
                for dt in data["portfolio_holdings_data"]:
                    dict_data = dict()
                    dict_data["Security Name"] = dt["holdingsecurity_name"]
                    dict_data["Wts (%)"] = round(dt["percentage_to_aum"],2) if dt["percentage_to_aum"] else 'NA'
                    dict_data["Sector"] = dt["sector_name"]
                    dict_data["Market Cap"] = dt["market_cap"]
                    dict_data["Instrument Type"] = dt["instrument_type"]
                    dict_data["Risk Rating"] = dt["risk_category"]
                    dict_data["Investment Style"] = dt["equity_style"]
                    dict_data["Held Since"] = dt["purchasedate"]
                    
                    data_list.append(dict_data)
            
            template_vars["portfolio_holdings_data_html"] = get_table_from_html_template(data_list, column_list, 100)
        else:
            data_list = list()    
            column_list = ["ISIN", "Issuer Name", "Instrument", "Rating", "Wts (%)", "Amount Invested (In Cr)"]

            if data["portfolio_holdings_data"]:
                for dt in data["portfolio_holdings_data"]:
                    dict_data = dict()
                    dict_data["Issuer Name"] = dt["issuer_name"]
                    dict_data["ISIN"] = dt["isin"]
                    dict_data["Instrument"] = dt["instrument"]
                    dict_data["Rating"] = dt["rating"]
                    dict_data["Wts (%)"] = round(dt["amount_invested_in_cr"],2) if dt["amount_invested_in_cr"] else 'NA'
                    dict_data["Amount Invested (In Cr)"] = round(float(dt["amount_invested_in_cr"]), 2)
                    
                    data_list.append(dict_data)
            
            template_vars["portfolio_holdings_data_html"] = get_table_from_html_template(data_list, column_list, 100)

    if not template_vars["hide_attribution"]:
        if data["attribution_data"]:
            template_vars["attribution_data_html"] = get_attribution_html_data(data["attribution_data"])
        else:
            template_vars["attribution_data_html"] = None
        
        template_vars["show_attribution_benchmark"] = False
        if attribution_benchmark_id:
            if data["attribution_benchmark_master_data"]:
                benchmark_details = data["attribution_benchmark_master_data"]
                
                template_vars["show_attribution_benchmark"] = True if benchmark_details else False
                template_vars["attribution_benchmark_name"] = benchmark_details.BenchmarkIndices_Name if benchmark_details else ''
    
    return prepare_pdf_from_html(template_vars, 'factsheethtml_template.html', report_generation_path)
    #Log error
    # logger = logging.getLogger('weasyprint')
    # logger.addHandler(logging.FileHandler('/path/to/weasyprint.log'))
    # https://kozea.github.io/WeasyPerf/samples/doc/doc.html
    # https://stackoverflow.com/questions/9198334/how-to-build-up-a-html-table-with-a-simple-for-loop-in-jinja2

def prepare_risk_volatility_data(data, period):
    
    column_list = ['Statistic','Current Date','3 Months','6 Months','1 Year','3 Years']

    final_data = list()
    final_data.append(prepare_risk_volatility_data_by_ratio(data, 'standard_deviation_' + str(period) +'_y', 'Standard Deviation'))
    final_data.append(prepare_risk_volatility_data_by_ratio(data, 'sharpe_ratio_' + str(period) +'_y', 'Sharpe Ratio'))
    final_data.append(prepare_risk_volatility_data_by_ratio(data, 'treynor_ratio_' + str(period) +'_y', 'Treynor Ratio'))
    final_data.append(prepare_risk_volatility_data_by_ratio(data, 'beta_' + str(period) +'_y', 'Beta'))
    final_data.append(prepare_risk_volatility_data_by_ratio(data, 'r_square_' + str(period) +'_y', 'R-Square'))
    final_data.append(prepare_risk_volatility_data_by_ratio(data, 'total_stocks', 'Total Stocks'))
    final_data.append(prepare_risk_volatility_data_by_ratio(data, 'pe_ratio', 'Portfolio P/E Ratio'))

    res = get_table_from_html_template(final_data, column_list, 100)

    return res

def get_attribution_html_data(data):
    resp = dict()
    #convert string to object
    att_data = json.loads(data)

    #attribution_summary
    column_list = ['Portfolio Return(%)', 'Benchmark Return(%)', 'Active Return(%)', 'Allocation Effect(%)', 'Selection Effect(%)', 'Interaction Effect(%)','Timing Effect(%)', 'Cash Contribution(%)']

    attribution_summary_data = list()
    dict_data = dict()
    dict_data["Portfolio Return(%)"] = round((float(att_data['attribution_summary'][0]['fund_returns']) * 100),2) if att_data['attribution_summary'][0]['fund_returns'] else 'NA'

    dict_data["Benchmark Return(%)"] = round((float(att_data['attribution_summary'][0]['index_returns']) * 100),2) if att_data['attribution_summary'][0]['index_returns'] else 'NA'

    dict_data["Active Return(%)"] = round((float(att_data['attribution_summary'][0]['excess_returns']) * 100),2) if att_data['attribution_summary'][0]['index_returns'] else 'NA'

    dict_data["Allocation Effect(%)"] = round((float(att_data['attribution_summary'][0]['sector_allocation']) * 100),2) if att_data['attribution_summary'][0]['sector_allocation'] else 'NA'

    dict_data["Selection Effect(%)"] = round((float(att_data['attribution_summary'][0]['stock_selection']) * 100),2) if att_data['attribution_summary'][0]['stock_selection'] else 'NA'

    dict_data["Interaction Effect(%)"] = round((float(att_data['attribution_summary'][0]['interaction']) * 100),2) if att_data['attribution_summary'][0]['interaction'] else 'NA'

    dict_data["Timing Effect(%)"] = round((float(att_data['attribution_summary'][0]['timing']) * 100),2) if att_data['attribution_summary'][0]['timing'] else 'NA'

    dict_data["Cash Contribution(%)"] = round((float(att_data['attribution_summary'][0]['cash_active_contribution']) * 100),2) if att_data['attribution_summary'][0]['cash_active_contribution'] else 'NA'
    attribution_summary_data.append(dict_data)

    resp["attribution_summary_html_data"] = get_table_from_html_template(attribution_summary_data, column_list, 100)

    if att_data:
        #Alpha Generators
        top_detractors = att_data['top_detractors'][0]
        top_detractor = get_unique_list(top_detractors, False)
        resp["attribution_top_detractors_html_data"] = get_attribution_htmltable_bydata(top_detractor)
        
        #top_performers
        top_performers = att_data['top_performers'][0]
        top_performers = get_unique_list(top_performers, True)
        resp["attribution_top_performers_html_data"] = get_attribution_htmltable_bydata(top_performers)

        #overweight stocks out
        overweight_performing = att_data['contribution_details'][0]['overweight_performing']
        overweight_performing = get_unique_list(overweight_performing)
        resp["attribution_overweight_performing_html_data"] = get_attribution_htmltable_bydata(overweight_performing)

        #overweight stocks under
        overweight_lagging = att_data['contribution_details'][0]['overweight_lagging']
        overweight_lagging = get_unique_list(overweight_lagging, False)
        resp["attribution_overweight_lagging_html_data"] = get_attribution_htmltable_bydata(overweight_lagging)

        #underweight stocks out
        underweight_performing = att_data['contribution_details'][0]['underweight_performing']    
        underweight_performing = get_unique_list(underweight_performing, False)
        resp["attribution_underweight_performing_html_data"] = get_attribution_htmltable_bydata(underweight_performing)

        #underweight stocks under 
        underweight_lagging = att_data['contribution_details'][0]['underweight_lagging']    
        underweight_lagging = get_unique_list(underweight_lagging, True)
        resp["attribution_underweight_lagging_html_data"] = get_attribution_htmltable_bydata(underweight_lagging)

        #BM stocks out
        exclusions_performing = att_data['contribution_details'][0]['exclusions_performing']
        exclusions_performing = get_unique_list(exclusions_performing, False)
        resp["attribution_exclusions_performing_html_data"] = get_attribution_htmltable_bydata(exclusions_performing)

        #BM stocks under
        exclusions_lagging = att_data['contribution_details'][0]['exclusions_lagging']
        exclusions_lagging = get_unique_list(exclusions_lagging, True)
        resp["attribution_exclusions_lagging_html_data"] = get_attribution_htmltable_bydata(exclusions_lagging)

        #stocks invested outside BM out
        additions_performing = att_data['contribution_details'][0]['additions_performing']
        additions_performing = get_unique_list(additions_performing, True)
        resp["attribution_additions_performing_html_data"] = get_attribution_htmltable_bydata(additions_performing)

        #stocks invested outside BM under
        additions_lagging = att_data['contribution_details'][0]['additions_lagging']
        additions_lagging = get_unique_list(additions_lagging, False)
        resp["attribution_additions_lagging_html_data"] = get_attribution_htmltable_bydata(additions_lagging)

    return resp

def get_attribution_htmltable_bydata(data):
    column_list = ['Security Name', 'Contribution(%)']

    attribution_data = list()
    counter = 0
    for dt in data:
        if counter < 5:
            dict_data = dict()
            dict_data["Security Name"] = dt['Name']
            dict_data["Contribution(%)"] = round((float(dt['active_contribution']) * 100),2) if dt['active_contribution'] else 'NA'
            attribution_data.append(dict_data)

            counter = counter + 1
        else:
            break
    
    return get_table_from_html_template(attribution_data, column_list, 50)



def get_unique_list(data, sort=True):
    res = [] 
    dt = list({dt["ISIN"]: dt for dt in data}.values())
    newlist = sorted(dt, key=lambda d: d['active_contribution'], reverse=sort) 
    if sort:        
        for a in newlist:  
            if a['active_contribution']>0: 
                res.append(a) 
    else:
        for a in newlist:  
                if a['active_contribution']<0: 
                    res.append(a) 

    return res
    

def prepare_risk_volatility_data_by_ratio(data, ratio_attribute_name, ratio_name):
    temp = dict()
    temp["Statistic"] = ratio_name
    for dt in data:        
        if dt == "current_date":            
            temp["Current Date"] = data[dt][ratio_attribute_name] if ratio_attribute_name in data[dt] else None     
        elif dt == "3_month":
            temp["3 Months"] = data[dt][ratio_attribute_name] if ratio_attribute_name in data[dt] else None
        elif dt == "6_month":
            temp["6 Months"] = data[dt][ratio_attribute_name] if ratio_attribute_name in data[dt] else None
        elif dt == "1_year":
            temp["1 Year"] = data[dt][ratio_attribute_name] if ratio_attribute_name in data[dt] else None
        elif dt == "3_year":
            temp["3 Years"] = data[dt][ratio_attribute_name] if ratio_attribute_name in data[dt] else None
    return temp
    

def get_performance_html(data, per_type, transaction_date, columnname):
    resp = dict()
    data_list = list()
    chart_x_label_list = list()
    dict_chartdata = list()

    #prepare table data
    column_list = [columnname, '']
    data = sorted(data[per_type], key=itemgetter('year', 'month'))
    
    for dt in data:
        dict_data = dict()
        dict_data[columnname] = dt["period"] 
        dict_data[""] = round(dt["performance"],2) if dt["performance"] else 'NA'
        data_list.append(dict_data)

        #chart data 
        dict_chartdata.append(round(dt["performance"],2))
        chart_x_label_list.append(dt["period"])

    temp = dict()
    temp["title"] = columnname
    temp["values"] = dict_chartdata

    resp["title"] = columnname
    resp["performance_table_html_data"] = get_table_from_html_template(data_list, column_list, 20)
    resp["performance_chart_html_data"] = get_barchart(chart_x_label_list, [temp], True, columnname, 16, 350, False, False, False)

    return resp


def get_rollingreturn_html(data, timeframe, for_active_rolling_return = False):
    res = dict()

    data_list = list()    
    column_list = ['Index/Strategy', 'Minimum','Median','Maximum','Average']
    
    if not for_active_rolling_return:
        dict_data = dict()
        dict_data["Index/Strategy"] = data[timeframe]["scheme_name"]
        dict_data["Minimum"] = data[timeframe]["scheme_min_return"]
        dict_data["Median"] = data[timeframe]["scheme_med_return"]
        dict_data["Maximum"] = data[timeframe]["scheme_max_return"]
        dict_data["Average"] = data[timeframe]["scheme_avg_return"]

        data_list.append(dict_data)

        dict_data = dict()
        dict_data["Index/Strategy"] = data[timeframe]["benchmark_name"]
        dict_data["Minimum"] = data[timeframe]["benchmark_min_return"]
        dict_data["Median"] = data[timeframe]["benchmark_med_return"]
        dict_data["Maximum"] = data[timeframe]["benchmark_max_return"]
        dict_data["Average"] = data[timeframe]["benchmark_avg_return"]

        data_list.append(dict_data)
    else:#for active rolling return
        dict_data = dict()
        dict_data["Index/Strategy"] = ''
        dict_data["Minimum"] = data[timeframe]["min_returns"]
        dict_data["Median"] = data[timeframe]["median_returns"]
        dict_data["Maximum"] = data[timeframe]["max_returns"]
        dict_data["Average"] = data[timeframe]["average_returns"]

        data_list.append(dict_data)

    res["rollingreturn_table_data_html"] = get_table_from_html_template(data_list, column_list, 50)


    #chart data preparation
    x_labels = ['Minimum','Median','Maximum','Average']
    data_label = []

    if not for_active_rolling_return:
        # fund
        fund_return = []
        fund_return.append(data[timeframe]["scheme_min_return"])
        fund_return.append(data[timeframe]["scheme_med_return"])
        fund_return.append(data[timeframe]["scheme_max_return"])
        fund_return.append(data[timeframe]["scheme_avg_return"])

        fund_dict = {
            'title':data[timeframe]["scheme_name"],
            'values':fund_return
        }
        data_label.append(fund_dict)

        # benchmark
        benchmark_return = []
        benchmark_return.append(data[timeframe]["benchmark_min_return"])
        benchmark_return.append(data[timeframe]["benchmark_med_return"])
        benchmark_return.append(data[timeframe]["benchmark_max_return"])
        benchmark_return.append(data[timeframe]["benchmark_avg_return"])

        fund_dict = {
            'title':data[timeframe]["benchmark_name"],
            'values':benchmark_return
        }
        data_label.append(fund_dict)
    else:
        active_return = []
        active_return.append(data[timeframe]["min_returns"])
        active_return.append(data[timeframe]["median_returns"])
        active_return.append(data[timeframe]["max_returns"])
        active_return.append(data[timeframe]["average_returns"])
        
        fund_dict = {
            'title':'',
            'values':active_return
        }
        data_label.append(fund_dict)


    encoded_string = get_barchart(x_labels,data_label,False, "", 24, 450, False, False, False)
    res["rollingreturn_chart_src"] = encoded_string
    
    #observations
    if data[timeframe]["observation_breakup"]:               

        data_list = list()    
        column_list = ['Return (%)', 'Observation No','Observation (%)']

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "above75", "Above 75%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between50_75", "50% to 75%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between40_50", "40% to 50%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between30_40", "30% to 40%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between20_30", "20% to 30%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between10_20", "10% to 20%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between1_10", "1% to 10%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between_m1_1", "1% to -1%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between_m1_m10", "-1% to -10%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between_m10_m20", "-10% to -20%"))

        data_list.append(get_rollingreturn_observation_breakup_bykey(data[timeframe]["observation_breakup"], "between_m20_m30", "-20% to -30%"))        

        res["rollingreturn_observations_table_data_html"] = get_table_from_html_template(data_list, column_list, 50)
    
    #Rolling Return Observations
    data_list = list()    
    column_list = ['Rolling Return Observations', '']

    dict_data = dict()
    dict_data["Rolling Return Observations"] = "Total observations"
    dict_data[""] = data[timeframe]["total_observations_no"]
    data_list.append(dict_data)

    dict_data = dict()
    dict_data["Rolling Return Observations"] = "Total number of positive return observations "
    dict_data[""] = data[timeframe]["positive_observation_no"]
    data_list.append(dict_data)

    dict_data = dict()
    dict_data["Rolling Return Observations"] = "Total number of neutral returns observations (between 1% to -1%)"
    dict_data[""] = data[timeframe]["neutral_observation_no"]
    data_list.append(dict_data)

    dict_data = dict()
    dict_data["Rolling Return Observations"] = "Total number of negative return observations "
    dict_data[""] = data[timeframe]["negative_observation_no"]
    data_list.append(dict_data)

    res["rollingreturn_rolling_observations_table_data_html"] = get_table_from_html_template(data_list, column_list, 50)

    #rolling observation chart
    data_label = []

    rollingret_dict = dict()
    rollingret_dict = {
        'title':"Positive",
        'values':data[timeframe]["positive_observation_perc"]
    }
    data_label.append(rollingret_dict)

    rollingret_dict = dict()
    rollingret_dict = {
        'title':"Neutral",
        'values':data[timeframe]["neutral_observation_perc"]
    }
    data_label.append(rollingret_dict)

    rollingret_dict = dict()
    rollingret_dict = {
        'title':"Negative",
        'values':data[timeframe]["negative_observation_perc"]
    }
    data_label.append(rollingret_dict)

    res["rollingreturn_rolling_observations_chart_html"] = get_pie_chart(data_label, "Total " + str(data[timeframe]["total_observations_no"]), True, 12, False, ('#228FCF', '#364EB9', '#36B9A5'), False, False, 300, 900)

    return res

def get_rollingreturn_observation_breakup_bykey(data, key, label):    
    dict_data = dict()
    if key in data.keys():
        dict_data["Return (%)"] = label
        dict_data["Observation No"] = data[key]
        dict_data["Observation (%)"] = data[key + "_perc"]
    else:
        dict_data["Return (%)"] = label
        dict_data["Observation No"] = 'NA'
        dict_data["Observation (%)"] = 'NA'

    return dict_data


def get_factsheet_data(db_session, plan_id, transactiondate, portfolio_date, attribution_from_date, attribution_to_date, organization_id, data_to_fetch, gsquare_url, attribution_benchmark_id="", is_annualized_return=False):
    factsheet_data = dict()
    #get schemedetails
    factsheet_data["scheme_details"] = get_scheme_details(db_session, plan_id, transactiondate)

    #get fund manager
    factsheet_data["fundmanager_details"] = getfundmanager(db_session, plan_id)

    #get performance
    factsheet_data["performance_data"] = get_performancetrend_data(db_session, plan_id, transactiondate)

    #get AIF debt - instrument data
    factsheet_data["performance_data"] = get_performancetrend_data(db_session, plan_id, transactiondate)

    factsheet_data["secured_unsecured_1m_data"] = get_securedunsecured_data(db_session, plan_id, portfolio_date)  

    factsheet_data["instrument_rating_1m_data"] = get_portfolio_instrumentrating_data(db_session, plan_id, portfolio_date)

    factsheet_data["instrument_1m_data"] = get_portfolio_instrument_data(db_session, plan_id, portfolio_date)

    #trend analysis - aif debt
    factsheet_data["historical_holdingsector_data"] = get_holdings_sector_data(db_session, plan_id, None)

    factsheet_data["historical_investmentstyle_data"] = investmentstyle_month_wise(db_session, plan_id, transactiondate)

    factsheet_data["historical_marketcap_data"] = marketcapcomposition_month_wise(db_session, plan_id, transactiondate)
    
    
    factsheet_data["historical_instrument_data"] = get_portfolio_instrument_data(db_session, plan_id, None)

    factsheet_data["historical_instrumentstyle_data"] = get_portfolio_instrumentrating_data(db_session, plan_id, None)

    # TODO : This requires change as per new return object from get_aum_monthwise. @Sachin to revisit this logic
    factsheet_data["historical_aum_data"] = get_aum_monthwise(db_session, plan_id, transactiondate, True)

    transaction_date_python = parser.parse(transactiondate)


    if not data_to_fetch["hide_section_1"]:
        #get sectors
        factsheet_data["sector_data"] = resp = get_sectorweightsdata(db_session, plan_id, transactiondate, composition_for = 'fund_level')

        #Get Composition data
        factsheet_data["composition_data"] = get_compositiondata(db_session, plan_id, transactiondate, composition_for='fund_level')

        #get portfolio characteristics
        port_ratios = get_portfolio_characteristics(db_session, plan_id, datetime.strptime(transactiondate, '%Y-%m-%d').date() if transactiondate else None)

        # delta indicated the fallback date for which prices of securities will be fetched
        dict_ratios = calculate_portfolio_level_analysis(db_session, plan_id, datetime.strptime(portfolio_date, '%d %b %Y').date() if portfolio_date else None, delta=3)
        port_ratios.update(dict_ratios)

        factsheet_data["portfolio_characteristics_data"] = port_ratios

        #get Risk Ratio
        factsheet_data["riskratio_data"] = get_fundriskratio_data(db_session, plan_id, transactiondate)
        
        #Holdings
        factsheet_data["topholding_data"] = get_fund_holdings(db_session, plan_id, portfolio_date)

        #used to show aif debt sector
        factsheet_data["holdingsector_data"] = get_holdings_sector_data(db_session, plan_id, portfolio_date)

    if not data_to_fetch["hide_section_2"]:
        #Marketcap composition
        factsheet_data["marketcap_composition_data"] = get_marketcap_composition(db_session, plan_id, transactiondate, 'fund_level')

        #Risk rating
        factsheet_data["riskrating_data"] = get_riskrating(db_session, plan_id, transactiondate)

        #Instrument type
        factsheet_data["instrumenttype_data"] = get_instrumenttype(db_session, plan_id, transactiondate)

        #investment style
        factsheet_data["investmentstyle_data"] = get_investmentstyle(db_session, plan_id, transactiondate)
    if not data_to_fetch["hide_portfolio_holdings"]:
        #Increase in exposure
        factsheet_data["increaseexposure_data"] = get_fund_portfolio_change(db_session, plan_id, 'increase', transactiondate)

        #Decrease in exposure
        factsheet_data["decreaseexposure_data"] = get_fund_portfolio_change(db_session, plan_id, 'decrease', transactiondate)

        #New Entrants
        factsheet_data["newentrants_data"] = get_fund_portfolio_change(db_session, plan_id, 'entry', transactiondate)

        #Complete Exit
        factsheet_data["completeexit_data"] = get_fund_portfolio_change(db_session, plan_id, 'exit', transactiondate)


    if not data_to_fetch["hide_nav_movement"]:
        #NAV
        factsheet_data["nav_data"] = get_fund_nav(db_session, plan_id, transactiondate)

    if not data_to_fetch["hide_rolling_return_1"]:
        #Rolling Return 1 year
        factsheet_data["rollingretrun_1yr_data"] = get_rollingreturn(db_session, plan_id, transactiondate, 1, is_annualized_return=is_annualized_return)

    if not data_to_fetch["hide_rolling_return_3"]:
        # #Rolling Return 3 year
        factsheet_data["rollingretrun_3yr_data"] = get_rollingreturn(db_session, plan_id, transactiondate, 3, is_annualized_return=is_annualized_return)

    if not data_to_fetch["hide_rolling_return_5"]:
        # #Rolling Return 5 year
        factsheet_data["rollingretrun_5yr_data"] = get_rollingreturn(db_session, plan_id, transactiondate, 5, is_annualized_return=is_annualized_return)

    if not data_to_fetch["hide_active_rolling_return_1"]:
        # #Active Rolling Return 1 year
        factsheet_data["active_rollingretrun_1yr_data"] = generate_active_rolling_returns(db_session, plan_id, None, 1, transactiondate)

    if not data_to_fetch["hide_active_rolling_return_3"]:
        # #Active Rolling Return 3 year
        factsheet_data["active_rollingretrun_3yr_data"] = generate_active_rolling_returns(db_session, plan_id, None, 3, transactiondate)

    if not data_to_fetch["hide_performance_graph"]:
        factsheet_data["performance_mom_data"] = get_fund_historic_performance(db_session, plan_id, None, None, 'MOM', transaction_date_python)

        factsheet_data["performance_fy_data"] = get_fund_historic_performance(db_session, plan_id, None, None, 'FY', transaction_date_python)

        factsheet_data["performance_cy_data"] = get_fund_historic_performance(db_session, plan_id, None, None, 'CY', transaction_date_python)


    if not data_to_fetch["hide_risk_volatility_measures"]:
        factsheet_data["risk_volatility_data"] = get_detailed_fund_risk_ratios(db_session, plan_id, transactiondate)

    if not data_to_fetch["hide_attribution"]:
        if attribution_from_date and attribution_to_date:
            try:
                factsheet_data["attribution_data"] = get_attributions(db_session,parser.parse(attribution_from_date), parser.parse(attribution_to_date), plan_id, gsquare_url, attribution_benchmark_id)
            except:
                factsheet_data["attribution_data"] = None
        else:
            factsheet_data["attribution_data"] = None

    if not data_to_fetch["hide_detailed_portfolio"]:
        #get portfolio date
        portfolio_date = get_portfolio_date(db_session, plan_id, transactiondate)

        if portfolio_date: 
            factsheet_data["portfolio_holdings_data"] = get_detailed_fund_holdings(db_session, plan_id, None, portfolio_date, get_full_holding=False)
        else:
            factsheet_data["portfolio_holdings_data"] = None

    factsheet_data["organization_whitelabel_data"] = get_organization_whitelabel(db_session, organization_id)

    if attribution_benchmark_id:
        factsheet_data["attribution_benchmark_master_data"] = benchmark_details = get_benchmarkdetails(db_session, attribution_benchmark_id)

    return factsheet_data


def get_fundcomparepdf(db_session, organization_id, image_path, whitelabel_dir, report_generation_path, list_plans):
    template_vars = dict()
    
    organization_whitelabel_data = get_organization_whitelabel(db_session, organization_id)

    if organization_whitelabel_data:
        template_vars["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{organization_whitelabel_data['logo']}"
        template_vars["disclaimer_text"] = organization_whitelabel_data['disclaimer']
        
    for index, plan in enumerate(list_plans):
        plan_id = plan["plan_id"]
        portfolio_date = None
        transaction_date = datetime.strftime(datetime.strptime(plan["transaction_date"], '%d %b %Y'), '%Y-%m-%d') 
        data = dict()

        data = get_fundcomparedata_planwise(db_session, transaction_date, plan_id)
        data["amc_imagepath"] = F"{image_path}/{data['amc_logo']}"

        data = beautify_fund_compare_data(data, image_path)
        
        html_body = prepare_pdf_from_html(data, 'fund_compare_table_component.html', report_generation_path, True)
        template_vars[F"fund_{index + 1}_html"] = html_body

    file_name = prepare_pdf_from_html(template_vars, 'fundcomparehtml_template.html', report_generation_path)

    return file_name

def get_portfoliopdf(db_session, organization_id, image_path, whitelabel_dir, generatereport_dir, data):
    template_vars = dict()
    
    organization_whitelabel_data = get_organization_whitelabel(db_session, organization_id)

    if organization_whitelabel_data:
        template_vars["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{organization_whitelabel_data['logo']}"
        template_vars["disclaimer_text"] = organization_whitelabel_data['disclaimer']
        
    template_vars = beautify_portfolio_data(template_vars, data)

    file_name = prepare_pdf_from_html(template_vars, 'portfoliohtml_template.html', generatereport_dir)

    return file_name

def beautify_portfolio_data(template_vars, data):
    template_vars['current_date'] = datetime.strftime(date.today(),'%d %b %Y')
    template_vars['portfolio_date'] = data['portfolio_date']
    template_vars["is_historic"] = True if data["is_historic"] == 1 else False
    template_vars["label"] = data["investor_details"]["label"]
    template_vars["pan_no"] = data["investor_details"]["pan_no"]
    #Holding table
    data_list = list()    
    column_list = ["Top Holdings(Top 10)", "Wts(%)"]

    index = 0
    for dt in data["securities"]:
        dict_data = dict()
        dict_data["Top Holdings(Top 10)"] = dt["name"]
        dict_data["Wts(%)"] = round(float(dt["weight"]),2) if dt["weight"] else 0
        data_list.append(dict_data)

        index += 1
        if index == 10:
            break
    
    template_vars["topholding_data_html"]= get_table_from_html_template(data_list, column_list, 33)

    #Instrument Type chart
    data_label = []

    for dt in data["instrument_types"]:
        instrumenttype_dict = dict()
        instrumenttype_dict = {
            'title':dt['instrument_type'],
            'values':round(float(dt["weight"]),2) if dt["weight"] else 0
        }
        data_label.append(instrumenttype_dict)

    template_vars["instrument_type_chart_html"] = get_pie_chart(data_label, '', False, 7, True, ('#228FCF', '#364EB9', '#36B9A5'), False, False, 300, 900)

    #sector table
    data_list = list()    
    column_list = ["Sector(Top 10)", "Wts(%)"]
    index = 0
    for dt in data["sectors"]:
        dict_data = dict()
        dict_data["Sector(Top 10)"] = dt["sector"]
        dict_data["Wts(%)"] = round(float(dt["weight"]),2) if dt["weight"] else 0
        data_list.append(dict_data)
        index += 1
        if index == 10:
            break
    
    template_vars["sector_data_html"] = get_table_from_html_template(data_list, column_list, 35)

    #Issuer table
    data_list = list()    
    column_list = ["Issuer", "Wts(%)"]

    index = 0
    for dt in data["issuers"]:
        dict_data = dict()
        dict_data["Issuer"] = dt["issuer"]
        dict_data["Wts(%)"] = round(float(dt["weight"]),2) if dt["weight"] else 0
        data_list.append(dict_data)

        index += 1
        if index == 10:
            break
    
    template_vars["issuer_data_html"] = get_table_from_html_template(data_list, column_list, 33)

    #Market Cap chart
    data_label = []

    for dt in data["market_cap"]:
        market_cap_dict = dict()
        market_cap_dict = {
            'title':dt['market_cap'],
            'values':round(float(dt["weight"]),2) if dt["weight"] else 0
        }
        data_label.append(market_cap_dict)

    template_vars["market_cap_chart_html"] = get_pie_chart(data_label, '', False, 7, True, ('#228FCF', '#364EB9', '#36B9A5'), False, False, 300, 900)

    #Equity Style chart
    data_label = []

    for dt in data["equity_style"]:
        equity_style_dict = dict()
        equity_style_dict = {
            'title':dt['equity_style'],
            'values':round(float(dt["weight"]),2) if dt["weight"] else 0
        }
        data_label.append(equity_style_dict)

    template_vars["equity_style_chart_html"] = get_pie_chart(data_label, '', False, 7, True, ('#228FCF', '#364EB9', '#36B9A5'), False, False, 300, 900)

    portfolio_history = data['portfolio_history']
    
    #sector weight - historical
    template_vars["trend_analysis_sectorweights_historical_data_html"] = get_trand_analysis_stacked_chart(portfolio_history['sectors'],'sector',12, False, 'weight', False)

    #issuer - historical
    template_vars["trend_analysis_issuer_historical_data_html"] = get_trand_analysis_stacked_chart(portfolio_history['issuers'],'issuer',12, False, 'weight', False)

    #instrument type - historical
    template_vars["trend_analysis_instrumenttype_historical_data_html"] = get_trand_analysis_stacked_chart(portfolio_history['instrument_types'],'instrument_type', 12, False, 'weight', False)

    template_vars["trend_analysis_aum_data_html"] = get_line_chart(portfolio_history['aum'],fontsize=8,in_miliseconds= False,add_range=True,height=400, width=1100, show_legend=False, show_label=True )

    template_vars["geo_allocation_data_html"] = get_geo_location_chart(data['geo_allocation'])
    

    return template_vars    


def get_request_for_table(req_data, name_key, value_key):
    data_list = list()
    counter = 0
    for dt in req_data:
        dict_data = dict()
        dict_data["Name"] = dt[name_key]
        dict_data["Wts(%)"] = round(float(dt[value_key]),2) if dt[value_key] else 'NA'
        data_list.append(dict_data)
        counter += 1

        if counter==10:
            break
    
    return data_list
    # template_vars["issuer_data_html"] = get_table_from_html_template(data_list, column_list, 33)

def get_fundoverlappdf(db_session, organization_id, image_path, whitelabel_dir, generatereport_dir, list_plans):
    template_vars = dict()
    
    organization_whitelabel_data = get_organization_whitelabel(db_session, organization_id)

    if organization_whitelabel_data:
        template_vars["organization_logo_path"] =F"{image_path}/{whitelabel_dir}/{organization_whitelabel_data['logo']}"
        template_vars["disclaimer_text"] = organization_whitelabel_data['disclaimer']
        
    template_vars["overlap"] = get_plan_overlap(db_session, list_plans)
    template_vars["details"] = get_plan_meta_info(db_session, list_plans)
    trailing_return, riskanalysis = get_trailing_return_and_riskanalysis(db_session, list_plans)
    template_vars["trailing_returns"] = trailing_return
    template_vars["risk_analysis"] = riskanalysis

    template_vars = beautify_fund_overlap_data(template_vars, image_path)

    file_name = prepare_pdf_from_html(template_vars, 'fundoverlaphtml_template.html', generatereport_dir)

    return file_name


def beautify_fund_overlap_data(data, image_path):
    plan_a = None
    plan_b = None
    if data:
        #logo
        for details in data["details"]:
            if not plan_a:
                plan_a = details
            else:
                plan_b = details

        dt = data["details"][plan_a]
        
        data["fund_a_logo"] = F"{image_path}/{dt['amc_logo']}"
        data["fund_a_product_name"] = dt["product_name"]
        data["fund_a_plan_name"] = dt["plan_name"]

        dt = data["details"][plan_b]
        data["fund_b_logo"] = F"{image_path}/{dt['amc_logo']}"
        data["fund_b_product_name"] = dt["product_name"]
        data["fund_b_plan_name"] = dt["plan_name"]

        dt = data["overlap"][plan_a][plan_b]


        data["common_stocks"] = dt['securities_info']['common_securities']   
        data["fund_a_total_securities"] = dt["securities_info"]['left_total_securities']
        data["fund_a_uncommon_securities"] = dt["securities_info"]['left_total_securities'] - dt['securities_info']['common_securities']
        data["fund_b_total_securities"] = dt["securities_info"]['right_total_securities']
        data["fund_b_uncommon_securities"] = dt["securities_info"]['right_total_securities'] - dt['securities_info']['common_securities']

        data["fund_a_common_weight"] = int(dt["securities_info"]['left_common_weight']) if dt["securities_info"]['left_common_weight'] else 0
        data["fund_a_unique_weight"] = int(dt["securities_info"]['left_unique_weight']) if dt["securities_info"]['left_unique_weight'] else 0
        data["fund_b_common_weight"] = int(dt["securities_info"]['right_common_weight']) if dt["securities_info"]['right_total_securities'] else 0
        data["fund_b_unique_weight"] = int(dt["securities_info"]['right_unique_weight']) if dt["securities_info"]['right_unique_weight'] else 0

        data["fund_a_returns"] = data["trailing_returns"][0]["returns"]
        data["fund_b_returns"] = data["trailing_returns"][1]["returns"]

        data["fund_a_riskanalysis"] = data["risk_analysis"][0]["analysis"]
        data["fund_b_riskanalysis"] = data["risk_analysis"][1]["analysis"]

        data["fund_a_riskanalysis"]["avgmktcap_rs_cr"] = round(data["fund_a_riskanalysis"]["avgmktcap_rs_cr"],2) if data["fund_a_riskanalysis"]["avgmktcap_rs_cr"] else None
        data["fund_b_riskanalysis"]["avgmktcap_rs_cr"] = round(data["fund_b_riskanalysis"]["avgmktcap_rs_cr"],2) if data["fund_b_riskanalysis"]["avgmktcap_rs_cr"] else None

        data["fund_a_riskanalysis"]["pe_ratio"] = round(data["fund_a_riskanalysis"]["pe_ratio"],2) if data["fund_a_riskanalysis"]["pe_ratio"] else None
        data["fund_b_riskanalysis"]["pe_ratio"] = round(data["fund_b_riskanalysis"]["pe_ratio"],2) if data["fund_b_riskanalysis"]["pe_ratio"] else None
        
        overlap = data["overlap"][plan_a][plan_b]['securities_overlap']

        x_labels = list()
        fund_a = []
        fund_b = []
        data_label = []
        for overlap_data in overlap:
            
            x_labels.append(overlap_data["name"])
            fund_a.append(overlap_data["weight_a"])
            fund_b.append(overlap_data["weight_b"])

        a_dict = {
            'title':"",
            'values':fund_a
        }
        
        b_dict = {
            'title':"",
            'values':fund_b
        }
        data_label.append(a_dict)
        data_label.append(b_dict)

        overlap_len = len(overlap)
        height = 450
        if overlap_len < 3:
            height = 100
        elif overlap_len < 5:
            height = 300
        elif overlap_len < 8:
            height = 450
        elif overlap_len < 12:
            height = 600
        else:
            height = 800

        data["security_overlap_html"] = get_barchart(x_labels,data_label, False, "", 9, height, True, False, False)


        #sector overlap        
        overlap = data["overlap"][plan_a][plan_b]['sector_overlap']

        x_labels = list()
        fund_a = []
        fund_b = []
        data_label = []
        for overlap_data in overlap:
            
            x_labels.append(overlap_data["name"])
            fund_a.append(overlap_data["weight_a"])
            fund_b.append(overlap_data["weight_b"])

        a_dict = {
            'title':"",
            'values':fund_a
        }
        
        b_dict = {
            'title':"",
            'values':fund_b
        }
        data_label.append(a_dict)
        data_label.append(b_dict)

        overlap_len = len(overlap)
        height = 450
        if overlap_len < 3:
            height = 100
        elif overlap_len < 5:
            height = 300
        elif overlap_len < 8:
            height = 450
        elif overlap_len < 12:
            height = 600
        else:
            height = 800
        
        data["sector_overlap_html"] = get_barchart(x_labels,data_label, False, "", 9, height, True, False, False)

    return data




def beautify_fund_compare_data(data, image_path):
    if data["asondate"]:
        data["asondate"] = datetime.strftime(data["asondate"], '%d %b %Y') 
    
    if data["inception_date"]:
        data["inception_date"] = datetime.strftime(data["inception_date"], '%d %b %Y') 
    
    data["expense_ratio"] = round(data["expense_ratio"],2) if data["expense_ratio"] else 'Not Available'
    data["nav"] = round(data["nav"],2) if data["nav"] else 'NA'
    data["aum"] = round(data["aum"],2) if data["aum"] else 'NA'
    data["classification"] = data["classification"] if data["classification"] else 'NA'
    data["benchmark_name"] = data["benchmark_name"] if data["benchmark_name"] else 'NA'
    data["min_investment"] = data["min_investment"] if data["min_investment"] else 'NA'
    data["fee_structure"] = data["fee_structure"] if data["fee_structure"] else 'NA'
    data["exit_load"] = data["exit_load"] if data["exit_load"] else 'NA'

    

    if data["min_investment"]:
        data["min_investment"] = round(data["min_investment"],0) if data["min_investment"] != 'NA' else data["min_investment"]

    if data["portfolio_characteristics"]:
        
        data["portfolio_characteristics"]["total_stocks"] = round(data["portfolio_characteristics"]["total_stocks"],0) if data["portfolio_characteristics"]["total_stocks"] else 'NA'
        
        data["portfolio_characteristics"]["avgmktcap_rs_cr"] = round(data["portfolio_characteristics"]["avgmktcap_rs_cr"],2) if data["portfolio_characteristics"]["avgmktcap_rs_cr"] else 'NA'
    
        data["portfolio_characteristics"]["portfoliop_eratio"] = round(data["portfolio_characteristics"]["portfoliop_eratio"],2) if data["portfolio_characteristics"]["portfoliop_eratio"] else 'NA'

    if data["marketcap_composition"]:        
        data["marketcap_composition"]["large_cap"] = round(data["marketcap_composition"]["large_cap"],2) if data["marketcap_composition"]["large_cap"] else 'NA'

        data["marketcap_composition"]["mid_cap"] = round(data["marketcap_composition"]["mid_cap"],2) if data["marketcap_composition"]["mid_cap"] else 'NA'

        data["marketcap_composition"]["small_cap"] = round(data["marketcap_composition"]["small_cap"],2) if data["marketcap_composition"]["small_cap"] else 'NA'

        data["marketcap_composition"]["unlisted"] = round(data["marketcap_composition"]["unlisted"],2) if data["marketcap_composition"]["unlisted"] else 'NA'
    else:
        mydict = dict()
        mydict["large_cap"] = 'NA'
        mydict["mid_cap"] = 'NA'
        mydict["small_cap"] = 'NA'
        mydict["unlisted"] = 'NA'
        data["marketcap_composition"] = mydict


    if data["risk_grade"] == 'Low':
        data["risk_grade_image"] = F"{image_path}/Images/low.png"
    elif data["risk_grade"] == 'Moderately Low':
        data["risk_grade_image"] = F"{image_path}/Images/BelowAverage.png"
    elif data["risk_grade"] == 'Moderate':
        data["risk_grade_image"] = F"{image_path}/Images/Average.png"
    elif data["risk_grade"] == 'Moderately High':
        data["risk_grade_image"] = F"{image_path}/Images/AboveAverage.png"
    elif data["risk_grade"] == 'High':
        data["risk_grade_image"] = F"{image_path}/Images/rsikometer.png"
    elif data["risk_grade"] == 'Very High':
        data["risk_grade_image"] = F"{image_path}/Images/very_high.png"
    else:
        data["risk_grade"] = "NA"

    
    if data["composition"]:
        data_label = []

        rollingret_dict = dict()
        rollingret_dict = {
            'title':"Equity",
            'values':data["composition"]["equity"]
        }
        data_label.append(rollingret_dict)

        rollingret_dict = dict()
        rollingret_dict = {
            'title':"Debt",
            'values':data["composition"]["debt"]
        }
        data_label.append(rollingret_dict)

        rollingret_dict = dict()
        rollingret_dict = {
            'title':"Cash",
            'values':data["composition"]["cash"]
        }
        data_label.append(rollingret_dict)

        data["composition_chart_src"] = get_pie_chart(data_label, "Composition", False, 9, True, ('#228FCF', '#364EB9', '#36B9A5'), False, False, 200, 600)

        data["composition"]["equity"] = round(data["composition"]["equity"],2) if data["composition"]["equity"] else 'NA'
        data["composition"]["debt"] = round(data["composition"]["debt"],2) if data["composition"]["debt"] else 'NA'
        data["composition"]["cash"] = round(data["composition"]["cash"],2) if data["composition"]["cash"] else 'NA'
   
    
    return data