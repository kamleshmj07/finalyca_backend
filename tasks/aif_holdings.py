import csv
from datetime import datetime as dt
from datetime import timedelta
from operator import index, or_
from sqlalchemy import desc, and_, func
import os
from bizlogic.importer_helper import save_planproductmapping, get_rel_path, write_csv

from utils import *
from utils.finalyca_store import *
from fin_models.transaction_models import *
from fin_models.masters_models import *
from fin_models.logics_models import *

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month
    
def import_aif_holdings_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))
    engine = get_unsafe_db_engine(config)

    header = [ "FundID", "ISIN Code", "Portfolio Date", "Company/Security Name", "Security/Asset Class", "Percentage to AUM", "STOCK_STOCKDERIVATIVE_INDEXDERIVATIVE", "INDEX_CODE", "LONG_SHORT", "LISTED_UNLISTED", "CIN", "Location_City", "Amount_Invested_Crs", "Asset Type Allocation", "Investment Structure", "Total_Receipts_Crs", "Exit Date", "Exit IRR", "Exit Multiple", "Project Stage", "Instrument", "Instrument Rating", "Sector","Secured_Unsecured", "Remarks" ]
    items = list()
    TODAY = dt.today()
    PRODUCT_ID = 5

    fund_dict = dict()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)
        
        for row in csvreader:
            fund_code = row["FundID"]
            isin_code = row["ISIN Code"] if row["ISIN Code"] != 'NA' else None
            pf_date = row["Portfolio Date"] if row["Portfolio Date"] != 'NA' else None
            portfolio_date = dt.strptime(pf_date, '%d-%m-%Y')
            sec_name = row["Company/Security Name"] if row["Company/Security Name"] != 'NA' else None
            sec_class = row["Security/Asset Class"] if row["Security/Asset Class"] != 'NA' else None
            per_aum = row["Percentage to AUM"] if row["Percentage to AUM"] != 'NA' else None
            stock_stockderivative_indexderivative = row["STOCK_STOCKDERIVATIVE_INDEXDERIVATIVE"] if row["STOCK_STOCKDERIVATIVE_INDEXDERIVATIVE"] != 'NA' else None
            index_code = row["INDEX_CODE"] if row["INDEX_CODE"] != 'NA' else None
            long_short = row["LONG_SHORT"] if row["LONG_SHORT"] != 'NA' else None
            listed_unlisted = row["LISTED_UNLISTED"] if row["LISTED_UNLISTED"] != 'NA' else None
            cin = row["CIN"] if row["CIN"] != 'NA' else None
            location_city = row["Location_City"] if row["Location_City"] != 'NA' else None
            amount_invested_crs = row["Amount_Invested_Crs"] if row["Amount_Invested_Crs"] != 'NA' else None
            asset_type_allocation = row["Asset Type Allocation"] if row["Asset Type Allocation"] != 'NA' else None
            investment_structure = row["Investment Structure"] if row["Investment Structure"] != 'NA' else None
            total_rec_crs = row["Total_Receipts_Crs"] if row["Total_Receipts_Crs"] != 'NA' else None
            exit_date = row["Exit Date"] if row["Exit Date"] != 'NA' else None
            exit_irr = row["Exit IRR"] if row["Exit IRR"] != 'NA' else None
            exit_multiple = row["Exit Multiple"] if row["Exit Multiple"] != 'NA' else None
            project_stage = row["Project Stage"] if row["Project Stage"] != 'NA' else None
            instrument = row["Instrument"] if row["Instrument"] != 'NA' else None
            instrument_rating = row["Instrument Rating"] if row["Instrument Rating"] != 'NA' else None
            sector_name = row["Sector"] if row["Sector"] != 'NA' else None
            secured_unsecured = row["Secured_Unsecured"] if row["Secured_Unsecured"] != 'NA' else None

            remark = None

            if fund_code not in fund_dict:
                fund_dict[fund_code] = list()

            fund_dict[fund_code].append(portfolio_date)

            sql_fund = db_session.query(Fund).filter(Fund.Fund_Code == fund_code).filter(Fund.Is_Deleted != 1).first()
            if not sql_fund:
                remark = "Fund details are not available in the system"

            if portfolio_date > TODAY:
                remark = "Underlying Holding cannot be uploaded for Future date."

            if sql_fund:
                if sql_fund.AIF_CATEGORY == 3:
                    if (stock_stockderivative_indexderivative== None or stock_stockderivative_indexderivative == '') and remark == None:
                        remark = "STOCK_STOCKDERIVATIVE_INDEXDERIVATIVE Cannot be blank."
                    
                    if (long_short== None or long_short=='') and remark == None:
                        remark = "LONG_SHORT Cannot be blank."

                
                if portfolio_date== None and remark == None:
                    remark = "Portfolio Date Cannot be blank."
                
                if (sec_name== None or sec_name=='') and remark == None:
                    remark = "Company/Security Name Cannot be blank."
                
                if (sql_fund.AIF_CATEGORY == 2 and sql_fund.AIF_SUB_CATEGORY =='EQUITY') or sql_fund.AIF_CATEGORY == 3:
                    if (sec_class== None or sec_class=='') and remark == None:
                        remark = "Security/Asset Class Cannot be blank."
                    
                    if (perc_aum== None or perc_aum=='') and remark == None:
                        remark = "Percentage to AUM Cannot be blank."
                
                if (listed_unlisted== None or listed_unlisted=='') and remark == None:
                    remark = "LISTED_UNLISTED Cannot be blank."

                if (sql_fund.AIF_CATEGORY == 2 and sql_fund.AIF_SUB_CATEGORY =='REAL ESTATE'):
                    if (location_city== None or location_city=='') and remark == None:
                        remark = "Location_City Cannot be blank."

                    if (amount_invested_crs== None or amount_invested_crs=='' or amount_invested_crs==0) and remark == None:
                        remark = "Amount_Invested_Crs Cannot be blank."

                    if (asset_type_allocation== None or asset_type_allocation=='') and remark == None:
                        remark = "Asset Type Allocation Cannot be blank."
                    
                    if (investment_structure== None or investment_structure=='') and remark == None:
                        remark = "Investment Structure Cannot be blank."

                    if (project_stage== None or project_stage=='') and remark == None:
                        remark = "Project Stage Cannot be blank."
                

            if remark == None:
                # Update old holdings set deleted flag to 1.
                update_values = {
                    UnderlyingHoldings.Is_Deleted : 1,
                    UnderlyingHoldings.Updated_By : user_id,
                    UnderlyingHoldings.Updated_Date : TODAY,
                }
                
                sql_h = db_session.query(UnderlyingHoldings).filter(and_(UnderlyingHoldings.Fund_Id == sql_fund.Fund_Id, UnderlyingHoldings.Portfolio_Date==portfolio_date, UnderlyingHoldings.ISIN_Code == isin_code, UnderlyingHoldings.Company_Security_Name==sec_name)).update(update_values)
                
                db_session.commit()
                remark = 'Flagged Deleted and uploaded new records Successfully.'

                holding_sec_id = db_session.query(func.max(HoldingSecurity.HoldingSecurity_Id)).filter(HoldingSecurity.ISIN_Code == isin_code).filter(HoldingSecurity.Is_Deleted != 1).filter(HoldingSecurity.active != 0).scalar()

                security_data = None
                if holding_sec_id:
                    security_data = db_session.query(Sector.Sector_Code, Sector.Sector_Name).select_from(HoldingSecurity).join(Sector, HoldingSecurity.Sector_Id == Sector.Sector_Id).filter(HoldingSecurity.HoldingSecurity_Id == holding_sec_id).filter(Sector.Is_Deleted != 1).one_or_none()
                
                ul = UnderlyingHoldings()
                ul.Fund_Id = sql_fund.Fund_Id
                ul.Fund_Code = sql_fund.Fund_Code
                ul.ISIN_Code = isin_code if isin_code != 'NA' else None
                ul.Portfolio_Date = portfolio_date
                ul.Company_Security_Name = sec_name
                ul.Asset_Class = sec_class if sec_class != 'NA' else None
                ul.Percentage_to_AUM = per_aum
                ul.Is_Deleted = 0
                ul.Created_By = user_id
                ul.Created_Date = TODAY
                ul.Is_Deleted = 0
                ul.HoldingSecurity_Id = holding_sec_id
                ul.STOCK_STOCKDERIVATIVE_INDEXDERIVATIVE = stock_stockderivative_indexderivative if stock_stockderivative_indexderivative != 'NA' else None
                ul.INDEX_CODE = index_code if index_code != 'NA' else None
                ul.LONG_SHORT = long_short
                ul.LISTED_UNLISTED = listed_unlisted if listed_unlisted != 'NA' else None
                ul.CIN = cin if cin else None
                ul.Location_City = location_city if location_city != 'NA' else None
                ul.Amount_Invested_Crs = amount_invested_crs if amount_invested_crs != 'NA' else None
                ul.Asset_Type_Allocation = asset_type_allocation if asset_type_allocation != 'NA' else None
                ul.Investment_Structure = investment_structure if investment_structure != 'NA' else None
                ul.Total_Receipts_Crs = total_rec_crs if total_rec_crs != 'NA' else None
                ul.Exit_Date = exit_date if exit_date != 'NA' else None
                ul.Exit_IRR = exit_irr if exit_irr != 'NA' else None
                ul.Exit_Multiple = exit_multiple if exit_multiple != 'NA' else None
                ul.Project_Stage = project_stage if project_stage != 'NA' else None
                if sector_name != 'NA':
                    ul.Sector_Names = sector_name if sector_name != 'NA' else None

                if security_data:
                    ul.Sector_Code = security_data.Sector_Code
                    ul.Sector_Names = security_data.Sector_Name
                ul.Instrument = instrument if instrument != 'NA' else None
                ul.Instrument_Rating = instrument_rating if instrument_rating != 'NA' else None
                ul.secured_unsecured = secured_unsecured if secured_unsecured != 'NA' else None

                db_session.add(ul)
                db_session.commit()
                remark = 'Uploaded Successfully'

                sql_allholdings = db_session.query(UnderlyingHoldings.Purchase_Date, UnderlyingHoldings.Underlying_Holdings_Id, UnderlyingHoldings.Portfolio_Date).filter(UnderlyingHoldings.Fund_Id == sql_fund.Fund_Id, UnderlyingHoldings.ISIN_Code == isin_code, UnderlyingHoldings.Is_Deleted != 1).order_by(desc('Portfolio_Date')).all()

                min_purchasedate = None
                list_holdings = []
                for allholding in sql_allholdings:
                    if min_purchasedate == None:
                        min_purchasedate = allholding.Portfolio_Date
                        list_holdings.append(allholding.Underlying_Holdings_Id)

                    else:
                        diff = diff_month(min_purchasedate, allholding.Portfolio_Date)
                        if diff > 1:
                            break
                        else:
                            if allholding.Purchase_Date == None:
                                min_purchasedate = allholding.Portfolio_Date
                                list_holdings.append(allholding.Underlying_Holdings_Id)
                            else:
                                min_purchasedate = allholding.Purchase_Date
                                break
                for Underlying_Holdings_Id in list_holdings:
                    update_values = {
                        UnderlyingHoldings.Purchase_Date : min_purchasedate,
                        UnderlyingHoldings.Updated_By : user_id,
                        UnderlyingHoldings.Updated_Date : TODAY,
                    }

                    sql_update = db_session.query(UnderlyingHoldings).filter(UnderlyingHoldings.Underlying_Holdings_Id == Underlying_Holdings_Id).update(update_values)
                    db_session.commit()

            item = row.copy()
            item["Remarks"] = remark
            items.append(item)    

        
    write_csv(output_file_path, header, items, __file__)

    # Trigger Analysis
    
    for fund_code, dates in fund_dict.items():
        for portfolio_date in set(dates):   
            
            AsOnDate_UH = db_session.query(func.count(Plans.Plan_Code).label('count_1'), Plans.Plan_Id).join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id).join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id).join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id).join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id).join(Product, Product.Product_Id == PlanProductMapping.Product_Id).filter(Product.Product_Id == PRODUCT_ID).filter(UnderlyingHoldings.Portfolio_Date == portfolio_date).filter(Fund.Fund_Code == fund_code).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(UnderlyingHoldings.Is_Deleted != 1).group_by(UnderlyingHoldings.Portfolio_Date, Plans.Plan_Id).all()

            for i in AsOnDate_UH:
                i_planid = i.Plan_Id
                i_numberofstocks = i[0]                

                update_values = {
                        FactSheet.TotalStocks : i_numberofstocks,
                        FactSheet.Updated_By : user_id,
                        FactSheet.Updated_Date : TODAY,
                    }
                sql_h = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id == i_planid, FactSheet.TransactionDate == portfolio_date, FactSheet.Is_Deleted != 1)).update(update_values)
                db_session.commit()
                
                date_1 = portfolio_date - timedelta(1)
                date_2 = portfolio_date - timedelta(2)
                
                Temp_PC = db_session.query(Plans.Plan_Id,Plans.Plan_Code, Plans.Plan_Name,Fund.Fund_Id,Fund.Fund_Code, Fund.Fund_Name,UnderlyingHoldings.Portfolio_Date, UnderlyingHoldings.ISIN_Code, UnderlyingHoldings.HoldingSecurity_Id,HoldingSecurity.HoldingSecurity_Name,HoldingSecurity.Co_Code, UnderlyingHoldings.Percentage_to_AUM, Fundamentals.DivYield, Fundamentals.EPS, Fundamentals.PE, Fundamentals.PBV, Fundamentals.mcap).join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id).join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id).join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id).join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id).join(Product, Product.Product_Id == PlanProductMapping.Product_Id).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id , isouter=True).join(Fundamentals, Fundamentals.CO_CODE == HoldingSecurity.Co_Code).filter(or_(or_(Fundamentals.PriceDate == portfolio_date, Fundamentals.PriceDate == date_1), Fundamentals.PriceDate == date_2)).filter(Product.Product_Id == PRODUCT_ID).filter(UnderlyingHoldings.Portfolio_Date == portfolio_date).filter(Plans.Plan_Id == i_planid).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(UnderlyingHoldings.Is_Deleted != 1).group_by(UnderlyingHoldings.Portfolio_Date, Plans.Plan_Id,Plans.Plan_Code, Plans.Plan_Name, Fund.Fund_Id, Fund.Fund_Code, Fund.Fund_Name, UnderlyingHoldings.ISIN_Code, UnderlyingHoldings.HoldingSecurity_Id, HoldingSecurity.HoldingSecurity_Name, HoldingSecurity.Co_Code, UnderlyingHoldings.Percentage_to_AUM, Fundamentals.DivYield, Fundamentals.EPS, Fundamentals.PE, Fundamentals.mcap, Fundamentals.PBV).all()
                
                Summary_PC = db_session.query(Plans.Plan_Id,Plans.Plan_Code, Plans.Plan_Name,UnderlyingHoldings.Portfolio_Date, func.sum(UnderlyingHoldings.Percentage_to_AUM).label('Percentage_to_AUM')).join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id).join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id).join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id).join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id).join(Product, Product.Product_Id == PlanProductMapping.Product_Id).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id , isouter=True).join(Fundamentals, Fundamentals.CO_CODE == HoldingSecurity.Co_Code).filter(or_(or_(Fundamentals.PriceDate == portfolio_date, Fundamentals.PriceDate == date_1), Fundamentals.PriceDate == date_2)).filter(UnderlyingHoldings.Portfolio_Date == portfolio_date).filter(Plans.Plan_Id == i_planid).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(UnderlyingHoldings.Is_Deleted != 1).group_by(UnderlyingHoldings.Portfolio_Date, Plans.Plan_Id,Plans.Plan_Code, Plans.Plan_Name).all()
                
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
                    json_obj["HoldingSecurity_Name"] = sql_obj['HoldingSecurity_Name']
                    json_obj["Co_Code"] = sql_obj['Co_Code']
                    json_obj["Percentage_to_AUM"] = sql_obj['Percentage_to_AUM']
                    json_obj["HoldingSecurity_Id"] = sql_obj['HoldingSecurity_Id']
                    json_obj["PE"] = sql_obj['PE']
                    json_obj["PBV"] = sql_obj['PBV']
                    json_obj["mcap"] = sql_obj['mcap']
                    json_obj["DivYield"] = sql_obj['DivYield']
                    json_obj["PE_Ratio"] = 0.0
                    json_obj["PB_Ratio"] = 0.0
                    json_obj["DY_Ratio"] = 0.0                  
                    Temp_PC_items.append(json_obj)

                    sum_Percentage_to_AUM += sql_obj['Percentage_to_AUM']

                for T in Temp_PC_items:
                    if T['PE'] != 0:
                        
                        T["PE_Ratio"] = ((((float(T['Percentage_to_AUM']) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 / float(T['PE'])))                      

                    if T['PBV'] != 0:
                        
                        T["PB_Ratio"] = (((((float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 /  float(T['PBV'])))
                        

                    if T['DivYield'] != 0:
                        
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
                    if sum_DY_Ratio!= 0:
                        sum_DY_Ratio = (1/sum_DY_Ratio) 

                    update_values = {
                    FactSheet.PortfolioP_BRatio : sum_PB_Ratio,
                    FactSheet.PortfolioP_ERatio : sum_PE_Ratio,
                    FactSheet.AvgMktCap_Rs_Cr : avg_mcap,
                    FactSheet.Portfolio_Dividend_Yield : sum_DY_Ratio,
                    FactSheet.Updated_By : user_id,
                    FactSheet.Updated_Date : TODAY,
                    }

                    sql_h = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id==T['Plan_Id'], FactSheet.TransactionDate == portfolio_date) , FactSheet.Is_Deleted != 1).update(update_values)
                    db_session.commit()

                    sql_factsheetdata = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id==T['Plan_Id'], FactSheet.TransactionDate == portfolio_date) , FactSheet.Is_Deleted != 1).one_or_none()

                    sql_allholdings = db_session.query(UnderlyingHoldings).join(Fund, Fund.Fund_Id == UnderlyingHoldings.Fund_Id).join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id).join(Product, Product.Product_Id == PlanProductMapping.Product_Id).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id , isouter=True).filter(Product.Product_Id == PRODUCT_ID).filter(UnderlyingHoldings.Portfolio_Date == portfolio_date).filter(Plans.Plan_Id == i_planid).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(UnderlyingHoldings.Is_Deleted != 1).all()

                    if sql_factsheetdata:
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
    

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/holding_to_upload.csv"
    READ_PATH = "read/holding_to_upload.csv"
    import_aif_holdings_file(FILE_PATH, READ_PATH, USER_ID)



                








                


            
            


