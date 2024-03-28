import csv
from datetime import datetime as dt
from datetime import timedelta
from operator import or_
from sqlalchemy import desc, and_, func
import os
from bizlogic.importer_helper import get_rel_path, write_csv

from utils import *
from utils.finalyca_store import *
from fin_models.transaction_models import *
from fin_models.masters_models import *
from fin_models.logics_models import *

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def import_holdings_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))
    # engine = get_unsafe_db_engine(config)

    header = ["Fund_Id", "Portfolio_Date", "ISIN Code", "Security Name", "Security/Asset Class",
              "Holding %", "Sector", "Listed/Unlisted", "Credit Rating", "Derivative", "Remarks"]

    # TODO: This needs to be shifted to a master table
    security_asset_class_lst = ["NOT DEFINED","LISTED EQUITY","UNLISTED EQUITY","ESOPS","DEBT","OTHER DEBTS",
                                "CASH AND EQUIVALENTS","COMMODITY","INVIT/REITS","MUTUAL FUNDS","OTHER FUNDS","REAL ESTATE"]

    items = list()
    TODAY = dt.today()
    PRODUCT_ID = None

    fund_dict = dict()

    with open(get_rel_path(input_file_path, __file__), 'r') as f:
        csvreader = csv.DictReader(f)

        for row in csvreader:
            # del row['']
            fund_code = row["Fund_Id"]
            portfolio_date = dt.strptime(row["Portfolio_Date"], '%d-%m-%Y')
            isin_code = row["ISIN Code"]
            sec_name = row["Security Name"]
            sec_class = row["Security/Asset Class"].strip().upper()
            perc_to_aum = row["Holding %"].strip().replace("%", "") if row["Holding %"] != 'NA' else 0
            sector = row["Sector"]
            listed_unlisted = "Listed" if row["Listed/Unlisted"] == "" else row["Listed/Unlisted"]
            credit_rating = row["Credit Rating"] if row["Credit Rating"] != "" else None
            long_short = row["Derivative"] if row["Derivative"] != "" else None
            remark = None
            sector_remark = ''

            # ------------------ Validations
            if portfolio_date==None:
                raise Exception("Portfolio Date cannot be blank/empty.")

            if (sec_name==None or sec_name=='') and remark == None:
                raise Exception("Security Name cannot be blank/empty.")

            if (sec_class==None or sec_class=='' or sec_class not in security_asset_class_lst)and remark == None:
                raise Exception(f"Security/Asset Class cannot be blank/empty. The value can be only >> {security_asset_class_lst}.")

            if (perc_to_aum== None or perc_to_aum=='') and remark == None:
                raise Exception("Percentage to AUM cannot be blank/empty.")

            if sec_class=='LISTED EQUITY' and isin_code == '':
                raise Exception("ISIN code is missing for a Listed Equity.")

            if fund_code not in fund_dict:
                fund_dict[fund_code] = list()

            fund_dict[fund_code].append(portfolio_date)

            sql_fund = db_session.query(Fund)\
                                 .filter(Fund.Fund_Code == fund_code, Fund.Is_Deleted != 1).first()

            if not sql_fund:
                remark = "Fund details are not available in the system."

            if portfolio_date > TODAY:
                remark = "Underlying holding cannot be uploaded for future date."

            # get the product id for the respective fund
            fund_id = sql_fund.Fund_Id if sql_fund else None
            PRODUCT_ID = db_session.query(PlanProductMapping.Product_Id)\
	                               .join(Plans, Plans.Plan_Id == PlanProductMapping.Plan_Id)\
	                               .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
	                               .filter(MFSecurity.Fund_Id == fund_id, PlanProductMapping.Is_Deleted != 1,
                                        Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1).scalar()

            if remark == None:
                # Update old holdings to deleted.
                update_values = {
                    UnderlyingHoldings.Is_Deleted : 1,
                    UnderlyingHoldings.Updated_By : user_id,
                    UnderlyingHoldings.Updated_Date : TODAY,
                }

                sql_h = db_session.query(UnderlyingHoldings)\
                                  .filter(and_(UnderlyingHoldings.Fund_Id == sql_fund.Fund_Id,
                                               UnderlyingHoldings.Portfolio_Date == portfolio_date,
                                               UnderlyingHoldings.ISIN_Code == isin_code)).update(update_values)

                db_session.commit()
                remark = 'Flagged as deleted for previous records with the respective portfolio date.'

                holding_sec_id = db_session.query(func.max(HoldingSecurity.HoldingSecurity_Id))\
                                           .filter(HoldingSecurity.ISIN_Code == isin_code, HoldingSecurity.Is_Deleted != 1, HoldingSecurity.active != 0).scalar()
                sector_info = None
                if holding_sec_id:
                    sector_info = db_session.query(Sector.Sector_Id,
                                                   Sector.Sector_Code,
                                                   Sector.Sector_Name)\
                                            .select_from(Sector)\
                                            .join(HoldingSecurity, HoldingSecurity.Sector_Id == Sector.Sector_Id)\
                                            .filter(HoldingSecurity.HoldingSecurity_Id == holding_sec_id, Sector.Is_Deleted != 1).one_or_none()
                else:
                    sector_info = db_session.query(Sector.Sector_Id,
                                                   Sector.Sector_Code,
                                                   Sector.Sector_Name)\
                                            .filter(Sector.Sector_Name == sector, Sector.Is_Deleted != 1).one_or_none()

                    # if there is no sector reported, then bucketing the holding security in 'Others'
                    # this if identified then we have to fix this manually for now till we find a better process to handle this
                    if (sector == '' or sector == None) and sec_class != 'CASH AND EQUIVALENTS':
                        sector_info = Sector()
                        sector_info.Sector_Id = 24
                        sector_info.Sector_Code = 'Others_01'
                        sector_info.Sector_Name = 'Others'
                        sector_remark = 'No Sector is reported, please confirm the Sector for this security with the AMC.'

                    if sec_class != 'CASH AND EQUIVALENTS':
                        # create a new holdings security if the holding security does not exist
                        holding_security_obj = HoldingSecurity()
                        holding_security_obj.HoldingSecurity_Name = sec_name
                        holding_security_obj.ISIN_Code = isin_code if isin_code != "" else None
                        holding_security_obj.HoldingSecurity_Type = sec_class
                        holding_security_obj.Sector_Id = sector_info.Sector_Id
                        # holding_security_obj.Credit_Rating = credit_rating
                        holding_security_obj.Is_Deleted = 0
                        holding_security_obj.active = 1
                        holding_security_obj.Created_By = user_id
                        holding_security_obj.Created_Date = TODAY
                        db_session.add(holding_security_obj)

                        isin_code = isin_code if isin_code != "" else None
                        holding_sec_id = db_session.query(func.max(HoldingSecurity.HoldingSecurity_Id))\
                                                   .filter(HoldingSecurity.ISIN_Code == isin_code,
                                                           HoldingSecurity.HoldingSecurity_Name == sec_name,
                                                           HoldingSecurity.Is_Deleted != 1, HoldingSecurity.active != 0).scalar()


                # add underlying holdings for the respective fund
                ul = UnderlyingHoldings()
                ul.Fund_Id = sql_fund.Fund_Id
                ul.Fund_Code = sql_fund.Fund_Code
                ul.ISIN_Code = isin_code
                ul.Portfolio_Date = portfolio_date
                ul.Company_Security_Name = sec_name
                ul.Percentage_to_AUM = perc_to_aum
                ul.Value_in_INR = 0
                ul.Instrument_Rating = credit_rating
                ul.LISTED_UNLISTED = listed_unlisted
                ul.LONG_SHORT = long_short
                ul.Is_Deleted = 0
                ul.Created_By = user_id
                ul.Created_Date = TODAY
                ul.Is_Deleted = 0
                ul.HoldingSecurity_Id = holding_sec_id
                if sector_info:
                    ul.Sector_Code = sector_info.Sector_Code
                    ul.Sector_Names = sector_info.Sector_Name

                db_session.add(ul)
                db_session.commit()
                remark = 'Underlying holdings uploaded successfully.'

                sql_allholdings = db_session.query(UnderlyingHoldings.Purchase_Date,
                                                   UnderlyingHoldings.Underlying_Holdings_Id,
                                                   UnderlyingHoldings.Portfolio_Date)\
                                            .filter(UnderlyingHoldings.Fund_Id == sql_fund.Fund_Id,
                                                    UnderlyingHoldings.ISIN_Code == isin_code,
                                                    UnderlyingHoldings.Is_Deleted != 1).order_by(desc('Portfolio_Date')).all()

                # some logic for min purchase date
                min_purchasedate = None
                list_holdings = []
                for holdings in sql_allholdings:
                    if min_purchasedate == None:
                        min_purchasedate = holdings.Portfolio_Date
                        list_holdings.append(holdings.Underlying_Holdings_Id)
                    else:
                        diff = diff_month(min_purchasedate, holdings.Portfolio_Date)
                        if diff > 1:
                            break
                        else:
                            if holdings.Purchase_Date == None:
                                min_purchasedate = holdings.Portfolio_Date
                                list_holdings.append(holdings.Underlying_Holdings_Id)
                            else:
                                min_purchasedate = holdings.Purchase_Date
                                break
                

                # update the purchase date for the holdings
                for Underlying_Holdings_Id in list_holdings:
                    update_values = {
                        UnderlyingHoldings.Purchase_Date : min_purchasedate,
                        UnderlyingHoldings.Updated_By : user_id,
                        UnderlyingHoldings.Updated_Date : TODAY,
                    }

                    sql_update = db_session.query(UnderlyingHoldings).filter(UnderlyingHoldings.Underlying_Holdings_Id == Underlying_Holdings_Id).update(update_values)
                    db_session.commit()

            item = row.copy()
            item["Remarks"] = sector_remark + remark
            items.append(item)

        
    write_csv(output_file_path, header, items, __file__)


    # Trigger Analysis
    # TODO Move this to a function call which needs to be centrally maintained, 
    # this is common for aif_holdings.py, pms_holdings.py
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
                        T["PE_Ratio"] = ((((float(T['Percentage_to_AUM']) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 / float(T['PE']))) if T['Percentage_to_AUM'] else 0

                    if T['PBV'] != 0:
                        T["PB_Ratio"] = (((((float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 /  float(T['PBV']))) if T['Percentage_to_AUM'] else 0

                    if T['DivYield'] != 0:
                        T["DY_Ratio"] = ((((( float(T['Percentage_to_AUM'])) / float(sum_Percentage_to_AUM)) * 100.00)/100) * (1 /  float(T['DivYield']))) if T['Percentage_to_AUM'] else 0


                for T in Summary_PC: # loop through each distinct plan id name date
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
                        FactSheet.PortfolioP_BRatio: sum_PB_Ratio,
                        FactSheet.PortfolioP_ERatio: sum_PE_Ratio,
                        FactSheet.AvgMktCap_Rs_Cr: avg_mcap,
                        FactSheet.Portfolio_Dividend_Yield: sum_DY_Ratio,
                        FactSheet.Updated_By: user_id,
                        FactSheet.Updated_Date: TODAY,
                    }

                    sql_h = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id==T['Plan_Id'], FactSheet.TransactionDate == portfolio_date) , FactSheet.Is_Deleted != 1).update(update_values)
                    db_session.commit()

                    sql_factsheetdata = db_session.query(FactSheet).filter(and_(FactSheet.Plan_Id==T['Plan_Id'], FactSheet.TransactionDate == portfolio_date) , FactSheet.Is_Deleted != 1).one_or_none()

                    sql_allholdings = db_session.query(UnderlyingHoldings).join(Fund, Fund.Fund_Id == UnderlyingHoldings.Fund_Id).join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id).join(Product, Product.Product_Id == PlanProductMapping.Product_Id).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id , isouter=True).filter(Product.Product_Id == PRODUCT_ID).filter(UnderlyingHoldings.Portfolio_Date == portfolio_date).filter(Plans.Plan_Id == i_planid).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(UnderlyingHoldings.Is_Deleted != 1).all()

                    if sql_factsheetdata:
                        for uh in sql_allholdings:
                            if uh.Percentage_to_AUM:
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
    FILE_PATH = "samples/cmots/PMS and AIF Holdings.csv"
    READ_PATH = "samples/cmots/PMS and AIF Holdings_R.csv"
    import_holdings_file(FILE_PATH, READ_PATH, USER_ID)

