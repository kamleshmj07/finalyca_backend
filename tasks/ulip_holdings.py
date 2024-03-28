from datetime import datetime as dt
from bizlogic.importer_helper import save_issuer, get_rel_path, lookup_sebi_sector_using_sebi_industry
from bizlogic.cmots_helper import get_holding_security_type_mapping
from sqlalchemy import desc, and_, func 
import os
import pandas as pd

from utils import *
from utils.finalyca_store import *
from fin_models.transaction_models import *
from fin_models.masters_models import *
from fin_models.logics_models import *
from bizlogic.common_helper import schedule_email_activity

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def import_ulip_holdings_file(input_file_path, output_file_path, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)
    
    db_session = get_finalyca_scoped_session(is_production_config(config))

    exceptions_data = []
    items = list()
    TODAY = dt.today()
    file_path = get_rel_path(input_file_path, __file__)

    df_holdings = pd.read_csv(file_path, delimiter='|', encoding= 'unicode_escape')
    gb = df_holdings.groupby(['FundID'])
    list_grouped_holdings = [gb.get_group(x) for x in gb.groups]
    portfolio_date_ = df_holdings.PortfolioDate.iloc[0]
    holding_security_type_mapping = get_holding_security_type_mapping()

    for df in list_grouped_holdings:
        df_ = df.copy()
        df_["Remarks"] = ""
        df_.fillna("", inplace=True)
        is_underlyingholdings_deleted = False
        with db_session.begin():
            try:
                for index, row in df_.iterrows():
                    holding_sec_id = ''
                    fund_code = row["FundID"]
                    isin_code = row["ISINCode"]
                    pf_date = row["PortfolioDate"].replace("/","-")
                    portfolio_date = portfolio_date_ if pf_date == "" else dt.strptime(pf_date, '%d-%m-%Y')
                    sector_code = None if row["SectorCode"] == "" else row["SectorCode"]
                    sector_name = row["SectorNames"]
                    security_name = row["Company_SecurityName"]
                    security_class = row["Security_AssetClass"]
                    percent_aum = to_float(row["Percentage_AUM"])
                    value_in_inr = to_float(row["Value_In_INR"])*10000000 # denominate to Rs INR
                    risk_category = row["RiskCategory"]
                    instrument_type = row["InstrumentType"]
                    issuer_name = row["IssuerName"]
                    issuer_code = str(row["IssuerCode"]).split('.')[0]
                    marketcap = row["MarketCap"]
                    stockrank = row["StocksRank"]
                    remark = None

                    sql_fund = db_session.query(Fund).filter(Fund.Fund_Code == "INS_" + str(fund_code)).one_or_none()

                    if not sql_fund:
                        remark = "Fund details are not available in the system"

                    # Update all previous holdings to be soft deleted
                    if is_underlyingholdings_deleted == False:
                        update_all_values = {
                            UnderlyingHoldings.Is_Deleted : 1,
                            UnderlyingHoldings.Updated_By : user_id,
                            UnderlyingHoldings.Updated_Date : TODAY,
                        }
                        db_session.query(UnderlyingHoldings).filter(and_(UnderlyingHoldings.Fund_Id==sql_fund.Fund_Id, UnderlyingHoldings.Portfolio_Date==portfolio_date))\
                                                            .update(update_all_values)
                        db_session.flush()
                        is_underlyingholdings_deleted = True

                    sebi_sector_info = lookup_sebi_sector_using_sebi_industry(db_session, sector_code, row, isin_code)

                    sql_issuerid = save_issuer(db_session, issuer_code, issuer_name, user_id, TODAY, commit_flag=False)[0]

                    

                    sql_sectorid = sebi_sector_info[0]
                    industry_name = sebi_sector_info[3]

                    holding_security_type = holding_security_type_mapping[security_class.strip()].upper()
                    if isin_code != "" and isin_code != "-":
                        sql_security = db_session.query(HoldingSecurity).filter(HoldingSecurity.ISIN_Code == isin_code,
                                                                            HoldingSecurity.Is_Deleted != 1,
                                                                            HoldingSecurity.active == 1).one_or_none()
                        if not sql_security:
                            HoldingSecurity1 = HoldingSecurity()
                            HoldingSecurity1.HoldingSecurity_Name = security_name
                            HoldingSecurity1.ISIN_Code = isin_code
                            HoldingSecurity1.Sector_Id = sql_sectorid
                            HoldingSecurity1.Sub_SectorName = industry_name
                            HoldingSecurity1.Asset_Class = security_class
                            HoldingSecurity1.Instrument_Type = instrument_type
                            HoldingSecurity1.Issuer_Code = issuer_code
                            HoldingSecurity1.Issuer_Name = issuer_name
                            HoldingSecurity1.Is_Deleted = 0
                            HoldingSecurity1.Created_By = user_id
                            HoldingSecurity1.Created_Date = TODAY
                            HoldingSecurity1.Issuer_Id = sql_issuerid
                            HoldingSecurity1.MarketCap = stockrank
                            HoldingSecurity1.HoldingSecurity_Type = holding_security_type if holding_security_type else None
                            HoldingSecurity1.active = 1

                            db_session.add(HoldingSecurity1)    
                            db_session.flush()                    

                    if portfolio_date > TODAY:
                        remark = "Underlying Holding cannot be uploaded for Future date."

                    if remark == None:
                        if isin_code != "" and isin_code != "-":
                            holding_sec_id = db_session.query(func.max(HoldingSecurity.HoldingSecurity_Id)).filter(HoldingSecurity.ISIN_Code==isin_code).filter(HoldingSecurity.active == 1).filter(HoldingSecurity.Is_Deleted != 1).scalar()  

                        ul = UnderlyingHoldings()
                        ul.Fund_Id = sql_fund.Fund_Id
                        ul.Fund_Code = sql_fund.Fund_Code
                        ul.ISIN_Code = isin_code
                        ul.Portfolio_Date = portfolio_date
                        ul.Sector_Code = sebi_sector_info[1]
                        ul.Sector_Names = sebi_sector_info[2]
                        ul.Company_Security_Name = security_name
                        ul.Asset_Class = security_class
                        ul.Percentage_to_AUM = percent_aum
                        ul.Value_in_INR = value_in_inr
                        ul.Risk_Category = risk_category
                        ul.InstrumentType = instrument_type
                        ul.IssuerName = issuer_name
                        ul.IssuerCode = issuer_code
                        ul.MarketCap =  None if marketcap == None else to_float(marketcap) # TODO: Handle this issue temporarily to fill in None as it is for US firms
                        ul.StocksRank = stockrank
                        ul.Created_By = user_id
                        ul.Created_Date = TODAY
                        ul.Is_Deleted = 0
                        ul.HoldingSecurity_Id = holding_sec_id
                        db_session.add(ul)
                        db_session.flush()
                        remark = 'Uploaded Successfully'

                        if isin_code != "" and isin_code != "-":
                            sql_allholdings = db_session.query(UnderlyingHoldings.Purchase_Date, 
                                                            UnderlyingHoldings.Underlying_Holdings_Id, 
                                                            UnderlyingHoldings.Portfolio_Date)\
                                                        .filter(UnderlyingHoldings.Fund_Id == sql_fund.Fund_Id,
                                                                UnderlyingHoldings.ISIN_Code == isin_code,
                                                                UnderlyingHoldings.Is_Deleted != 1)\
                                                        .order_by(desc('Portfolio_Date')).all()

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

                                sql_update = db_session.query(UnderlyingHoldings)\
                                                    .filter(UnderlyingHoldings.Underlying_Holdings_Id == Underlying_Holdings_Id)\
                                                    .update(update_values)
                                db_session.flush()
                    row["Remarks"] = remark
                    print(remark)
            except Exception as ex:
                ex_record = {}
                ex_record['CMOTS_FundCode'] = row["FundID"]
                ex_record['CMOTS_ISIN'] = row["ISINCode"]
                ex_record['CMOTS_CompanyName'] = row["Company_SecurityName"]
                ex_record['Remarks'] = remark
                ex_record['Exception'] = str(ex)
                exceptions_data.append(ex_record)
                print('Exception Recorded!!!')
                
                db_session.rollback()
                db_session.flush()
                continue
                        
            items.append(df_)
            db_session.commit()
            
    if exceptions_data:
        exception_file_path = get_rel_path(output_file_path, __file__).lower().replace('holdingsupdateinsurance', 'holdingsupdateinsurance_exception')
        exception_file_path = exception_file_path.replace('/',r'\\')
        # check whether the specified path exists or not
        read_folder = '\\'.join(get_rel_path(output_file_path, __file__).split('/')[:-1])
        isExist = os.path.exists(read_folder)
        if not isExist:
            # create a new directory because it does not exist
            try:
                os.makedirs(read_folder)
            except Exception as ex:
                print('File could not be created. Moving on....')

        df_exception = pd.DataFrame(exceptions_data)
        df_exception.to_csv(exception_file_path)

        attachements = list()
        attachements.append(exception_file_path)
        schedule_email_activity(db_session, 'devteam@finalyca.com', '', '', F"ULIP - Holding - Exception file{TODAY.strftime('%Y-%b-%d')}", F"ULIP - Holding - Exception file{TODAY.strftime('%Y-%b-%d')}", attachements)

    output_file = get_rel_path(output_file_path, __file__).lower()
    pd.concat(items).to_csv(output_file)

if __name__ == '__main__':
    USER_ID = 1
    FILE_PATH = "samples/HoldingsUpdateInsurance_11-07-2023-11-57-16.csv"
    READ_PATH = "read/HoldingsUpdateInsurance_11-07-2023-11-57-16.csv"
    import_ulip_holdings_file(FILE_PATH, READ_PATH, USER_ID)