import os
import csv
from datetime import datetime as dt
from bizlogic.importer_helper import lookup_sebi_sector_using_sebi_industry, save_issuer, write_csv
from utils import *
from utils.finalyca_store import get_finalyca_scoped_session, is_production_config
from fin_models import *
from bizlogic.cmots_helper import save_mf_in_holding_master, save_equity_in_holding_master


def import_equityscriptmaster_file(filepath, readpath, user_id):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../config/local.config.yaml"
    )
    config = get_config(config_file_path)

    db_session = get_finalyca_scoped_session(is_production_config(config))
    
    TODAY = dt.today()
    header = [ "ScripCode", "CompanyName", "BSECODE", "NSESYMB","BSEGRUP", "NSEGRUP", "SECTOR", "ISIN_DEMAT", "SECURITY_CATEGORY",
              "PAIDUP", "MarketCap", "MCXSYMB", "MCXGRUP", "SectorCode", "IssuerName", "IssuerCode", "ScripStatus", "Remarks"]
    items = list()

    with open(filepath, 'r') as f:
        csvreader = csv.DictReader(f, delimiter="|")
        for row in csvreader:
            scrip_code = row["ScripCode"]
            company_name = row["CompanyName"]
            bse_code = row["BSECODE"]
            nse_symbol = row["NSESYMB"]
            bse_group = row['BSEGRUP']
            nse_group = row['NSEGRUP']
            sector_name = row["SECTOR"]
            isin_demat = row["ISIN_DEMAT"]
            security_category = row["SECURITY_CATEGORY"]
            paid_up = row["PAIDUP"]
            market_cap = row["MarketCap"] # Large cap, Mid cap or small cap
            mcx_symbol = row["MCXSYMB"]
            mcx_group = row["MCXGRUP"]
            sector_code = row["SectorCode"] if row["SectorCode"] != '' else None
            issuer_name = row["IssuerName"]
            issuer_code = row["IssuerCode"]
            script_status = row['ScripStatus']
            remark = None

            is_sec_MF = False
            if isin_demat:
                if isin_demat.startswith("INF"):
                    is_sec_MF = True
                try:
                    sebi_sector_info = lookup_sebi_sector_using_sebi_industry(db_session, sector_code, row, isin_demat) if not is_sec_MF else None
                    
                    sql_sectorid = sebi_sector_info[0]
                    industry_name = sebi_sector_info[3]

                    sql_issuerid = save_issuer(db_session, issuer_code, issuer_name, user_id, TODAY)[0]

                    if is_sec_MF:
                        remark = save_mf_in_holding_master(db_session, company_name, bse_code, nse_symbol, isin_demat, issuer_name, issuer_code, script_status, user_id, TODAY)
                    else:
                        remark = save_equity_in_holding_master(db_session, company_name, bse_code, nse_symbol, sql_sectorid,
                                                               isin_demat, industry_name, market_cap, issuer_name, issuer_code,
                                                               sql_issuerid, user_id, TODAY)
                except Exception as ex:
                    remark = ex.args[0]
            else:
                remark = "ISIN value is not present for the scrip."

            item = row.copy()
            item["Remarks"] = remark
            items.append(item)
            print(remark)

    # write_csv_v2(readpath, header, items, ',')
    write_csv(readpath, header, items, __file__)

if __name__ == '__main__':
    USER_ID = 1
    input_file = "samples/cmots/11052023_Holdings/EquityScripMaster.csv"
    read_file = "samples/cmots/11052023_Holdings/EquityScripMaster_R.csv"

    FILE_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), input_file )
    READ_PATH = os.path.join( os.path.dirname(os.path.abspath(__file__)), read_file )

    import_equityscriptmaster_file(FILE_PATH, READ_PATH, USER_ID)