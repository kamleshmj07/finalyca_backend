from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import desc, func, or_, text
from werkzeug.exceptions import BadRequest
from fin_models import FundStocks
import pandas as pd
from .utils import required_user_access_pro
from utils.utils import print_query

exposure_bp = Blueprint("exposure_bp", __name__)

def get_exposure(db_session, entity_type, entity_id, amc_id, product_id, classification_id):
    q = db_session.query(
                            FundStocks.AMC_Id, 
                            FundStocks.AMC_Name, 
                            FundStocks.AMC_Logo, 
                            FundStocks.Fund_Id, 
                            FundStocks.Fund_Name,
                            FundStocks.Plan_Id, 
                            FundStocks.Plan_Name,
                            FundStocks.Product_Id, 
                            FundStocks.Product_Code, 
                            FundStocks.Product_Name, 
                            FundStocks.Classification_Id, 
                            FundStocks.Classification_Name,
                            FundStocks.HoldingSecurity_Name if not FundStocks.Sector_Id == entity_type else FundStocks.Plan_Name.label('ununsed_column1'),
                            FundStocks.Sector_Names,
                            FundStocks.IssuerName if not FundStocks.Sector_Id == entity_type else FundStocks.Plan_Name.label('ununsed_column2'),  
                            func.sum(FundStocks.Percentage_to_AUM).label('Percentage_to_AUM'),
                            func.sum(FundStocks.Value_In_Inr/10000000).label('Value_In_Cr'), 
                            func.min(FundStocks.Purchase_Date).label("Purchase_Date")
                        )
    #Commented below as it was not matching with equity analysis report.
    # q = q.filter(FundStocks.Value_In_Inr > 0.0).filter(FundStocks.Percentage_to_AUM > 0.0)
    q = q.filter(entity_type == entity_id).filter(FundStocks.ExitStockForFund != 1)

    if entity_type == FundStocks.HoldingSecurity_Id:
        q = q.filter(FundStocks.ISIN_Code.like("INE%"))
    if amc_id:
        q = q.filter(FundStocks.AMC_Id==amc_id)
    if classification_id:
        q = q.filter(FundStocks.Classification_Id==classification_id)
    if product_id:
        q = q.filter(FundStocks.Product_Id==product_id)
    q = q.group_by(FundStocks.AMC_Id, 
                   FundStocks.AMC_Name, 
                   FundStocks.AMC_Logo, 
                   FundStocks.Fund_Id, 
                   FundStocks.Fund_Name, 
                   FundStocks.Plan_Id, 
                   FundStocks.Plan_Name, 
                   FundStocks.Product_Id, 
                   FundStocks.Product_Code, 
                   FundStocks.Product_Name, 
                   FundStocks.Classification_Id, 
                   FundStocks.Classification_Name,
                   FundStocks.HoldingSecurity_Name if not FundStocks.Sector_Id == entity_type else text(''),
                   FundStocks.Sector_Names,
                   FundStocks.IssuerName if not FundStocks.Sector_Id == entity_type else text(''))
    # print_query(q)
    sql_fund_stocks = q.all()

    funds = pd.DataFrame(sql_fund_stocks)
    
    # rename columns to lower case
    funds.columns= funds.columns.str.lower()

    resp = dict()
    if not funds.empty:
        funds = funds.sort_values(by='percentage_to_aum', ascending=False)

        resp["total_funds"] = len(funds)
        resp["data"] = funds.to_dict(orient="records")

        resp["highest_weight"] = funds.head(1).to_dict(orient="records")[0]
        resp["lowest_weight"] = funds.tail(1).to_dict(orient="records")[0]

        funds = funds.sort_values(by='value_in_cr', ascending=False)
        resp["highest_value"] = funds.head(1).to_dict(orient="records")[0]
        resp["lowest_value"] = funds.tail(1).to_dict(orient="records")[0]

    return resp

@exposure_bp.route('/api/v1/exposure_api/security')
@required_user_access_pro
def get_security_exposure():
    security_id = request.args.get("id", type=int, default=None)
    isin = request.args.get("isin", type=str, default=None)

    if not security_id and not isin:
        raise BadRequest(description="Parameter Required: <id> or <isin>")

    classification_id = request.args.get("Classification_Id", type=int, default=None)
    product_id = request.args.get("Product_Id", type=int, default=None)
    amc_id = request.args.get("AMC_Id", type=int, default=None)

    entity_type, entity_id = None, None
    if security_id:
        entity_type = FundStocks.HoldingSecurity_Id
        entity_id = security_id
    elif isin:
        entity_type = FundStocks.ISIN_Code
        entity_id = isin

    resp = get_exposure(current_app.store.db, entity_type , entity_id, amc_id, product_id, classification_id)

    return jsonify(resp)

@exposure_bp.route('/api/v1/exposure_api/issuer')
@required_user_access_pro
def get_issuer_exposure():
    issuer_id = request.args.get("id", type=int, default=None)
    if not issuer_id:
        raise BadRequest(description="No issuer id was provided")

    AMC_Id = request.args.get("AMC_Id", type=int, default=None)
    Product_Id = request.args.get("Product_Id", type=int, default=None)
    Classification_Id = request.args.get("Classification_Id", type=int, default=None)

    resp = get_exposure(current_app.store.db, FundStocks.Issuer_Id, issuer_id, AMC_Id, Product_Id, Classification_Id)
    
    return jsonify(resp)

@exposure_bp.route('/api/v1/exposure_api/sector')
@required_user_access_pro
def get_sector_exposure():
    sector_id = request.args.get("id", type=int, default=None)
    if not sector_id:
        raise BadRequest(description="No sector id was provided")

    Classification_Id = request.args.get("Classification_Id", type=int, default=None)
    Product_Id = request.args.get("Product_Id", type=int, default=None)
    AMC_Id = request.args.get("AMC_Id", type=int, default=None)
    
    resp = get_exposure(current_app.store.db, FundStocks.Sector_Id, sector_id, AMC_Id, Product_Id, Classification_Id)
    
    return jsonify(resp)
