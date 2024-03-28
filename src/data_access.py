from typing import Dict
from flask import current_app
from utils import print_query
from fin_models import FundManager, Product

def get_fund_managers(fund_id: int, get_fund_manager_code: bool=False) -> Dict :
    sql_fund_managers = current_app.store.db.query(FundManager).filter(FundManager.Fund_Id==fund_id).filter(FundManager.DateTo==None).filter(FundManager.Is_Deleted != 1).all()
    managers = dict()
    for sql_fund_manager in sql_fund_managers: 
        if sql_fund_manager.FundManager_Id not in managers:
            managers[sql_fund_manager.FundManager_Code if get_fund_manager_code else sql_fund_manager.FundManager_Id] = sql_fund_manager.FundManager_Name
    return managers

def get_products() -> Dict:
    products = dict()
    sql_products = current_app.store.db.query(Product).all()
    for sql_product in sql_products:
        products[sql_product.Product_Id] = sql_product.Product_Name
    return products