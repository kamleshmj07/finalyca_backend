import logging
import os
from typing import List
from sqlalchemy import and_
from sebi_lib.models import * 
from datetime import date, datetime
from sebi_lib.utils import mssql_prod_uri, get_database_scoped_session, get_last_day_for_prev_month
import csv
import yaml
from async_tasks.send_email import send_email_with_attachements

def get_amc_export(db_session, as_on_dates: List, export_file_path):
    sql_objs = db_session.query(
        AMC.sebi_nr, AMC.name, AMC.register_date, 
        AMCInfo.as_of, AMCInfo.address, AMCInfo.principal_officer_name, AMCInfo.principal_officer_email, AMCInfo.principal_officer_contact, AMCInfo.compliance_officer_name, AMCInfo.compliance_officer_email, AMCInfo.total_client, AMCInfo.total_aum_in_cr, AMCInfo.is_discretionary_active, AMCInfo.is_non_discretionary_active, AMCInfo.is_advisory_active, 
        AMCTransactions.service_type, AMCTransactions.sales_in_month_in_cr, AMCTransactions.purchase_in_month_in_cr, AMCTransactions.ptr, 
        AMCClients.service_type, AMCClients.pf_clients, AMCClients.corporates_clients, AMCClients.non_corporates_clients, AMCClients.nri_clients, AMCClients.fpi_clients, AMCClients.others_clients, AMCClients.pf_aum, AMCClients.corporates_aum, AMCClients.non_corporates_aum, AMCClients.nri_aum, AMCClients.fpi_aum, AMCClients.others_aum,
        DiscretionaryDetails.aum, DiscretionaryDetails.inflow_month_in_cr, DiscretionaryDetails.outflow_month_in_cr, DiscretionaryDetails.net_flow_month_in_cr, DiscretionaryDetails.inflow_fy_in_cr, DiscretionaryDetails.outflow_fy_in_cr, DiscretionaryDetails.net_flow_fy_in_cr, DiscretionaryDetails.ptr_1_month, DiscretionaryDetails.ptr_1_yr,  
        AMCComplaints.pf_pending_at_month_start, AMCComplaints.pf_received_during_month, AMCComplaints.pf_resolved_during_month, AMCComplaints.pf_pending_at_month_end, AMCComplaints.corporates_pending_at_month_start, AMCComplaints.corporates_received_during_month, AMCComplaints.corporates_resolved_during_month, AMCComplaints.corporates_pending_at_month_end, AMCComplaints.non_corporates_pending_at_month_start, AMCComplaints.non_corporates_received_during_month, AMCComplaints.non_corporates_resolved_during_month, AMCComplaints.non_corporates_pending_at_month_end, AMCComplaints.nri_pending_at_month_start, AMCComplaints.nri_received_during_month, AMCComplaints.nri_resolved_during_month, AMCComplaints.nri_pending_at_month_end, AMCComplaints.fpi_pending_at_month_start, AMCComplaints.fpi_received_during_month, AMCComplaints.fpi_resolved_during_month, AMCComplaints.fpi_pending_at_month_end, AMCComplaints.others_pending_at_month_start, AMCComplaints.others_received_during_month, AMCComplaints.others_resolved_during_month, AMCComplaints.others_pending_at_month_end
        ).join(
            AMCComplaints, and_(AMCComplaints.sebi_nr==AMCInfo.sebi_nr, AMCComplaints.as_of == AMCInfo.as_of)
        ).join(
            AMCTransactions, and_(AMCTransactions.sebi_nr==AMCInfo.sebi_nr, AMCTransactions.as_of == AMCInfo.as_of, AMCTransactions.service_type==ServiceType.discretionary.name)
        ).join(
            AMCClients, and_(AMCClients.sebi_nr==AMCInfo.sebi_nr, AMCClients.as_of == AMCInfo.as_of, AMCClients.service_type==ServiceType.discretionary.name)
        ).join(
            AMCAssetClasses, and_(AMCAssetClasses.sebi_nr==AMCInfo.sebi_nr, AMCAssetClasses.as_of == AMCInfo.as_of, AMCAssetClasses.service_type==ServiceType.discretionary.name)
        ).join(
            AMC, AMCInfo.sebi_nr==AMC.sebi_nr
        ).join(
            DiscretionaryDetails, and_(DiscretionaryDetails.sebi_nr==AMCInfo.sebi_nr, DiscretionaryDetails.as_of == AMCInfo.as_of)
        ).filter(AMCInfo.as_of.in_(as_on_dates)).order_by(AMC.name)
    sql_objs = sql_objs.all()

    objs = list()
    for sql_obj in sql_objs:
        d = sql_obj._asdict()
        objs.append(d)

    export_csv(objs, export_file_path)

def export_csv(records, export_file_path):
    if len(records) > 0:
        with open(export_file_path, 'w', newline='') as fd:
            field_names = records[0].keys()
            writer = csv.DictWriter(fd, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(records)

def get_scheme_export(db_session, as_on_dates: List, export_file_path):
    sql_objs = db_session.query(
        Scheme.sebi_nr, Scheme.name, 
        SchemeAssetClasses.as_of, SchemeAssetClasses.listed_equity, SchemeAssetClasses.unlisted_equity, SchemeAssetClasses.listed_plain_debt, SchemeAssetClasses.unlisted_plain_debt, SchemeAssetClasses.listed_structured_debt, SchemeAssetClasses.unlisted_structured_debt, SchemeAssetClasses.equity_derivatives, SchemeAssetClasses.commodity_derivatives, SchemeAssetClasses.other_derivatives, SchemeAssetClasses.mutual_funds, SchemeAssetClasses.others, SchemeAssetClasses.total, 
        SchemePerformance.aum, SchemePerformance.scheme_returns_1_month, SchemePerformance.scheme_returns_1_yr, SchemePerformance.ptr_1_month, SchemePerformance.ptr_1_yr, SchemePerformance.benchmark_name, SchemePerformance.benchmark_returns_1_month, SchemePerformance.benchmark_returns_1_yr,
        SchemeFlow.inflow_month_in_cr, SchemeFlow.outflow_month_in_cr, SchemeFlow.net_flow_month_in_cr, SchemeFlow.inflow_fy_in_cr, SchemeFlow.outflow_fy_in_cr, SchemeFlow.net_flow_fy_in_cr
    ).join(
        SchemePerformance, and_(SchemePerformance.scheme_id==SchemeAssetClasses.scheme_id, SchemePerformance.as_of == SchemeAssetClasses.as_of)
    ).join(
        SchemeFlow, and_(SchemeFlow.scheme_id==SchemeAssetClasses.scheme_id, SchemeFlow.as_of == SchemeAssetClasses.as_of)
    ).join(
        Scheme, Scheme.id==SchemeAssetClasses.scheme_id
    ).filter(SchemeAssetClasses.as_of.in_(as_on_dates)).all()

    objs = list()
    for sql_obj in sql_objs:
        d = sql_obj._asdict()
        objs.append(d)

    export_csv(objs, export_file_path)

def prepare_export(export_dir_path, as_on_dates):
    db_url = mssql_prod_uri(True, "SEBI_PMS")

    db_session = get_database_scoped_session(db_url)

    amc_path = os.path.join(export_dir_path, 'amc.csv')
    scheme_path = os.path.join(export_dir_path, 'scheme.csv')

    logging.warning('Getting AMC Export')
    get_amc_export(db_session, as_on_dates, amc_path)

    logging.warning('Getting Scheme Export')
    get_scheme_export(db_session, as_on_dates, scheme_path)

    attachements = list()
    attachements.append(amc_path)
    attachements.append(scheme_path)

    return attachements

if __name__ == '__main__':
    now = date.today()
    as_on_date = get_last_day_for_prev_month(now.month, now.year)
    # as_on_dates  = [ as_on_date ]

    as_on_dates  = [ 
        date(2021, 1, 31), date(2021, 2, 28), date(2021, 3, 31), date(2021, 4, 30), date(2021, 5, 31), date(2021, 6, 30),
        date(2021, 7, 31), date(2021, 8, 31), date(2022, 9, 30), date(2022, 10, 31), date(2022, 11, 30), date(2022, 12, 31), 
        date(2022, 1, 31), date(2022, 2, 28), date(2022, 3, 31), date(2022, 4, 30), date(2022, 5, 31), date(2022, 6, 30) 
    ]

    target_emails = list()
    target_emails.append("vijay.shah@finalyca.com")
    target_emails.append("sachin.jaiswal@finalyca.com")

    export_path = "../"

    attachements = prepare_export(export_path, as_on_dates)
    
    # # Read SMTP settings from local.config.yaml
    # yaml_config_path = "../config/local.config.yaml"
    # with open(yaml_config_path) as conf_file:
    #     config = yaml.load(conf_file, Loader=yaml.FullLoader)

    # send_email_with_attachements(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], target_emails,  F"SEBI data for {str(as_on_date)}", "", attachements)