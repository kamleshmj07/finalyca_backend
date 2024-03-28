import sys
import xml.dom.minidom
from fin_models.servicemanager_models import DeliveryManager, DeliveryRequest, ReportJobs, ReportSchedules
# from .sebi_export import prepare_export
# from .sebi_scrapper import main
from .report_usage import export_business_report, export_data_report, check_trial_user_usage
import logging
from utils import *

from datetime import date, timedelta
from datetime import datetime as dt
from sebi_lib.utils import get_last_day_for_prev_month

from async_tasks.send_email import send_email_complete

from dateutil.relativedelta import relativedelta
from tasks.sebi_scrapper import main
from tasks.sebi_export import prepare_export as prepare_export
from tasks.sebi_export_website import prepare_export as prepare_export_website
from tasks.ftp_fetch import download_cmots_files, extract_bilav_files
from sebi_lib.utils import get_last_day_for_prev_month
from utils.utils import print_query
import os
from sqlalchemy import or_
import xml.etree.ElementTree as ET
import tasks.cmots_cmasmaster, tasks.cmots_dlyprice, tasks.cmots_latesteqcttm, tasks.cmots_nsebseweight,\
       tasks.cmots_trireturnsmaster, tasks.mf_factsheet, tasks.mf_fundmaster, tasks.mf_holdings,\
       tasks.mf_nav, tasks.equityscriptmaster, tasks.ulip_factsheet, tasks.ulip_nav, tasks.ulip_fund_master,\
       tasks.ulip_fundmanagers, tasks.ulip_holdings, tasks.bilav_debt_importer, tasks.bilav_debt_price_importer
from bizlogic.fund_portfolio_analysis import *
from bizlogic.fund_manager_analysis import *
from tasks.update_equity_style_holdings import *
from bizlogic.fill_nav_gap import *
from tasks.client_report import *
from tasks.cmots_fundmanager import upload_mf_fundmanager
# from src.portfolio import process_portfolio_xray_pdf_report
from tasks.ecas_portfolio import process_portfolio_xray_pdf_report
from bizlogic.common_helper import schedule_email_activity
from tasks.common_jobs import *
# from xml.etree.ElementTree import Element,tostring

def get_delivery_channel(db_session, channel_id):
    obj = dict()

    sql_obj = db_session.query(DeliveryManager).filter(DeliveryManager.Channel_Id == channel_id).one_or_none()
    if sql_obj:
        obj["channel_id"] = sql_obj.Channel_Id
        obj["channel_name"] = sql_obj.Channel_Name
        obj["channel_description"] = sql_obj.Channel_Description
        # obj["parameters"] = sql_obj.Parameters
        parameters = sql_obj.Parameters
        obj["frequency_in_sec"] = sql_obj.Frequency_In_Seconds
        obj["is_enabled"] = sql_obj.Enabled
        obj["log_mode"] = sql_obj.Log_Mode

        DOMTree = xml.dom.minidom.parseString(parameters)
        collection = DOMTree.documentElement
        host = collection.getElementsByTagName("Host")[0]
        obj["email_smtp_host"] = host.childNodes[0].data
        port = collection.getElementsByTagName("Port")[0]
        obj["email_smtp_port"] = port.childNodes[0].data
        enable_ssl = collection.getElementsByTagName("EnableSSL")[0]
        obj["email_is_ssl_enabled"] = int(enable_ssl.childNodes[0].data)
        uname = collection.getElementsByTagName("UserName")[0]
        obj["email_username"] = uname.childNodes[0].data
        pwd = collection.getElementsByTagName("Password")[0]
        obj["email_password"] = pwd.childNodes[0].data
        default_cred = collection.getElementsByTagName("UseDefaultCredentials")[0]
        obj["email_use_default_credentials"] = int(default_cred.childNodes[0].data)
        mail_from = collection.getElementsByTagName("MailFrom")[0]
        obj["email_email_id"] = mail_from.childNodes[0].data

    return obj  
 
def process_job_requests(db_session):   
    now = dt.now()
 
    sql_objs = db_session.query(ReportJobs, ReportSchedules).join(ReportSchedules, ReportSchedules.Schedule_Id == ReportJobs.Schedule_Id).filter(ReportSchedules.Start_Date <= now.date()).filter(or_(ReportSchedules.End_Date > now.date(), ReportSchedules.End_Date == None)).filter(ReportJobs.Enabled_Python == 1).filter(ReportJobs.Status.in_([0, 1, 2])).all()

    for sql_obj in sql_objs:
        execute_now = to_be_executed(sql_obj[0].Last_Run, sql_obj[1].Type, sql_obj[1].Frequency, sql_obj[1].Pickup_Hours, sql_obj[1].Pickup_Minutes)
        
        sql_job : ReportJobs = sql_obj[0]

        if execute_now:
            logging.warning(F"Report job with id {sql_job.Job_Id} start" )
            sql_job.Status = 3
            sql_job.Status_Message = "Executing"
            db_session.commit()

            try:

                if sql_job.Report_Type == 0:
                    # 0 -> SQL Table report -> Get SQL table in csv or txt and send email

                    pass
                elif sql_job.Report_Type == 1:
                    # 1 -> SSRS Reports -> Not Implemented

                    pass
                elif sql_job.Report_Type == 2:
                    # 2 -> Excel Reports -> Create Excel from results and send email
                    #same copy is below report type 8

                    pass
                elif sql_job.Report_Type == 3:
                    # 3 -> FTP File Import -> Download FTP file from remote server
                    # /Parameters/ImportType : FTP -> Read from FTP
                    # /Parameters/ImportType : LOCAL -> read from local drive

                    Job_Lookup = dict()
                    Job_Lookup[3] = tasks.cmots_cmasmaster.import_cmots_cmasmaster
                    Job_Lookup[5] = tasks.cmots_trireturnsmaster.import_cmotsupload_trireturnsmaster_file
                    Job_Lookup[6] = tasks.cmots_dlyprice.import_cmotsupload_dlyprice_file
                    Job_Lookup[7] = tasks.cmots_nsebseweight.import_cmotsupload_nsebseweight
                    Job_Lookup[8] = tasks.cmots_nsebseweight.import_cmotsupload_nsebseweight
                    Job_Lookup[9] = tasks.cmots_latesteqcttm.import_cmotsupload_latesteqcttm_file
                    Job_Lookup[28] = tasks.mf_fundmaster.import_mf_fundmaster_file
                    Job_Lookup[29] = tasks.mf_nav.import_mf_nav_file
                    Job_Lookup[30] = tasks.mf_holdings.import_mf_holding_file
                    Job_Lookup[31] = tasks.mf_factsheet.import_mf_factsheet_file
                    Job_Lookup[32] = tasks.equityscriptmaster.import_equityscriptmaster_file
                    Job_Lookup[34] = tasks.cmots_trireturnsmaster.import_cmotsupload_trireturnsmaster_file
                    Job_Lookup[35] = tasks.ulip_fund_master.import_ulip_fund_master_file
                    Job_Lookup[36] = tasks.ulip_nav.import_ulip_nav_file
                    Job_Lookup[37] = tasks.ulip_holdings.import_ulip_holdings_file
                    Job_Lookup[38] = tasks.ulip_factsheet.import_ulip_factsheet_file
                    Job_Lookup[47] = tasks.bilav_debt_importer.import_bilav_debt_data
                    Job_Lookup[48] = tasks.bilav_debt_price_importer.import_bilav_price_file


                    DOMTree = xml.dom.minidom.parseString(sql_job.Parameters)
                    collection = DOMTree.documentElement
                    #config parameters
                    import_type = collection.getElementsByTagName("ImportType")[0].childNodes[0].data

                    if import_type == "FTP":
                        indextype = ''

                        ftp_1 = collection.getElementsByTagName("FTPURL")[0]
                        ftp_host = ftp_1.childNodes[0].data
                        ftp_2 = collection.getElementsByTagName("FTPPORT")[0]
                        ftp_port = int(ftp_2.childNodes[0].data)
                        ftp_3 = collection.getElementsByTagName("FTPUserId")[0]
                        ftp_user = ftp_3.childNodes[0].data
                        ftp_4 = collection.getElementsByTagName("FTPPassword")[0]
                        ftp_pwd = ftp_4.childNodes[0].data
                        ftp_5 = collection.getElementsByTagName("ImportPath")[0]
                        local_dir = ftp_5.childNodes[0].data
                        ftp_6 = collection.getElementsByTagName("ImportFileName")[0]
                        remote_dir = ftp_6.childNodes[0].data
                        ftp_7 = collection.getElementsByTagName("DaysCount")[0]
                        days_count = ftp_7.childNodes[0].data                        

                        if collection.getElementsByTagName("IndexType"):
                            ftp_8 = collection.getElementsByTagName("IndexType")[0]
                            indextype = ftp_8.childNodes[0].data

                        #Mail parameters
                        et_2 = collection.getElementsByTagName("SendEmail")[0]
                        send_email = et_2.childNodes[0].data
                        et_3 = collection.getElementsByTagName("Recipients")[0]
                        recipients = et_3.childNodes[0].data
                        et_4 = collection.getElementsByTagName("Subject")[0]
                        subject = et_4.childNodes[0].data
                        et_5 = collection.getElementsByTagName("Body")[0]
                        email_body = et_5.childNodes[0].data if len(et_5.childNodes) else ''


                        attachement = list()
                        # for filename in filenames:
                        today = dt.today()
                        target_date = today + timedelta(days=int(days_count)) #added negative value -1

                        #get file name
                        dire = str(remote_dir).split('/')
                        dire_length = len(dire)
                        file_name = dire[dire_length - 1]

                        remote_dir = remote_dir.replace(file_name,'')
                        remote_dir = remote_dir.replace("@ddmmyyyy",target_date.strftime('%d%m%Y'))
                        # remote_dir = F"{remote_dir}/{target_date.strftime('%d%m%Y')}"

                        resp_file_name = download_cmots_files(ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir, local_dir, target_date, file_name)

                        response_file = os.path.join(local_dir, resp_file_name)

                        sl = os.path.splitext(resp_file_name)
                        read_file_name = F"{sl[0]}_R{sl[1]}"
                        read_file_path = os.path.join(local_dir, read_file_name)
                        # attach as an email attachement and send email
                        if response_file:
                            attachement.append(response_file)

                        if send_email:
                            schedule_email_activity(db_session, recipients, '', '', subject, email_body, attachement)

                        # upload the files
                        attachement = list()
                        func_exec = Job_Lookup[sql_job.Job_Id]
                        if indextype != '':
                            func_exec(response_file, read_file_path, indextype, '1')
                            attachement.append(read_file_path)
                        else:
                            func_exec(response_file, read_file_path, '1')
                            attachement.append(read_file_path)

                        #send response file over mail after upload
                        if attachement:
                            schedule_email_activity(db_session, recipients, '', '', subject + " Response file", email_body, attachement)

                    elif import_type == "DirectUpload":
                        # Very basic upload
                        path = collection.getElementsByTagName("UploadPath")[0]
                        local_dir = path.childNodes[0].data
                        local_file_name = collection.getElementsByTagName("FileName")[0]
                        file_name = local_file_name.childNodes[0].data
                        config_3 = collection.getElementsByTagName("DaysCount")[0]
                        days_count = config_3.childNodes[0].data

                        today = dt.today()
                        target_date = today + timedelta(days=int(days_count)) # added negative value -1

                        # get file name
                        local_dir = local_dir.replace(file_name,'')
                        local_dir = local_dir.replace("@ddmmyyyy", target_date.strftime('%d%m%Y'))
                        response_file = os.path.join(local_dir, file_name)
                        read_file_path = ''

                        # upload the files
                        func_exec = Job_Lookup[sql_job.Job_Id]
                        func_exec(response_file, read_file_path, 1)
                    elif import_type == "ZIP":
                        indextype = ''

                        ftp_5 = collection.getElementsByTagName("ImportPath")[0]
                        local_import_dir = ftp_5.childNodes[0].data
                        ftp_6 = collection.getElementsByTagName("ImportFileName")[0]
                        zip_file_name = ftp_6.childNodes[0].data
                        ftp_7 = collection.getElementsByTagName("DaysCount")[0]
                        days_count = ftp_7.childNodes[0].data

                        #Mail parameters
                        et_2 = collection.getElementsByTagName("SendEmail")[0]
                        send_email = et_2.childNodes[0].data
                        et_3 = collection.getElementsByTagName("Recipients")[0]
                        recipients = et_3.childNodes[0].data
                        et_4 = collection.getElementsByTagName("Subject")[0]
                        subject = et_4.childNodes[0].data
                        et_5 = collection.getElementsByTagName("Body")[0]
                        email_body = et_5.childNodes[0].data if len(et_5.childNodes) else ''            


                        # for filename in filenames:
                        today = dt.today()
                        target_date = today + timedelta(days=int(days_count)) #added negative value -1

                        # get file name
                        zip_file_name = str(zip_file_name).split('/')
                        zip_file_name = zip_file_name[len(zip_file_name) - 1]
                        zip_file_name = zip_file_name.replace("@ddmmyyyy",target_date.strftime('%d%m%Y'))

                        extracted_file_path = extract_bilav_files(local_import_dir, target_date, zip_file_name)

                        sl = os.path.splitext(os.path.basename(extracted_file_path))
                        read_file_name = F"{sl[0]}_R.csv"
                        read_file_path = os.path.join(local_import_dir, read_file_name)

                        # upload the files
                        attachement = list()
                        func_exec = Job_Lookup[sql_job.Job_Id]
                        
                        func_exec(extracted_file_path, read_file_path, '1')
                        attachement.append(read_file_path)

                        #send response file over mail after upload
                        if attachement:
                            schedule_email_activity(db_session, recipients, '', '', subject + " Response file", email_body, attachement)

                elif sql_job.Report_Type == 4:
                    # 4 -> Upload File -> read from https://api.finalyca.com/Uploads/ api
                    # /Parameters/ImportType : FILECOPY -> 
                    pass

                elif sql_job.Report_Type == 5:
                    # 5 -> SEBI Scrapper for PMS data
                    # CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'sebi_config.yaml')

                    DOMTree = xml.dom.minidom.parseString(sql_job.Parameters)
                    collection = DOMTree.documentElement
                    et_1 = collection.getElementsByTagName("ImportType")[0]
                    import_type = et_1
                    et_2 = collection.getElementsByTagName("SendEmail")[0]
                    send_email = et_2.childNodes[0].data
                    et_3 = collection.getElementsByTagName("Recipients")[0]
                    recipients = et_3.childNodes[0].data
                    et_4 = collection.getElementsByTagName("Subject")[0]
                    subject = et_4.childNodes[0].data
                    et_5 = collection.getElementsByTagName("Body")[0]
                    email_body = et_5.childNodes[0].data if len(et_5.childNodes) else ''
                    et_6 = collection.getElementsByTagName("HTML_Backup_Dir")[0]
                    html_backup_dir = et_6.childNodes[0].data if len(et_6.childNodes) else ''
                    et_7 = collection.getElementsByTagName("Log_Dir")[0]
                    log_dir = et_7.childNodes[0].data if len(et_7.childNodes) else ''
                    et_8 = collection.getElementsByTagName("Table_Create")[0]
                    table_create = et_8.childNodes[0].data if len(et_8.childNodes) else ''
                    et_9 = collection.getElementsByTagName("Export_Dir")[0]
                    export_dir = et_9.childNodes[0].data

                    main(log_dir, html_backup_dir, table_create)

                elif sql_job.Report_Type == 6:
                    # 6 -> SEBI Scrapper Data - csv

                    DOMTree = xml.dom.minidom.parseString(sql_job.Parameters)
                    collection = DOMTree.documentElement
                    
                    et_2 = collection.getElementsByTagName("SendEmail")[0]
                    send_email = et_2.childNodes[0].data
                    et_3 = collection.getElementsByTagName("Recipients")[0]
                    recipients = et_3.childNodes[0].data
                    et_4 = collection.getElementsByTagName("Subject")[0]
                    subject = et_4.childNodes[0].data
                    et_5 = collection.getElementsByTagName("Body")[0]
                    email_body = et_5.childNodes[0].data if len(et_5.childNodes) else ''
                    et_9 = collection.getElementsByTagName("Export_Dir")[0]
                    export_dir = et_9.childNodes[0].data

                    now = dt.today()
                    as_on_date = get_last_day_for_prev_month(now.month, now.year)

                    attachements = prepare_export (export_dir, as_on_date)
 
                    if send_email:
                        schedule_email_activity(db_session, recipients, '', '', subject, email_body, attachements)

                elif sql_job.Report_Type == 7:
                    # 6 -> SEBI Scrapper Data - json

                    DOMTree = xml.dom.minidom.parseString(sql_job.Parameters)
                    collection = DOMTree.documentElement
                    
                    et_2 = collection.getElementsByTagName("SendEmail")[0]
                    send_email = et_2.childNodes[0].data
                    et_3 = collection.getElementsByTagName("Recipients")[0]
                    recipients = et_3.childNodes[0].data
                    et_4 = collection.getElementsByTagName("Subject")[0]
                    subject = et_4.childNodes[0].data
                    et_5 = collection.getElementsByTagName("Body")[0]
                    email_body = et_5.childNodes[0].data if len(et_5.childNodes) else ''
                    et_9 = collection.getElementsByTagName("Export_Dir")[0]
                    export_dir = et_9.childNodes[0].data

                    now = dt.today()
                    as_on_date = get_last_day_for_prev_month(now.month, now.year)

                    # export_path = "..\prelogin_next\data"

                    attachements = prepare_export_website(export_dir, as_on_date)
 
                    if send_email:
                        schedule_email_activity(db_session, recipients, '', '', subject, email_body, attachements)

                elif sql_job.Report_Type == 8:
                    # 2 -> Excel Reports -> Create Excel from results and send email
                    job_lookup = dict()
                    job_lookup[22] = export_business_report
                    job_lookup[23] = export_data_report
                    job_lookup[44] = check_trial_user_usage
                    job_lookup[45] = upload_mf_fundmanager

                    DOMTree = xml.dom.minidom.parseString(sql_job.Parameters)
                    collection = DOMTree.documentElement
                    et_2 = collection.getElementsByTagName("SendEmail")[0]
                    send_email = et_2.childNodes[0].data
                    et_3 = collection.getElementsByTagName("Recipients")[0]
                    recipients = et_3.childNodes[0].data
                    et_4 = collection.getElementsByTagName("Subject")[0]
                    subject = et_4.childNodes[0].data
                    et_5 = collection.getElementsByTagName("Body")[0]
                    email_body = et_5.childNodes[0].data if len(et_5.childNodes) else ''                    
                    et_9 = collection.getElementsByTagName("ReportOutputPath")[0]
                    reportoutputpath = et_9.childNodes[0].data

                    func_exec = job_lookup[sql_job.Job_Id]
                    attachements = func_exec(db_session, reportoutputpath)

                    if send_email:
                        schedule_email_activity(db_session, recipients, '', '', subject, email_body, attachements)
                elif sql_job.Report_Type == 9:
                    #python function to execute
                    domtree = xml.dom.minidom.parseString(sql_job.Parameters)
                    collection = domtree.documentElement
                    et_1 = collection.getElementsByTagName("FunctionName")[0]
                    func_exec = et_1.childNodes[0].data
                    eval(func_exec)(db_session)

                elif sql_job.Report_Type == 10:
                    #execute sqlscript                    
                    config_file_path = os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "../config/local.config.yaml"
                    )
                    config = get_config(config_file_path)
                    engine = get_unsafe_db_engine(config)
                    connection = engine.raw_connection()
                    cursor_obj = connection.cursor()

                    domtree = xml.dom.minidom.parseString(sql_job.Parameters)
                    collection = domtree.documentElement
                    sqlscript = collection.getElementsByTagName("Script")[0].childNodes[0].data
                    tablenames = collection.getElementsByTagName("TableNames")[0]
                    
                    if tablenames.childNodes:
                        tablenames = tablenames.childNodes[0].data
                    
                        tables = tablenames.split(',')
                        if tables:                            
                                try:
                                    cursor_obj = connection.cursor()
                                    for table in tables:
                                        script = sqlscript.replace("##tablename##", str(table.strip()))
                                        cursor_obj.execute(script)

                                    cursor_obj.close()
                                    connection.commit()
                                finally:
                                    connection.close()
                    else:
                        try:
                            cursor_obj = connection.cursor()                            
                            cursor_obj.execute(sqlscript)

                            cursor_obj.close()
                            connection.commit()
                        finally:
                            connection.close()
                elif sql_job.Report_Type == 11:
                    # 2 -> Excel Reports -> call function to execute - and send email with attachment
                                       
                    domtree = xml.dom.minidom.parseString(sql_job.Parameters)
                    collection = domtree.documentElement
                    et1 = collection.getElementsByTagName("FunctionName")[0]
                    func_exec = et1.childNodes[0].data

                    et9 = collection.getElementsByTagName("SendEmail")[0]
                    send_email = et9.childNodes[0].data if len(et9.childNodes) else ''

                    et2 = collection.getElementsByTagName("Recipients")[0]
                    recipients = et2.childNodes[0].data

                    et5 = collection.getElementsByTagName("Recipientscc")[0]
                    Recipientscc = et5.childNodes[0].data if len(et5.childNodes) else ''

                    et6 = collection.getElementsByTagName("Recipientsbcc")[0]
                    Recipientsbcc = et6.childNodes[0].data if len(et6.childNodes) else ''

                    et3 = collection.getElementsByTagName("Subject")[0]
                    subject = et3.childNodes[0].data

                    et4 = collection.getElementsByTagName("Body")[0]
                    email_body = et4.childNodes[0].data

                    et7 = collection.getElementsByTagName("Day")[0]
                    report_day = et7.childNodes[0].data #on which it will run
                    report_day = str(report_day).strip().split(',')
                    
                    #get today date
                    if str(now.day) in report_day:
                        attachements = eval(func_exec)(db_session)

                        if send_email:
                            schedule_email_activity(db_session, recipients, Recipientscc, Recipientsbcc, subject, email_body, attachements)

                    
                sql_job.Status = 2
                sql_job.Status_Message = "Success"
                sql_job.Last_Run = dt.now()
                db_session.commit()          
                
            except Exception as ex:
                logging.warning(F"Error: Delivery request for id - {sql_job.Job_Id} Error : {str(ex)}" )
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                sql_job.Status_Message = F"Fail: {fname} - Line number {exc_tb.tb_lineno} - {str(ex)}"
                sql_job.Status = 2
                sql_job.Last_Run = dt.now()
                db_session.commit()
            finally:
                db_session.expire_all()
            



def process_delivery_requests(db_session):
    # Data Upload Status
    # 0 -> pending
    # 1 -> success
    # 2 -> Fail
    # 3 -> Executing
    # Check for all requests that have status 0
    sql_requests = db_session.query(DeliveryRequest).join(DeliveryManager, DeliveryManager.Channel_Id == DeliveryRequest.Channel_Id).filter(DeliveryManager.Enabled == 1).filter(DeliveryRequest.Status == 0).all()

    # for each sub services, check the pending tasks and execute them if required.
    for sql_request in sql_requests:
        logging.warning(F"Delivery request for id - {sql_request.Request_Id} start" )
        # Add info
        sql_request.Pick_Time = dt.now()
        sql_request.Status = 3
        sql_request.Pick_Time = dt.now()
        sql_request.Status_Message = "Executing"
        db_session.commit()

        status = 'Success'

        # process the request
        try:
            if sql_request.Type == 'X-Ray PDF':
                config_file_path = os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "../config/local.config.yaml"
                    )
                config = get_config(config_file_path)
                secret_key = config['SECRET_KEY']
                exp_in_min = config['REPORT_EXPIRY_PERIOD_IN_MIN']

                sql_request = process_portfolio_xray_pdf_report(db_session, sql_request, secret_key, exp_in_min)
            else:
                channel_id = sql_request.Channel_Id
                channel_obj = get_delivery_channel(db_session, channel_id)

                to_email = sql_request.Recipients
                cc_email = sql_request.RecipientsCC
                bcc_email = sql_request.RecipientsBCC
                email_subject = sql_request.Subject
                email_body = sql_request.Body
                is_html_body = sql_request.IsBodyHTML
                attach = xml.dom.minidom.parseString(sql_request.Attachments)
                tags = attach.getElementsByTagName("File")
                attachements = list()
                for tag in tags:
                    attachment_name = tag.childNodes[0].data
                    attachements.append(attachment_name)
                    
                # parameters = sql_request.Parameters
                # resources = sql_request.Resources

                # Here do the operation
                send_email_complete(channel_obj["email_smtp_host"], channel_obj["email_smtp_port"], channel_obj["email_username"], channel_obj["email_password"], to_email, cc_email, bcc_email, email_subject, email_body, attachements)
                
            sql_request.Status = 1
            sql_request.Status_Message = status
            sql_request.Completion_Time = dt.now()
            db_session.commit()

        except Exception as ex:
            logging.warning(F"Error: Delivery request for id - {sql_request.Request_Id} Error : {str(ex)}" )
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            sql_request.Status = 2
            sql_request.Status_Message = F"Fail: {fname} - Line number {exc_tb.tb_lineno} - {str(ex)}"
            sql_request.Completion_Time = dt.now()
            db_session.commit()

def to_be_executed(last_run: datetime, execution_type: str, execution_frequency: int, pickup_hours: int, pickup_minutes: int):
    # if frequency is 6 and type is 'hourly', the function will be executed every 6 hours. so frequency has to be linked to the TYPE. for minutely range will be from 1 to 60 and for hourly it will be from 1 to 24.
    execute_now = False

    # define next execution time
    if execution_type == 'secondly':
        next_run = last_run + relativedelta(seconds=execution_frequency)

    elif execution_type == 'minutely':
        next_run = last_run + relativedelta(minutes=execution_frequency)

    elif execution_type == 'hourly':
        next_run = last_run + relativedelta(hours=execution_frequency)
        # USE pickup minutes to find exact time for next execution
        next_run = next_run.replace(minute=pickup_minutes) 

    elif execution_type == 'daily':
        next_run = last_run + relativedelta(days=execution_frequency)
        next_run = next_run.replace(hour=pickup_hours, minute=pickup_minutes) 

    elif execution_type == 'weekly':
        next_run = last_run + relativedelta(weeks=execution_frequency)
        next_run = next_run.replace(hour=pickup_hours, minute=pickup_minutes) 
        
    elif execution_type == 'monthly':
        next_run = last_run + relativedelta(months=execution_frequency)
        next_run = next_run.replace(hour=pickup_hours, minute=pickup_minutes) 

    elif execution_type == 'quarterly':
        # Multiply frequency with 3 for a quarter e.g. 1 quarter will mean 3 months.
        next_run = last_run + relativedelta(months=execution_frequency * 3)
        next_run = next_run.replace(hour=pickup_hours, minute=pickup_minutes) 

    elif execution_type == 'yearly':
        next_run = last_run + relativedelta(years=execution_frequency)
        next_run = next_run.replace(hour=pickup_hours, minute=pickup_minutes) 

    if next_run < dt.now():
        execute_now = True

    return execute_now


# if __name__ == "__main__":
#     currenta
#     process_job_requests()