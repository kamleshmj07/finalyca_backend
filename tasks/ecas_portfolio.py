import xmltodict
import datetime
from reports.utils import prepare_pdf_from_html
from utils.utils import generate_report_jwt_token
from compass.portfolio_pdf_helper import prepare_portfolio_report 
from bizlogic.common_helper import schedule_email_activity
import xml.etree.ElementTree as ET
from bizlogic.importer_helper import get_organizationid_by_userid, get_organization_whitelabel

def process_portfolio_xray_pdf_report(db_session, sql_request, secret_key, exp_in_min):
    dic = xmltodict.parse(sql_request.Parameters)

    portfolio_dic = dic['portfolio_parameters']
    user_id = int(portfolio_dic['user_id'])
    plan_id = portfolio_dic['plan_id']
    benchmark_id = portfolio_dic['benchmark_id']    
    portfolio_date = datetime.datetime.strptime(portfolio_dic['portfolio_date'], '%Y-%m-%d %H:%M:%S')
    from_date = datetime.datetime.strptime(portfolio_dic['from_date'], '%Y-%m-%d %H:%M:%S')
    to_date = datetime.datetime.strptime(portfolio_dic['to_date'], '%Y-%m-%d %H:%M:%S')

    account_ids = [portfolio_dic['account_ids']['item']] if isinstance(portfolio_dic['account_ids']['item'], str) else portfolio_dic['account_ids']['item']
    is_detailed = True if portfolio_dic['is_detailed'] == '1' else False

    show_trend_analysis = portfolio_dic['show_trend_analysis']
    show_performance = portfolio_dic['show_performance']
    show_stock_details = portfolio_dic['show_stock_details']
    page_break_required = portfolio_dic['page_break_required']

    image_path = portfolio_dic['image_path']
    whitelabel_dir = portfolio_dic['whitelabel_dir']
    generatereport_dir = portfolio_dic['generatereport_dir']
    time_period_type = portfolio_dic['time_period_type']
    
    #Generate report
    file_name = prepare_portfolio_report(db_session, user_id, account_ids, portfolio_date, from_date, to_date, plan_id, benchmark_id, is_detailed, show_trend_analysis, show_performance, show_stock_details, page_break_required, image_path, whitelabel_dir, generatereport_dir, time_period_type)

    if file_name:
        attachements = list()
        attachements.append(file_name)

        elem = ET.Element("Attachments")
        for attachment in attachements:
            child = ET.Element("File")
            child.text = attachment
            elem.append(child)
        
        #Generate X-Token
        file_info = {
            'User_Id':sql_request.Created_By,
            'Recipients': sql_request.Recipients,
            'Request_Id' : sql_request.Request_Id,
            'File': file_name
        }
        
        token = generate_report_jwt_token(file_info, secret_key, exp_in_min )
        sql_request.X_Token = token

        if sql_request.Recipients: #if email requested
            organization_whitelabel_data = get_organization_whitelabel(db_session, get_organizationid_by_userid(db_session, user_id))

            #send mail with link
            template_vars = dict()
            template_vars["organization_logo_path"] =F"{portfolio_dic['image_path']}/{portfolio_dic['whitelabel_dir']}/{organization_whitelabel_data['logo']}"        
            template_vars["customer_name"] = ''
            template_vars["organization_name"] = ''   
            template_vars["report_name"] = "Portfolio Report"
            template_vars["token"] = token

            #Get email body from template
            html_body = prepare_pdf_from_html(template_vars, 'pdf_portfolioxray_email_template.html', portfolio_dic['generatereport_dir'], True)
            subject = F"Your Portfolio X-ray Report from Finalyca"

            #Send email               
            schedule_email_activity(db_session, sql_request.Recipients, '', '', subject, html_body, [])

        sql_request.Attachments = ET.tostring(elem).decode()
        
    return sql_request