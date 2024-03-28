"""
TODO: 
1] Add SMS feature as well for the users with registered mobile numbers
2] Incomplete logic, to be finalized and tested before release.
"""

import os
from utils.utils import *
from datetime import date, timedelta
from sqlalchemy import and_
from fin_models.controller_master_models import *
from utils.finalyca_store import get_data_store
from async_tasks.send_email import send_email_async
from jinja2 import Environment, FileSystemLoader, select_autoescape
# from bizlogic.importer_helper import get_rel_path

def send_trial_renewal_email():    
    """
    Find organization level users with active trial, then 
    send email to registered email id for renewal.
    """
    # setting up configurations # TODO Move this to config file/set this as inputs, review naming of configurations
    trial_check_in_days = 7
    notification_gap = 3

    # returns the current local date
    start_date = date.today()
    end_date = date.today() + timedelta(days=trial_check_in_days)

    # setting up jinja environment
    jinja_env = Environment(
        loader = FileSystemLoader(r"templates"),   # TODO : Template placement in tasks folder ? Check for right path w/ Sachin or Ibrahim
        autoescape = select_autoescape()
    )

    template = jinja_env.get_template("subscribe.html")

    # db session
    config_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../config/local.config.yaml"
        )

    config = get_config(config_file_path)
    db_session = get_data_store(config).db

    # get mail server info
    mail_server = config["EMAIL_SERVER"]
    port = config["EMAIL_PORT"]
    from_email = config["EMAIL_ID"]
    pwd = config["EMAIL_PASS"]
    subject_line = "Finalyca is excited to welcome you onboard!"

    # get Organization info from db
    org_info = db_session.query(Organization.Organization_Name,
                                Organization.License_Expiry_Date,
                                Organization.Adminuser_Fullname,
                                Organization.Adminuser_Email,
                                Organization.Adminuser_Mobile).select_from(Organization) \
                                                            .filter(and_(Organization.License_Expiry_Date >= start_date
                                                                        ,Organization.License_Expiry_Date <= end_date)) \
                                                            .filter(Organization.Is_Active == 1) \
                                                            .filter(Organization.is_trial_active == 1) \
                                                            .all()
                                                            
    # TODO Remove print
    print(org_info)

    for user in org_info:
        org_name = user[0]
        exp_date = datetime.strptime(user[1], r'%y-%m-%d')
        admin_name = user[2]
        to_email = user[3]
        days_remaining = exp_date - start_date
        send_email_flag = True if (days_remaining % notification_gap == 0) or (days_remaining == 1) else False

        if send_email_flag:
            # TODO Review with Philip, org_name or admin_name?. Also subject line
            # replace the name and other things in the jinja template
            html_message = template.render(name=org_name)

            send_email_async(mail_server, port, from_email, pwd, to_email, subject_line, html_message)



if __name__ == '__main__':
    # TODO Testing pending for trial renewal, set up 3-4 accounts with legit emails
    # Days remaining should be 6, 3, 1
    send_trial_renewal_email()




