import yaml
from sebi_lib.models import Base 
from sebi_lib.sebi_parsing import *
from sebi_lib.utils import setup_file_logger
from sebi_lib.utils import LinuxRunnable
from sebi_lib.sebi_isin import scrap_sebi_isin

def main(log_dir, html_backup_dir, create_sql_tables):
    app_name = "sebi_pms_scrapper"
    # runnable = LinuxRunnable(config['APP_NAME'])

    setup_file_logger(app_name, log_dir)
    logging.info(F"{app_name} has started.")

    url = mssql_prod_uri(True, "SEBI_PMS")

    if create_sql_tables:
        create_tables(url, Base)

    db_session = get_database_scoped_session(url)
    # db_session = get_db_session(url, Base, True)

    # Scrap Sebi website for PMS Content
    scrap_whole_website(db_session, html_backup_dir, False)

    # Check for missing data based on AMCScrapperStatus
    # check_missing_data(db_session, html_backup_dir, False)

    # check for ISIN status once a day (defined in the function)
    # scrap_sebi_isin(db_session, False)

    logging.info(F"{app_name} has finished.")

if __name__ == '__main__':
    log_dir = ""
    html_backup_dir = "scrap/"
    create_sql_tables = True
    
    main(log_dir, html_backup_dir, create_sql_tables)