import datetime
import ftplib
from datetime import date, timedelta
import os
import logging

def get_download_file_name(file_name, date):
    sl = file_name.split('.')
    ext = sl.pop()
    new_filename = F"{'.'.join(sl)}_{date.strftime('%d-%m-%Y')}.{ext}"
    return new_filename

def download_file(ftp: ftplib.FTP, remote_file_name, local_dir, target_date):
    local_file_name = get_download_file_name(remote_file_name, target_date)
    with open(os.path.join(local_dir, local_file_name), 'wb') as fp:
        ftp.retrbinary(F'RETR cmas.csv', fp.write) 
    return local_file_name

def download_cmots_dir(ftp_host, ftp_port, user_name, pwd, remote_dir, local_dir, target_date):
    return_filenames = []

    with ftplib.FTP(host=ftp_host, port=ftp_port, user=user_name, passwd=pwd) as ftp:
        ftp.cwd(remote_dir)
        local_file_name = download_file(ftp, "cmas.csv", local_dir, target_date)
        return_filenames.append(local_file_name)

        # Read the directory
        ftp_dir = F"{remote_dir}/{target_date.strftime('%d%m%Y')}"
        ftp.cwd(ftp_dir)
        remote_files = ftp.nlst()
        for remote_file in remote_files:
            local_file_name = download_file(ftp, remote_file, local_dir, target_date)
            return_filenames.append(local_file_name)

    return return_filenames

def download_cmots_file(ftp_host, ftp_port, user_name, pwd, remote_dir, local_dir, target_date, file_name):
    return_filename = None

    with ftplib.FTP(host=ftp_host, port=ftp_port, user=user_name, passwd=pwd) as ftp:
        if file_name == "cmas.csv":
            ftp.cwd(remote_dir)
            return_filename = download_file(ftp, "cmas.csv", local_dir, target_date)
        else:
            ftp_dir = F"{remote_dir}/{target_date.strftime('%d%m%Y')}"
            ftp.cwd(ftp_dir)
            remote_files = ftp.nlst()

            if file_name in remote_files:
                return_filename = download_file(ftp, file_name, local_dir, target_date)
            else:
                logging.warning(F"{file_name} for {target_date} was not found in {ftp_dir}")

    return return_filename

def scan_directory(local_dir, start_date, end_date, ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir):
    target_date = start_date - timedelta(days=1)

    keep_fetching = True
    while keep_fetching:
        target_date = target_date + timedelta(days=1)
        if target_date > end_date:
            keep_fetching = False
            break

        print(F"reading {target_date}")

        new_local_dir = os.path.join(local_dir, target_date.strftime("%Y%m%d"))
        os.makedirs(new_local_dir, exist_ok=True)

        download_cmots_dir(ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir, new_local_dir, target_date)

if __name__ == "__main__":
    ftp_host = '203.197.64.8'
    ftp_port = 21
    ftp_user = 'finalyca'
    ftp_pwd = "FiNa1LcA"
    remote_dir = "/finalycadata/"

    local_dir = 'C:\\_Finalyca\\cmots_data'

    # Read all the data from start date till end date. both dates are included
    start_date = date(2022, 3, 1)
    end_date = date(2022, 9, 14)
    scan_directory(local_dir, start_date, end_date, ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir)

    # Read one file for a particular date
    # Download files
    # filenames = list()
    # filenames.append('bseweight.csv')
    # filenames.append('nseweight.csv')
    # filenames.append('trireturns.csv')
    # filenames.append('dlyprice.csv')
    # filenames.append('latesteqcttm.csv')
    # target_date = date(2022, 9, 11)

    # attachement = list()
    # for filename in filenames:
    #     file_name = download_cmots_file(ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir, local_dir, target_date, filename)

    #     # attach as an email attachement 
    #     if file_name:
    #         attachement.append(file_name)

    #send the email
    # schedule_email_activity(db_session, recipients, '', '', subject, email_body, attachements)
    
# upload the files
