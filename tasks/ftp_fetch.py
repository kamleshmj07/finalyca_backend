import datetime
import ftplib
from datetime import date, timedelta
import os
from posixpath import split
from utils import unzip

def download_cmots_files(remote_host, ftp_port, user_name, pwd, remote_dir, local_dir, target_date, file_name):
    ftp = ftplib.FTP()
    ftp.connect(host=remote_host, port=ftp_port)
    ftp.login(user=user_name, passwd=pwd)
        
    # ftp_dir = F"{remote_dir}/{target_date.strftime('%d%m%Y')}"
    ftp.cwd(remote_dir)

    remote_files = ftp.nlst()
    for remote_file in remote_files:
        print(remote_file)

    idx = file_name.index('.')
    new_filename = file_name[:idx] + '_' + str(target_date.strftime('%d-%m-%Y-%H-%M-%S')) + file_name[idx:]
    return_filename = None

    with open(os.path.join(local_dir, new_filename), 'wb') as fp:
        ftp.retrbinary(F'RETR {file_name}', fp.write)   
        return_filename = new_filename
    ftp.quit()

    return return_filename


def extract_bilav_files(local_dir, target_date, file_name):
    try:
        zip_file_path = os.path.join(local_dir, file_name)
        zip_file_dir = os.path.dirname(zip_file_path)
        unzip_file_name = unzip(zip_file_path, zip_file_dir)[0]
        from_directory = os.path.join(zip_file_dir,unzip_file_name)
        to_directory = os.path.join(local_dir, target_date.strftime('%d_%m_%Y'))
        if not os.path.exists(to_directory):
            os.makedirs(to_directory)

        to_directory = os.path.join(to_directory, unzip_file_name)
        os.replace(from_directory, to_directory)

        return to_directory
    except Exception as ex:
        raise ex


if __name__ == "__main__":
# Download files
    filenames = list()
    filenames.append('bseweight.csv')
    filenames.append('nseweight.csv')
    filenames.append('trireturns.csv')
    filenames.append('dlyprice.csv')
    filenames.append('latesteqcttm.csv')

    attachement = list()
    for filename in filenames:
        target_date = date(2022, 8, 23)
        ftp_host = '203.197.64.8'
        ftp_port = 21
        ftp_user = 'finalyca'
        ftp_pwd = "FiNa1LcA"
        remote_dir = F"/finalycadata/{target_date.strftime('%d%m%Y')}/{filename}"
        local_dir = 'D:\\finalyca_dev\\backend\\cmot_data\\'

        file_name = download_cmots_files(ftp_host, ftp_port, ftp_user, ftp_pwd, remote_dir, local_dir, target_date, filename)

        # attach as an email attachement 
        if file_name:
            attachement.append(file_name)

    #send the email
    # schedule_email_activity(db_session, recipients, '', '', subject, email_body, attachements)
    
# upload the files
