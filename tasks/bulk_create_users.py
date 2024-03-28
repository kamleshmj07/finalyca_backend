"""
TODO: Update the license count for the respective organization in db
TODO: Generate an permanent but common API key for the bulk create process
TODO: Update the database to setup phone number as not mandatory, but email mandatory
TODO: Update the admin user creation logic to only send otp if phone number exists
TODO: Set up this process as an intermittent task which can be triggered when necessary
TODO: Add email functionality to internal team for success and failure creations
"""

import requests as req
import pandas as pd
# from async_tasks.send_email import send_email_async


def bulk_create_users():
    # setting up configurations # TODO Move this to config file/set this as inputs, review naming of configurations
    organization_id = 10068
    file_path = r"C:\\dev\\releases\\Bulk Create Users\\TestUsers.xlsx"
    domain_to_validate = "finalyca.com" # set this as none to skip validation
    column_mapping = {"RM Name" : "name", "Email id" : "email"}
    base_url = "http://localhost:5100/"    # https://api-v1.finalyca.com/
    api_key = None # set this as none for running locally | eE7i4vQS9k24fUZuWsHsGo9LmE5y3gJNNWaKJGPZE

    # prep url and headers based on inputs
    url = F"{base_url}access/org/user?organization_id={organization_id}"
    print(url)
    headers = { "X_API_Key" : api_key } if api_key else {}

    # read the file and convert to dictionary of records
    df_users = pd.read_excel(file_path)
    new_column_names = [column_mapping[x] if x in column_mapping else x for x in df_users.columns]
    df_users.columns = new_column_names

    user_info_records = df_users.to_dict(orient='records')

    # store success and failure records
    success = []
    failure = []

    for user in user_info_records:
        print(user)
        try:
            # Step 1: validate the email id with it's domain
            if domain_to_validate:
                print("before validation of email")
                valid_email = validate_email(user["email"], domain_to_validate)
                print(F"Email Valid ? {valid_email}")
                if not valid_email:
                    failure.append(user) # if unsuccessful then add to failure list
                    continue

            # Step 2: prepare the form data for the post request
            form_data = {
                            "name": user["name"],
                            "email": user["email"].trim(),
                            "status":1
                        }

            # Step 3: call the api with post call
            response = None
            with req.Session() as s:
                response = s.post(url, headers=headers, data=form_data)

            # Step 4: if user is created successfully then add to success list, other wise add to failure list
            if response.status_code == 200:
                success.append(user)
            else:
                # print(response.text)
                user["error"] = dict(response.json()).get("description")
                failure.append(user)
        except Exception as ex:
            print(F'Exception : {ex}')
            failure.append(user)
            continue
        
    # print(" Success List : ", success)
    # print(" Failure List : ", failure)
    pd.DataFrame(success).to_csv("success_list.csv")
    pd.DataFrame(failure).to_csv("failure_list.csv")

    # TODO: in future, we can prepare one excel with 2 sheets, success and failure. Then send email to notify



def validate_email(email, domain):
    """
    validates email id for the respective domain
    """
    domain_of_email = email.split('@')[1]
    if domain_of_email.lower() == domain.lower():
        return True

    return False



if __name__ == '__main__':
    bulk_create_users()

