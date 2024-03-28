from datetime import date, datetime, timedelta
from time import strptime
import requests

from utils.time_func import last_date_of_month

def getbetweendate(months =0, years=0, transactiondate=None, isstartdate=True):
    yr = transactiondate[0]
    mn = transactiondate[1]
    day = transactiondate[2]
    
    if years>0:
        yr = yr - years
        if mn == 12:
            mn = 1
            yr = yr + 1
        else:
            mn = mn + 1
        
    if months>0:
        if months < mn:
            mn = mn - months
        elif months == mn:
            mn = 12
            yr = yr - 1
        elif months > mn:
            mn = (12 +(mn - months))
            yr = yr - 1

    if isstartdate:
        dt = datetime(yr, mn,1)
    else:
        dt = last_date_of_month(yr, mn)
    return dt



plans = list()
periods = list()

plans.append("7")
periods.append("1M")
# periods.append("3M")
# periods.append("6M")
# periods.append("1Y")
# periods.append("3Y")

asondate =strptime("2021-11-30", '%Y-%m-%d')  
todate = datetime.strptime("2021-11-30", '%Y-%m-%d')
for plan in plans:
    for period in periods:
        fromdate = ""
        if period == "1M":
            fromdate = getbetweendate(0,0,asondate) - timedelta(days = 1)
        elif period == "3M":
            fromdate = getbetweendate(2,0,asondate) - timedelta(days = 1)
        elif period == "6M":
            fromdate = getbetweendate(5,0,asondate) - timedelta(days = 1)
        elif period == "1Y":
            fromdate = getbetweendate(0,1,asondate) - timedelta(days = 1)
        elif period == "3Y":
            fromdate = getbetweendate(0,3,asondate) - timedelta(days = 1)
        
        f = {
        "RequestStatus": 1,
        "RequestMessage": "Success",
        "RequestAuthorization": {
            "Token": "py2JQJDF8qP/UeaKjbsVUWRZjIYVQLW+3U54e1uj/Hs=",
            "Application_Id": 1,
            "User_Id": 0,
            "Role_Id": 0,
            "Controller": "Gsquare",
            "Method": "M07"
        },
        "RequestObject": {
            "Plan_Id": plan,
            "BenchmarkIndices_Id": 242,
            "Period": "1M",
            "Dates": "[\"" + str(fromdate.strftime('%Y-%m-%d')) + "\",\""+ str(todate.strftime('%Y-%m-%d')) +"\"]",
            "User_Name": "X8tP/yI2/QomqPeXTV0AVw==",
            "Password": "Iwoh0pJDJv8TTUfxPPTMYg=="
        }
        }
 
        r = requests.post('https://api.finalyca.com/api_layer/Gsquare/M07', json=f)
        # http://localhost:30011/api_layer/Gsquare/M07
        print(r.status_code)
        print(r.json())

