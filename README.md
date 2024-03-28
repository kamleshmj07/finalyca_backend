# backend
This project contains source code for the backend server.

## First Time Install
- Install Python 3.10 for windows from the [python website](https://www.python.org/downloads/). 
- on command line, run `python -m venv venv` to create a virtual environment
- Make sure the virtual environment python interpreter is selected for the project in VS Code.
- Install current list of packages by running following command.
``` python
pip install -r requirements.txt
```

## Installing finalyca libraries
- clone the finalyca lib repository with personal credentials. following is for production version.
``` sh
git clone https://deploy:oJfHVyFPT_Ea3xgX32Zp@gitlab.com/f2504/backend.git
```

- go to the directory where it is installed and execute following command
``` python
pip install -e . 
```
> Do not miss the dot in the end. it means current working directory.

## Selecting libraries
- MS SQL Drivers: [pyodbc](https://mkleehammer.github.io/pyodbc/)

    based on [docs](https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-ver15), this is the preferred driver. It will require *Microsoft Visual C++ 14.0 or greater* from **Microsot C++ Build Tools**. 

## Genering SQL Models
- Install sqlacodegen to auto generate models
` pip install sqlacodegen==3.0.0b2 `
Because of the [bug](https://github.com/agronholm/sqlacodegen/issues/131) in the default version, a specific version is installed rather than default version.
- Once installed, run following command to generate models.
`sqlacodegen.exe 'mssql+pymssql://DESKTOP-RHOETVN/PMS_Base' --schema Transactions > transaction_models.py `
Server name must be replaced with *DESKTOP-RHOETVN* and database name must be replaced with *PMS_Base*.


### Generating MS SQL scripts from ORM Classes
``` python
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import mssql 
from fin_models.controller_transaction_models import *
print(CreateTable(Investor.__table__).compile(dialect=mssql.dialect()))
print(CreateTable(InvestorDemat.__table__).compile(dialect=mssql.dialect()))
print(CreateTable(InvestorHoldings.__table__).compile(dialect=mssql.dialect()))
print(CreateTable(InvestorRecommendation.__table__).compile(dialect=mssql.dialect()))
```

## Setting up SQL database on local windows system
``` sql
-- Enabling Database to use SQL Server and Windows authentication modes.
USE [master]
GO
EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'LoginMode', REG_DWORD, 2
GO

USE [master]
GO
-- Create user identical to the production server and adding to databases
CREATE LOGIN [finalyca] WITH PASSWORD=N'F!n@lyca', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [finalyca]
GO
USE [PMS_Base]
GO
CREATE USER [finalyca] FOR LOGIN [finalyca]
GO
USE [PMS_Controller]
GO
CREATE USER [finalyca] FOR LOGIN [finalyca]
GO
USE [PMS_Logger]
GO
CREATE USER [finalyca] FOR LOGIN [finalyca]
GO
USE [ServiceManager]
GO
CREATE USER [finalyca] FOR LOGIN [finalyca]
GO
```

- Now restart MS SQL service and backend should be able to connect to local database with uid and pwd.


###Install PDF generation dependencies 
pip install jinja2 --- (if not exists)
pip install weasyprint
pip install pygal

download GTK3 from below url
https://github.com/tschoonj/GTK-for-Windows-Runtime-Environment-Installer/releases

set environment variable - path of bin folder - example - C:\Program Files\GTK3-Runtime Win64\bin
