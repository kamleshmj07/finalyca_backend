# sebi_scrapper

This project will scan the SEBI Website for the following information and store it in relevant data dumps.

# Important Files:
- scrapper.py: This will scrap the Sebi website for PMS information and save it to database
- server.py: This will provide API service to the scrapped data.

# Scrapper Operation
- regular: Scrapper will check AMCInfo table to find the last successful date for the import. Scrapper will try to read data after that. The status will be updated in AMCScrapperStatus table.

# Possible Analytics:
- Make sure all AMCScrapperStatus entries have to_ignore bit set when the AMC registration date is higher than as of date
- If a scheme has 0 AUM for consistent 3 months, make is_active to False. This will be imported, but not included in the web content.
- If an AMC has 0 AUM for consistent 3 months, make is_active to False. This will be imported, but not included in the web content.

# Exporting mysql database with data
mysqldump --databases sebi_pms -u finalyca -p123456 > dump.sql

# Setting up MS SQL
```
CREATE DATABASE SEBI_PMS
use SEBI_PMS
CREATE USER [finalyca] FOR LOGIN [finalyca]
```