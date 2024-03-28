ALTER TABLE [ServiceManager].[Reporting].[ReportJobs] ADD [Enabled_Python] BIT NOT NULL DEFAULT '0';

ALTER TABLE [ServiceManager].[Upload].[UploadTemplates] ADD [Enabled_Python] BIT NOT NULL DEFAULT '0';

INSERT INTO [Reporting].[ReportSchedules]
([Schedule_Name],[Schedule_Description],[Start_Date],[End_Date],[Type],[Frequency],[Pickup_Hours],[Pickup_Minutes],[Enabled])
VALUES
('Sebi Scrapper','Scheduling for sebi data scrap','2022-07-14', NULL,'daily', 1, 00, 30, 1 )

INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Sebi Data Scrapper','This job will scrap data from SEBI',5,'','<Parameters><ImportType>WEBSCRAP</ImportType><HTML_Backup_Dir>C:\_Finalyca\_finalyca\backend\logs\html\</HTML_Backup_Dir><Log_Dir>C:\_Finalyca\_finalyca\backend\logs\</Log_Dir><Export_Dir>C:\_Finalyca\_finalyca\backend\logs\</Export_Dir><Table_Create>0</Table_Create><SendEmail>YES</SendEmail><Recipients>vijay.shah@finalyca.com,sachin.jaiswal@finalyca.com</Recipients><Subject>SEBI Data</Subject><Body /></Parameters>',0,0,'Pending','2022-04-01',(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Sebi Scrapper'),1,2);

UPDATE [ServiceManager].[Upload].[UploadTemplates] SET Enabled_Python = 1, Status = 0 where UploadTemplates_Name Like 'PMS%';


--json
INSERT INTO [Reporting].[ReportSchedules]
([Schedule_Name],[Schedule_Description],[Start_Date],[End_Date],[Type],[Frequency],[Pickup_Hours],[Pickup_Minutes],[Enabled])
VALUES
('Sebi Scrapper Website','Scheduling for sebi data scrap','2022-09-01', NULL,'weekly', 1, 5, 0, 1 )



INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Sebi Scrapper Data - json','This job will send the SEBI data in json format.',7,'',
'<Parameters><ImportType>WEBSCRAP</ImportType><HTML_Backup_Dir>C:\_Finalyca\_finalyca\backend\logs\html\</HTML_Backup_Dir><Log_Dir>C:\_Finalyca\_finalyca\backend\logs\</Log_Dir><Export_Dir>C:\_Finalyca\_finalyca\backend\logs\</Export_Dir><Table_Create>0</Table_Create><SendEmail>YES</SendEmail><Recipients>vijay.shah@finalyca.com,sachin.jaiswal@finalyca.com,ibrahim.saifuddin@finalyca.com,nirmit.shah@finalyca.com</Recipients><Subject>SEBI Data</Subject><Body /></Parameters>',
0,0,'Pending','2022-04-01',(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Sebi Scrapper Website'),1,2);


--csv
INSERT INTO [Reporting].[ReportSchedules]
([Schedule_Name],[Schedule_Description],[Start_Date],[End_Date],[Type],[Frequency],[Pickup_Hours],[Pickup_Minutes],[Enabled])
VALUES
('Sebi Scrapper export','Scheduling for sebi data scrap','2022-09-01', NULL,'weekly', 1, 5, 0, 1 )



INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Sebi Scrapper Data - csv','This job will send the SEBI data in csv format.',6,'',
'<Parameters><ImportType>WEBSCRAP</ImportType><HTML_Backup_Dir>C:\_Finalyca\_finalyca\backend\logs\html\</HTML_Backup_Dir><Log_Dir>C:\_Finalyca\_finalyca\backend\logs\</Log_Dir><Export_Dir>C:\_Finalyca\_finalyca\backend\logs\</Export_Dir><Table_Create>0</Table_Create><SendEmail>YES</SendEmail><Recipients>vijay.shah@finalyca.com,sachin.jaiswal@finalyca.com,philip.shah@finalyca.com,support@finalyca.com</Recipients><Subject>SEBI Data</Subject><Body /></Parameters>',
0,0,'Pending','2022-04-01',(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Sebi Scrapper export'),1,2);



update a set a.Parameters = '<Parameters>
  <ImportType>FTP</ImportType>
  <FTPURL>203.197.64.8</FTPURL>
  <FTPPORT>21</FTPPORT>
  <FTPUserId>finalyca</FTPUserId>
  <FTPPassword>FiNa1LcA</FTPPassword>
  <ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
  <ImportFileName>/finalycadata/@ddmmyyyy/nseweight.csv</ImportFileName>
  <FileHeaders>CO_CODE|WDATE|INDEXCODE|CLOSEPRICE|NOOFSHARES|FULLMCAP|FF_ADJFACTOR|FF_MCAP|WEIGHT_INDEX|FLAG</FileHeaders>
  <ColumnSeperator>|</ColumnSeperator>
  <RowSeperator>\n</RowSeperator>
  <IndexType>NSE</IndexType>
  <DaysCount>-1</DaysCount>
  <SendEmail>YES</SendEmail>
  <Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
  <RecipientsCC>danielgm@pmsbazaar.com,rpallavarajan@pmsbazaar.com,finalyca.product@gmail.com</RecipientsCC>
  <RecipientsBCC>it@pmsbazaar.com</RecipientsBCC>
  <Subject>CMOTS Upload - NSE Weight</Subject>
  <Body> CMOTS Upload - NSE Weight Report</Body>
  <IsBodyHTML>1</IsBodyHTML>
</Parameters>', Enabled=0, Enabled_Python = 1 from Reporting.ReportJobs a where Job_Id=7

update a set a.Parameters = '<Parameters>
  <ImportType>FTP</ImportType>
  <FTPURL>203.197.64.8</FTPURL>
  <FTPPORT>21</FTPPORT>
  <FTPUserId>finalyca</FTPUserId>
  <FTPPassword>FiNa1LcA</FTPPassword>
  <ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
  <ImportFileName>/finalycadata/@ddmmyyyy/trireturns.csv</ImportFileName>
  <FileHeaders>Exchange|IndexCode|IndexName|IndexDate|Ret1W|Ret1M|Ret3M|Ret6M|Ret1year|Ret3Year|baseindexcode</FileHeaders>
  <ColumnSeperator>|</ColumnSeperator>
  <RowSeperator>\n</RowSeperator>
  <DaysCount>-1</DaysCount>
  <SendEmail>YES</SendEmail>
  <Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
  <RecipientsCC>danielgm@pmsbazaar.com,rpallavarajan@pmsbazaar.com,finalyca.product@gmail.com</RecipientsCC>
  <RecipientsBCC>it@pmsbazaar.com</RecipientsBCC>
  <Subject>CMOTS Upload - TRI Returns</Subject>
  <Body>CMOTS Upload - TRI Returns Report</Body>
  <IsBodyHTML>1</IsBodyHTML>
</Parameters>', Enabled=0, Enabled_Python = 1 from Reporting.ReportJobs a where Job_Id=5

update a set a.Parameters = '<Parameters>
  <ImportType>FTP</ImportType>
  <FTPURL>203.197.64.8</FTPURL>
  <FTPPORT>21</FTPPORT>
  <FTPUserId>finalyca</FTPUserId>
  <FTPPassword>FiNa1LcA</FTPPassword>
  <ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
  <ImportFileName>/finalycadata/@ddmmyyyy/dlyprice.csv</ImportFileName>
  <FileHeaders>SC_CODE|DATE|ST_EXCHNG|CO_CODE|HIGH|LOW|OPEN|CLOSE|TDCLOINDI|VOLUME|NO_TRADES|NET_TURNOV|FLAG</FileHeaders>
  <ColumnSeperator>|</ColumnSeperator>
  <RowSeperator>\n</RowSeperator>
  <DaysCount>-1</DaysCount>
  <SendEmail>YES</SendEmail>
  <Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
  <RecipientsCC>danielgm@pmsbazaar.com,rpallavarajan@pmsbazaar.com,finalyca.product@gmail.com</RecipientsCC>
  <RecipientsBCC>it@pmsbazaar.com</RecipientsBCC>
  <Subject>CMOTS Upload - Daily Price</Subject>
  <Body>CMOTS Upload - TRI Returns Report</Body>
  <IsBodyHTML>1</IsBodyHTML>
</Parameters>', Enabled=0, Enabled_Python = 1 from Reporting.ReportJobs a where Job_Id=6

update a set a.Parameters = '<Parameters>
  <ImportType>FTP</ImportType>
  <FTPURL>203.197.64.8</FTPURL>
  <FTPPORT>21</FTPPORT>
  <FTPUserId>finalyca</FTPUserId>
  <FTPPassword>FiNa1LcA</FTPPassword>
  <ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
  <ImportFileName>/finalycadata/@ddmmyyyy/bseweight.csv</ImportFileName>
  <FileHeaders>CO_CODE|WDATE|INDEXCODE|CLOSEPRICE|NOOFSHARES|FULLMCAP|FF_ADJFACTOR|FF_MCAP|WEIGHT_INDEX|FLAG</FileHeaders>
  <ColumnSeperator>|</ColumnSeperator>
  <RowSeperator>\n</RowSeperator>
  <IndexType>BSE</IndexType>
  <DaysCount>-1</DaysCount>
  <SendEmail>YES</SendEmail>
  <Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
  <RecipientsCC>danielgm@pmsbazaar.com,rpallavarajan@pmsbazaar.com,finalyca.product@gmail.com</RecipientsCC>
  <RecipientsBCC>it@pmsbazaar.com</RecipientsBCC>
  <Subject>CMOTS Upload - BSE Weight</Subject>
  <Body>CMOTS Upload - BSE Weight Report</Body>
  <IsBodyHTML>1</IsBodyHTML>
</Parameters>', Enabled=0, Enabled_Python = 1 from Reporting.ReportJobs a where Job_Id=8

update a set a.Parameters = '<Parameters>
  <ImportType>FTP</ImportType>
  <FTPURL>203.197.64.8</FTPURL>
  <FTPPORT>21</FTPPORT>
  <FTPUserId>finalyca</FTPUserId>
  <FTPPassword>FiNa1LcA</FTPPassword>
  <ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
  <ImportFileName>/finalycadata/@ddmmyyyy/latesteqcttm.csv</ImportFileName>
  <FileHeaders>CO_CODE|PriceDate|PE|EPS|DivYield|PBV|mcap</FileHeaders>
  <ColumnSeperator>|</ColumnSeperator>
  <RowSeperator>\n</RowSeperator>
  <DaysCount>-1</DaysCount>
  <SendEmail>YES</SendEmail>
  <Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
  <RecipientsCC>danielgm@pmsbazaar.com,rpallavarajan@pmsbazaar.com,finalyca.product@gmail.com</RecipientsCC>
  <RecipientsBCC>it@pmsbazaar.com</RecipientsBCC>
  <Subject>CMOTS Upload - Latest EQ</Subject>
  <Body>CMOTS Upload - Latest EQ Report</Body>
  <IsBodyHTML>1</IsBodyHTML>
</Parameters>', Enabled=0, Enabled_Python = 1 from Reporting.ReportJobs a where Job_Id=9


INSERT INTO [Reporting].[ReportSchedules]
([Schedule_Name],[Schedule_Description],[Start_Date],[End_Date],[Type],[Frequency],[Pickup_Hours],[Pickup_Minutes],[Enabled])
VALUES
('Finalyca Report Triggers - python','Finalyca Report Triggers',getdate(), NULL,'daily', 1, 5, 0, 1 )


-- TODO change Report_Type from 8 to 2
INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Finalyca Business Health Check - python','Finalyca Business Health Check',8,'',
'<Parameters>
  <ReportOutputPath>E:\\Finalyca\\api\\Reports\\</ReportOutputPath>
  <ReportFileName>Finalyca Data Health Check Report</ReportFileName>
  <ReportHeader>Finalyca Data Health Check Report</ReportHeader>
  <ReportWorksheetName>Underlying Holdings#Sum of Underlying Holdings not equal to 100%#Last FactSheet Date is less that Last Underlying Holding Date#Market Cap Composition (%) = 0#Investment Style (%) = 0#FactSheet#Fund NAV#Fund Manager not available#Benchmark NAV#Bechmark Not Available#List of Equity Securities where Market Cap is blank#List of Equity Securities where Sector is blank#List of Equity Securities where Investment style is blank#PMS - Portfolio returns for last 2 End of Month Fund NAV#List of plans where NAV change is more than 3 percent#List of plans where NAV change is more than 10 percent#List of sectors where change is more than 5 percent#List of plans where attribution tab is not available#List of NAV where change is more than 3 percent since inception daily - generated every sunday#List of NAV where change is more than 10 percent since inception monthly - generated every sunday#List of user whose account will expire in 45 days.#List of user login count for last month - generated every sunday</ReportWorksheetName>
  <SendEmail>YES</SendEmail>
  <Recipients>sachin.jaiswal@finalyca.com,vijay.shah@finalyca.com</Recipients>
  <RecipientsCC></RecipientsCC>
  <RecipientsBCC />
  <Subject>Finalyca Business Health Check Report</Subject>
  <Body>
    <html>
      <span style="color:green">Finalyca Business Health Check report</span>
    </html>
  </Body>
  <IsBodyHTML>1</IsBodyHTML>
</Parameters>',
0,0,'Pending',getdate(),(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Finalyca Report Triggers - python'),1,2);

INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Finalyca Data Health Check - python','Finalyca data Health Check',8,'',
'<Parameters>
  <ReportOutputPath>E:\\Finalyca\\api\\Reports\\</ReportOutputPath>
  <ReportFileName>Finalyca Data Health Check Report</ReportFileName>
  <ReportHeader>Finalyca Data Health Check Report</ReportHeader>
  <ReportWorksheetName></ReportWorksheetName>
  <SendEmail>YES</SendEmail>
  <Recipients>sachin.jaiswal@finalyca.com</Recipients>
  <RecipientsCC></RecipientsCC>
  <RecipientsBCC />
  <Subject>Finalyca Data Health Check Report</Subject>
  <Body>
    <html>
      <span style="color:green">Finalyca Data Health Check report</span>
    </html>
  </Body>
  <IsBodyHTML>1</IsBodyHTML>
</Parameters>',
0,0,'Pending',getdate(),(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Finalyca Report Triggers - python'),1,2);


-- fundstock and fundscreener sql Job migration to python job
INSERT INTO [Reporting].[ReportSchedules]
([Schedule_Name],[Schedule_Description],[Start_Date],[End_Date],[Type],[Frequency],[Pickup_Hours],[Pickup_Minutes],[Enabled])
VALUES
('Execute Python function','Execute python function mentioned in xml','2022-11-07', NULL,'daily', 1, 1, 00, 1 )

INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Fundstock generation','This job will execute fundstock generation function.',9,'',
'<Parameters><ImportType>FunctiontoExecute</ImportType><FunctionName>prepare_fund_stocks_table</FunctionName></Parameters>',0,1,'Success','2022-11-06',(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Execute Python function'),1,2);

INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Fundscreener generation','This job will execute fundscreener generation function.',9,'',
'<Parameters><ImportType>FunctiontoExecute</ImportType><FunctionName>prepare_fund_screener</FunctionName></Parameters>',0,1,'Success','2022-11-06',(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Execute Python function'),1,2);

INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Fundscreener generation','This job will execute generate fund manager details function.',9,'',
'<Parameters><ImportType>FunctiontoExecute</ImportType><FunctionName>generate_fundmanager_details</FunctionName></Parameters>',0,1,'Success','2022-11-29',(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Execute Python function'),1,2);


INSERT INTO [Reporting].[ReportSchedules]
([Schedule_Name],[Schedule_Description],[Start_Date],[End_Date],[Type],[Frequency],[Pickup_Hours],[Pickup_Minutes],[Enabled])
VALUES
('Re-build sql index','will re-build sql indexes','2022-12-18', NULL,'weekly', 1, 1, 00, 1 )

INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Re-build sql index','This job will Re-build sql index of tables which is mentioned in parameters.',10,'',
'<Parameters>
	<Script>ALTER INDEX ALL ON ##tablename## REBUILD</Script>
  <TableNames>Transactions.NAV, Masters.Plans</TableNames>
</Parameters>',0,1,'Success','2022-12-18',
(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Re-build sql index'),0,2);


INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Update security Equity style','This job will Update security Equity style.',9,'',
'<Parameters><ImportType>FunctiontoExecute</ImportType><FunctionName>update_equity_style</FunctionName></Parameters>',0,1,'Success','2022-11-29',
(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Execute Python function'),1,2);

--CMOTS Upload - trireturns Master - Month end
INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('CMOTS Upload - trireturns Master - Month end','CMOTS Upload - trireturns Master - Month end',3,'',
'<Parameters>
  <ImportType>FTP</ImportType>
  <FTPURL>203.197.64.8</FTPURL>
  <FTPPORT>21</FTPPORT>
  <FTPUserId>finalyca</FTPUserId>
  <FTPPassword>FiNa1LcA</FTPPassword>
  <ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
  <ImportFileName>/finalycadata/@ddmmyyyy/trireturns_monthend.csv</ImportFileName>
  <FileHeaders>Exchange|IndexCode|IndexName|IndexDate|Ret1W|Ret1M|Ret3M|Ret6M|Ret1year|Ret3Year|baseindexcode</FileHeaders>
  <ColumnSeperator>|</ColumnSeperator>
  <RowSeperator>\n</RowSeperator>
  <DaysCount>-1</DaysCount>
  <SendEmail>YES</SendEmail>
  <Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
  <RecipientsCC>danielgm@pmsbazaar.com,rpallavarajan@pmsbazaar.com,finalyca.product@gmail.com</RecipientsCC>
  <RecipientsBCC>it@pmsbazaar.com</RecipientsBCC>
  <Subject>CMOTS Upload - TRI Returns - Month end</Subject>
  <Body>CMOTS Upload - TRI Returns - Month end Report</Body>
  <IsBodyHTML>1</IsBodyHTML>
</Parameters>',
0,0,'Pending','2023-03-27',4,0,2);



-- ULIP jobs

--ULIP Scheme Master
INSERT 
INTO [Reporting].[ReportJobs] ([Job_Name],
							   [Job_Description],
							   [Report_Type],
							   [Report_Object],
							   [Parameters],
							   [Enabled],
							   [Status],
							   [Status_Message],
							   [Last_Run],
							   [Schedule_Id],
							   [Enabled_Python],
							   [Channel_Id])
VALUES	('CMOTS Upload - ULIP_scheme_master',
		 'CMOTS Upload - ULIP_scheme_master',
		 3,
		 '',
		 '<Parameters>
			<ImportType>FTP</ImportType>
			<FTPURL>203.197.64.8</FTPURL>
			<FTPPORT>21</FTPPORT>
			<FTPUserId>finalyca</FTPUserId>
			<FTPPassword>FiNa1LcA</FTPPassword>
			<ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
			<ImportFileName>/finalycadata/@ddmmyyyy/SchemeMasterUpdate_Insurance.csv</ImportFileName>
			<FileHeaders></FileHeaders>
			<ColumnSeperator>|</ColumnSeperator>
			<RowSeperator>\n</RowSeperator>
			<DaysCount>-1</DaysCount>
			<SendEmail>YES</SendEmail>
			<Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
			<RecipientsCC>sachin.jaiswal@finalyca.com</RecipientsCC>
			<RecipientsBCC></RecipientsBCC>
			<Subject>CMOTS Upload - ULIP Scheme Master</Subject>
			<Body>CMOTS Upload - ULIP Scheme Master</Body>
			<IsBodyHTML>1</IsBodyHTML>
		  </Parameters>',
		  0,
		  2,
		  'Success',
		  '2022-12-18',
		  4,
		  0,
		  2);


-- ULIP NAV
INSERT 
INTO [Reporting].[ReportJobs] ([Job_Name],
							   [Job_Description],
							   [Report_Type],
							   [Report_Object],
							   [Parameters],
							   [Enabled],
							   [Status],
							   [Status_Message],
							   [Last_Run],
							   [Schedule_Id],
							   [Enabled_Python],
							   [Channel_Id])
VALUES	('CMOTS Upload - ULIP_nav',
		 'CMOTS Upload - ULIP_nav',
		 3,
		 '',
		 '<Parameters>
			<ImportType>FTP</ImportType>
			<FTPURL>203.197.64.8</FTPURL>
			<FTPPORT>21</FTPPORT>
			<FTPUserId>finalyca</FTPUserId>
			<FTPPassword>FiNa1LcA</FTPPassword>
			<ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
			<ImportFileName>/finalycadata/@ddmmyyyy/NavUpdateInsurance.csv</ImportFileName>
			<FileHeaders></FileHeaders>
			<ColumnSeperator>|</ColumnSeperator>
			<RowSeperator>\n</RowSeperator>
			<DaysCount>-1</DaysCount>
			<SendEmail>YES</SendEmail>
			<Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
			<RecipientsCC>sachin.jaiswal@finalyca.com</RecipientsCC>
			<RecipientsBCC></RecipientsBCC>
			<Subject>CMOTS Upload - ULIP NAV</Subject>
			<Body>CMOTS Upload - ULIP NAV</Body>
			<IsBodyHTML>1</IsBodyHTML>
		  </Parameters>',
		  0,
		  2,
		  'Success',
		  '2022-12-18',
		  4,
		  0,
		  2);


--ULIP Holdings
INSERT 
INTO [Reporting].[ReportJobs] ([Job_Name],
							   [Job_Description],
							   [Report_Type],
							   [Report_Object],
							   [Parameters],
							   [Enabled],
							   [Status],
							   [Status_Message],
							   [Last_Run],
							   [Schedule_Id],
							   [Enabled_Python],
							   [Channel_Id])
VALUES	('CMOTS Upload - ULIP_holdings',
		 'CMOTS Upload - ULIP_holdings',
		 3,
		 '',
		 '<Parameters>
			<ImportType>FTP</ImportType>
			<FTPURL>203.197.64.8</FTPURL>
			<FTPPORT>21</FTPPORT>
			<FTPUserId>finalyca</FTPUserId>
			<FTPPassword>FiNa1LcA</FTPPassword>
			<ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
			<ImportFileName>/finalycadata/@ddmmyyyy/HoldingsUpdateInsurance.csv</ImportFileName>
			<FileHeaders></FileHeaders>
			<ColumnSeperator>|</ColumnSeperator>
			<RowSeperator>\n</RowSeperator>
			<DaysCount>-1</DaysCount>
			<SendEmail>YES</SendEmail>
			<Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
			<RecipientsCC>sachin.jaiswal@finalyca.com</RecipientsCC>
			<RecipientsBCC></RecipientsBCC>
			<Subject>CMOTS Upload - ULIP Holdings</Subject>
			<Body>CMOTS Upload - ULIP Holdings</Body>
			<IsBodyHTML>1</IsBodyHTML>
		  </Parameters>',
		  0,
		  2,
		  'Success',
		  '2022-12-18',
		  4,
		  0,
		  2);



--ULIP Factsheet
INSERT 
INTO [Reporting].[ReportJobs] ([Job_Name],
							   [Job_Description],
							   [Report_Type],
							   [Report_Object],
							   [Parameters],
							   [Enabled],
							   [Status],
							   [Status_Message],
							   [Last_Run],
							   [Schedule_Id],
							   [Enabled_Python],
							   [Channel_Id])
VALUES	('CMOTS Upload - ULIP_factsheet',
		 'CMOTS Upload - ULIP_factsheet',
		 3,
		 '',
		 '<Parameters>
			<ImportType>FTP</ImportType>
			<FTPURL>203.197.64.8</FTPURL>
			<FTPPORT>21</FTPPORT>
			<FTPUserId>finalyca</FTPUserId>
			<FTPPassword>FiNa1LcA</FTPPassword>
			<ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
			<ImportFileName>/finalycadata/@ddmmyyyy/FactSheetInsurance.csv</ImportFileName>
			<FileHeaders></FileHeaders>
			<ColumnSeperator>|</ColumnSeperator>
			<RowSeperator>\n</RowSeperator>
			<DaysCount>-1</DaysCount>
			<SendEmail>YES</SendEmail>
			<Recipients>Laijin@pmsbazaar.com,support@finalyca.com</Recipients>
			<RecipientsCC>sachin.jaiswal@finalyca.com</RecipientsCC>
			<RecipientsBCC></RecipientsBCC>
			<Subject>CMOTS Upload - ULIP Factsheet</Subject>
			<Body>CMOTS Upload - ULIP Factsheet</Body>
			<IsBodyHTML>1</IsBodyHTML>
		  </Parameters>',
		  0,
		  2,
		  'Success',
		  '2022-12-18',
		  4,
		  0,
		  2);



		  --select * from [Reporting].[ReportJobs] where Job_Id in (35,36,37,38)
		  --update a set Enabled_Python=1 from [Reporting].[ReportJobs] a where Job_Id in (35,36,37,38)


		  --Fill nav gap job
		  INSERT INTO [Reporting].[ReportJobs]
		([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
		VALUES
		('Fill nav gap for MF and ULIP','This job will fill nav gap for MF and ULIP.',9,'',
		'<Parameters><ImportType>FunctiontoExecute</ImportType><FunctionName>mf_ulip_fill_missing_nav</FunctionName></Parameters>',0,1,'Success','2022-11-29',
		(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Execute Python function'),0,2);


/**************************** PMS & AIF Common Holdings Template *****************************/
USE [ServiceManager]
GO

INSERT INTO [Upload].[UploadTemplates]
           ([UploadTemplates_Name]
           ,[Template_Description]
           ,[Parameters]
           ,[Status]
           ,[Is_Deleted]
           ,[Enabled_Python])
     VALUES
           ('PMS and AIF Holdings'
           ,'Common template to upload PMS and AIF underlying holdings'
           ,'<Parameters></Parameters>'
           ,0
           ,0
           ,1)
GO



/**************************** Yes bank report *****************************/
INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Yes Bank Report','This job will send Yes bank report.',11,'',
'<Parameters>
  <SendEmail>YES</SendEmail>
  <Recipients>sachin.jaiswal@finalyca.com,support@finalyca.com,kamlesh.manjrekar@finalyca.com</Recipients>
  <Recipientscc>ibrahim.saifuddin@finalyca.com</Recipientscc>
  <Recipientsbcc />
  <Subject>Yes Bank Report</Subject>
  <Body>Yes Bank Report</Body>
  <Day>20</Day>
  <FunctionName>get_yesbank_report</FunctionName>
</Parameters>',
0,0,'Pending','2023-06-26',5,0,2);


/**************************** Axis bank report *****************************/
INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Axis Bank Report','This job will send Axis bank report.',11,'',
'<Parameters>
  <SendEmail>YES</SendEmail>
  <Recipients>sachin.jaiswal@finalyca.com,support@finalyca.com,kamlesh.manjrekar@finalyca.com</Recipients>
  <Recipientscc>ibrahim.saifuddin@finalyca.com</Recipientscc>
  <Recipientsbcc />
  <Subject>Axis Bank Report</Subject>
  <Body>Axis Bank Report</Body>
  <Day>15,20</Day>
  <FunctionName>get_axis_bank_report</FunctionName>
</Parameters>',
0,0,'Pending','2023-06-26',5,0,2);



/**************************** Motilal oswal report *****************************/
INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Motilal Oswal Report','This job will send Motilal Oswal report.',11,'',
'<Parameters>
  <SendEmail>YES</SendEmail>
  <Recipients>sachin.jaiswal@finalyca.com,support@finalyca.com,kamlesh.manjrekar@finalyca.com</Recipients>
  <Recipientscc>ibrahim.saifuddin@finalyca.com</Recipientscc>
  <Recipientsbcc />
  <Subject>Motilal Oswal Report</Subject>
  <Body>Motilal Oswal Report</Body>
  <Day>20,27</Day>
  <FunctionName>get_motilaloswal_report</FunctionName>
</Parameters>',
0,0,'Pending','2023-06-26',5,0,2);



/**************************** Cervin report *****************************/
INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Cervin Report','This job will send Cervin report.',11,'',
'<Parameters>
  <SendEmail>YES</SendEmail>
  <Recipients>sachin.jaiswal@finalyca.com,support@finalyca.com,kamlesh.manjrekar@finalyca.com</Recipients>
  <Recipientscc>ibrahim.saifuddin@finalyca.com</Recipientscc>
  <Recipientsbcc />
  <Subject>Cervin Report</Subject>
  <Body>Cervin Report</Body>
  <Day>9,15,20</Day>
  <FunctionName>get_detailed_nav_aum_report</FunctionName>
</Parameters>',
0,0,'Pending','2023-06-26',5,0,2);


INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Send Email and SMS - Trial access','This job will send email and SMS to trial user and business team.',9,'',
'<Parameters><ImportType>FunctiontoExecute</ImportType><FunctionName>check_trial_user_usage</FunctionName></Parameters>',0,1,'Success','2023-08-25',
(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Execute Python function'),0,2);

INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('CMOTS - MF - Fund Manager Upload (FTP)','This job will fetch required files from FTP and upload fund manager details.',9,'',
'<Parameters><ImportType>FunctiontoExecute</ImportType><FunctionName>upload_mf_fundmanager</FunctionName><ImportType>FTP</ImportType>
  <FTPURL>203.197.64.8</FTPURL>
  <FTPPORT>21</FTPPORT>
  <FTPUserId>finalyca</FTPUserId>
  <FTPPassword>FiNa1LcA</FTPPassword>
  <ImportPath>E:\Finalyca\ServiceManager\Imports\</ImportPath>
  <ImportFileName>/Mutual/@ddmmyyyy/</ImportFileName>
  <FileHeaders />
  <ColumnSeperator>|</ColumnSeperator>
  <RowSeperator>\n</RowSeperator>
  <DaysCount>-1</DaysCount>
  <SendEmail>YES</SendEmail>
  <Recipients>support@finalyca.com</Recipients>
  <RecipientsCC>sachin.jaiswal@finalyca.com</RecipientsCC>
  <RecipientsBCC />
  <Subject>CMOTS Upload - Fund Manager</Subject>
  <Body>CMOTS Upload - Fund Manager</Body>
  <IsBodyHTML>1</IsBodyHTML></Parameters>',0,1,'Success','2023-08-30',
(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Execute Python function'),0,2);


-- addition of column in deliveryRequest
ALTER table Delivery.DeliveryRequest 
add Created_By bigint default null

ALTER table Delivery.DeliveryRequest 
add X_Token nvarchar(max) default null

ALTER table Delivery.DeliveryRequest 
add Is_Deleted bit default 0

-- #delete old delivery request
INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Enabled_Python], [Channel_Id])
VALUES
('Mark Inactive - Portfolio PDF','This job will mark all portfolio pdf delivery requests which is expired as Inactive.',9,'',
'<Parameters><ImportType>FunctiontoExecute</ImportType><FunctionName>delete_old_delivery_requests</FunctionName></Parameters>',0,1,'Success','2024-01-08',(SELECT Schedule_id from [Reporting].[ReportSchedules] where Schedule_Name = 'Execute Python function'),1,2);
