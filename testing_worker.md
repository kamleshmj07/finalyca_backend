
``` sql
INSERT INTO [Reporting].[ReportSchedules]
([Schedule_Name],[Schedule_Description],[Start_Date],[End_Date],[Type],[Frequency],[Pickup_Hours],[Pickup_Minutes],[Enabled])
VALUES
('new 1','Something nice','2022-07-13', NULL,'hourly', 1, 0, 20, 1 ),
('new 2','Something nice','2022-07-13', '2022-07-17','daily', 1, 12, 20, 1 ),
('new 3','Something nice','2022-07-11', '2022-07-13','daily', 1, 12, 20, 1 )
```

above schedules should create 12, 13, 14 as the primary id.


``` sql
INSERT INTO [Reporting].[ReportJobs]
([Job_Name],[Job_Description],[Report_Type],[Report_Object],[Parameters],[Enabled],[Status],[Status_Message],[Last_Run],[Schedule_Id],[Channel_Id])
VALUES
('job 1','somethign nice',2,'',NULL,1,0,'Pending','2022-05-01',12,2),
('job 2','somethign nice',2,'',NULL,1,0,'Pending','2022-05-01',13,2),
('job 3','somethign nice',2,'',NULL,1,0,'Pending','2022-05-01',14,2)
```

only job 1 and job 2 should be picked for execution.