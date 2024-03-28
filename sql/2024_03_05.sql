USe [PMS_Controller]

ALTER table [Transactions].[AccountAggregatorAPIStatus]
ADD fetch_callback_req_time DATETIME NULL;

ALTER table [Transactions].[AccountAggregatorAPIStatus]
ADD report_fetch_resp NVARCHAR(max) NULL;