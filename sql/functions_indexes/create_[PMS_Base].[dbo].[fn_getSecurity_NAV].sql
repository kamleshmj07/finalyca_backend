USE [PMS_Base]
GO

CREATE FUNCTION [dbo].[fn_getSecurity_NAV] (
    @ISIN varchar(50)
)
RETURNS TABLE
AS
RETURN
    select distinct CV.ISIN_Code
	,CV.[Date]
	,CV.[CLOSE]
from 
	 Transactions.ClosingValues CV with(nolock)
	 join (select distinct a.Vendor_Code from Masters.HoldingSecurity a
			left join Masters.HoldingSecurity b on a.BSE_Code = b.BSE_Code or a.NSE_Symbol = b.NSE_Symbol
			where a.Vendor_Code is not null and a.Is_Deleted <> 1 and b.ISIN_Code = @ISIN) as HS on HS.Vendor_Code = CV.Co_Code
										and isnull(CV.Is_Deleted,0) <> 1
										and CV.ST_EXCHNG = 'NSE'
										and CV.ISIN_Code is not null
