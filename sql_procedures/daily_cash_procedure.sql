CREATE OR ALTER PROCEDURE [DAILY_CASH_TABLE] 
as begin

declare @gdate as date 

set @gdate= DATEADD(day, -1, cast(getdate() as date))
insert into [DBO].[DAILY_CASH_TRANS]

select *  
--into [DBO].[DAILY_CASH_TRANS] 
from [ORCL_DB_Cashcomp_DR]..[CASHCOMP_USER].[DAILY_CASH_TRANS]
where expl_code in (1,2,3) and CUR_CODE in (1,2,3,4) and TRA_DATE=@gdate

end






