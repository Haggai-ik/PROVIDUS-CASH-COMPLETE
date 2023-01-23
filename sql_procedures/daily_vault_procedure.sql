CREATE OR ALTER PROCEDURE [DAILY_VAULT_TABLE] 
as begin
 
declare @gdate as date 
set @gdate= DATEADD(day, -1, cast(getdate() as date))

insert into [DBO].[VAULT_BALANCE]

select BRA_CODE,BANK_DATE,case when CURRENCY ='NAIRA' then 1
when CURRENCY ='US Dollar' then 2
when CURRENCY ='EURO' then 3
when CURRENCY ='Pound Sterling' then 4 end CUR_CODE,SUM(LED_BAL) LED_BAL,SUM(AVAIL_BAL)_AVAIL_BAL
--INTO [DBO].[VAULT_BALANCE]
FROM  [ORCL_DB_Cashcomp_DR]..[CASHCOMP_USER].[VAULT_BALANCE]
WHERE GL_CODE='101' and BANK_DATE=@gdate 
GROUP BY BRA_CODE,BANK_DATE,CURRENCY

end






