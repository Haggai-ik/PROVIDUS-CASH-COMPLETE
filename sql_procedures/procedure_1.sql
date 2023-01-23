create or alter  procedure [dbo].[prc_analytics_cash_MGT_3] (@gdate date)
as begin


--select * from cash_management_B
truncate table cash_management_B
insert into cash_management_B
SELECT  [BRA_CODE] as BRANCH_CODE,
      cast ([TRA_DATE] as date ) transaction_date
    ,[CUR_CODE] tran_crncy_code     
      ,isnull(sum([DEBIT]),0) as withdrawer_Amount
      ,isnull(sum([CREDIT]),0) deposit_amount,
	  isnull(sum([CRNT_BAL]),0) current_balance 
	  

	-- into cash_management_B
FROM [DBO].[DAILY_CASH_TRANS]
--where [TRA_DATE] between DATEADD(day, -14,  @gdate)and   DATEADD(day, 3,  @gdate)
	  
group by [TRA_DATE],[CUR_CODE],[BRA_CODE]



truncate table Cash_mamangement_Deposit_B
insert into Cash_mamangement_Deposit_B 
Select @gdate transaction_date, BRANCH_CODE,tran_crncy_code,
        maX(case when transaction_date =  DATEADD(day, -1,  @gdate) then Deposit_Amount else 0 end ) CREDIT_1_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -2,  @gdate) then Deposit_Amount else 0 end ) CREDIT_2_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -3,  @gdate) then Deposit_Amount else 0 end  )CREDIT_3_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -4,  @gdate) then Deposit_Amount else 0 end  )CREDIT_4_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -5,  @gdate) then Deposit_Amount else 0 end ) CREDIT_5_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -6,  @gdate) then Deposit_Amount else 0 end ) CREDIT_6_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -7,  @gdate) then Deposit_Amount else 0 end  )CREDIT_7_Day_Back,
        max(case when transaction_date =  DATEADD(day, -8,  @gdate) then Deposit_Amount else 0 end  )CREDIT_8_Day_Back,
        max(case when transaction_date =  DATEADD(day, -9,  @gdate) then Deposit_Amount else 0 end  )CREDIT_9_Day_Back,
        max(case when transaction_date =  DATEADD(day, -10,  @gdate) then Deposit_Amount else 0 end  )CREDIT_10_Day_Back,
        max(case when transaction_date =  DATEADD(day, -11,  @gdate) then Deposit_Amount else 0 end  )CREDIT_11_Day_Back,
        max(case when transaction_date =  DATEADD(day, -12,  @gdate) then Deposit_Amount else 0 end  )CREDIT_12_Day_Back,
        max(case when transaction_date =  DATEADD(day, -13,  @gdate) then Deposit_Amount else 0 end  )CREDIT_13_Day_Back,
        max(case when transaction_date =  DATEADD(day, -14,  @gdate) then Deposit_Amount else 0 end  )CREDIT_14_Day_Back,
        max(case when transaction_date >=  DATEADD(day, -14,  @gdate) then Deposit_Amount else 0 end  )MAX_CREDIT_14_Day_Back,
        MIn(case when transaction_date >=  DATEADD(day, -14,  @gdate) then Deposit_Amount else 0 end  )Min_CREDIT_14_Day_Back,
        Sum(case when transaction_date >=  DATEADD(day, -14,  @gdate) then Deposit_Amount else 0 end  )Sum_CREDIT_14_Day_Back,
        Avg(case when transaction_date >=  DATEADD(day, -14,  @gdate) then Deposit_Amount else 0 end  )Avg_CREDIT_14_Day_Back,
        max(case when transaction_date = DATEADD(day, 2,  @gdate) then Deposit_Amount else 0 end ) Target_Credit ,
   DATEADD(day, 2,  @gdate) FORCAST_DATE
      
      -- into Cash_mamangement_Deposit_B 
from 
[dbo].[cash_management_B]
 
where transaction_date between DATEADD(day, -14,  @gdate) and   DATEADD(day, 2,  @gdate) 
group by Branch_code, tran_crncy_code

truncate table Cash_mamangement_withdrawer_B
insert into Cash_mamangement_withdrawer_B
Select @gdate transaction_date,Branch_code, tran_crncy_code,
        maX(case when transaction_date =  DATEADD(day, -1,  @gdate) then withdrawer_Amount else 0 end ) DEBIT_1_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -2,  @gdate) then withdrawer_Amount else 0 end ) DEBIT_2_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -3,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_3_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -4,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_4_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -5,  @gdate) then withdrawer_Amount else 0 end ) DEBIT_5_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -6,  @gdate) then withdrawer_Amount else 0 end ) DEBIT_6_Day_Back,
        maX(case when transaction_date =  DATEADD(day, -7,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_7_Day_Back,
        max(case when transaction_date =  DATEADD(day, -8,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_8_Day_Back,
        max(case when transaction_date =  DATEADD(day, -9,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_9_Day_Back,
        max(case when transaction_date =  DATEADD(day, -10,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_10_Day_Back,
        max(case when transaction_date =  DATEADD(day, -11,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_11_Day_Back,
        max(case when transaction_date =  DATEADD(day, -12,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_12_Day_Back,
        max(case when transaction_date =  DATEADD(day, -13,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_13_Day_Back,
        max(case when transaction_date =  DATEADD(day, -14,  @gdate) then withdrawer_Amount else 0 end  )DEBIT_14_Day_Back,
        max(case when transaction_date >=  DATEADD(day, -14,  @gdate) then withdrawer_Amount else 0 end  )MAX_DEBIT_14_Day_Back,
        MIn(case when transaction_date >=  DATEADD(day, -14,  @gdate) then withdrawer_Amount else 0 end  )Min_DEBIT_14_Day_Back,
        Sum(case when transaction_date >=  DATEADD(day, -14,  @gdate) then withdrawer_Amount else 0 end  )Sum_DEBIT_14_Day_Back,
        Avg(case when transaction_date >=  DATEADD(day, -14,  @gdate) then withdrawer_Amount else 0 end  )Avg_DEBIT_14_Day_Back,
        max(case when transaction_date =  DATEADD(day, +2,  @gdate) then withdrawer_Amount else 0 end  )Target_Debit,
   DATEADD(day, 2,  @gdate) FORCAST_DATE
      -- into Cash_mamangement_withdrawer_B 
from 
[dbo].[cash_management_B]
 
where transaction_date between DATEADD(day, -14,  @gdate) and   DATEADD(day, 2,  @gdate)
group by Branch_code, tran_crncy_code



truncate table lag_table
insert into lag_table
Select full_date,
case when day_short_name in ('MON','TUE','WED','THU','FRI' ) then 1
    when day_short_name in ('SAT','SUN') then 0 end New_day_Number,
    
     case when [date] is null then 1 else 0 end holiday_status
--into lag_table
from dim_date 
left join [dim_holidays] 
on [dim_date].[FULL_DATE] = [dim_holidays].[Date]



truncate table lead_lead_table
insert into lead_lead_table 
SELECT full_date,
New_day_Number, 
   lead(New_day_Number,1) OVER(ORDER BY full_date) AS NEXT_Number_1,
   lead(New_day_Number,2) OVER(ORDER BY full_date) AS NEXT_Number_2,
   lead(New_day_Number,3) OVER(ORDER BY full_date) AS NEXT_Number_3,
   LEad(New_day_Number,4) OVER(ORDER BY full_date) AS NEXT_Number_4,
    LEad(New_day_Number,5) OVER(ORDER BY full_date) AS NEXT_Number_5,
    LEad(New_day_Number,6) OVER(ORDER BY full_date) AS NEXT_Number_6,
    LEad(New_day_Number,7) OVER(ORDER BY full_date) AS NEXT_Number_7,   
    lag(New_day_Number,1) OVER(ORDER BY full_date) AS Previous_Number_1,
   lag(New_day_Number,2) OVER(ORDER BY full_date) AS Previous_Number_2,
   lag(New_day_Number,3) OVER(ORDER BY full_date) AS Previous_Number_3,
   lag(New_day_Number,4) OVER(ORDER BY full_date) AS Previous_Number_4,
   lag(New_day_Number,5) OVER(ORDER BY full_date) AS Previous_Number_5,
    lag(New_day_Number,6) OVER(ORDER BY full_date) AS Previous_Number_6,
    lag(New_day_Number,7) OVER(ORDER BY full_date) AS Previous_Number_7
--into lead_lead_table 
FROM lag_table


truncate table lead_lag
insert into lead_lag
select b.*,
NEXT_Number_1+NEXT_Number_2+NEXT_Number_3+NEXT_Number_4 + NEXT_Number_5+ NEXT_Number_6+ NEXT_Number_7 
as sum_Leads,
Previous_Number_1+Previous_Number_2+Previous_Number_3+Previous_Number_4 +Previous_Number_5+Previous_Number_6+Previous_Number_7
as sum_lag 
--into lead_lag
from
(Select full_date  as full_date_period, 
NEXT_Number_1,
NEXT_Number_2,
NEXT_Number_3,
NEXT_Number_4,
NEXT_Number_5,
NEXT_Number_6,
NEXT_Number_7,
Previous_Number_1,
Previous_Number_2,
Previous_Number_3,
Previous_Number_4,
Previous_Number_5,
Previous_Number_6,
Previous_Number_7
 from lead_lead_table
)b



truncate table holiday_status
insert  into holiday_status

select B.*, Weeekdend_status+Holiday_status AS Holiday_Count 
--into holiday_status 
from 
(
Select full_date , day_short_name,
case when  day_short_name in ('SUN','SAT') then 1 else 0 end Weeekdend_status, DAY_IN_MONTH DAY_MONTH, MONTH_NAME [MONTH],
case when b.Date is null then 0 else 1 end Holiday_status
from [dim_date ] a 
left join [dbo].[dim_holidays]b  on a.full_date =b.date

)B

delete from PRE_POST_HOLIDAY where FULL_DATE between   DATEADD(day, -7,  @gdate)  and  DATEADD(day, 8,  @gdate)


insert into PRE_POST_HOLIDAY

Select b.*,
Holiday_status_1_Day_Back + Holiday_status_2_Day_Back + Holiday_status_3_Day_Back + Holiday_status_4_Day_Back + Holiday_status_5_Day_Back+Holiday_status_6_Day_Forward+Holiday_status_7_Day_Back  PREBUBHOL,
Holiday_status_1_Day_Forward + Holiday_status_2_Day_Forward + Holiday_status_3_Day_Forward + Holiday_status_4_Day_Forward + Holiday_status_5_Day_Forward+Holiday_status_6_Day_Forward+Holiday_status_7_Day_Forward  POSTBUBHOL
,c.DAY_IN_MONTH DAY_MONTH, c.MONTH_NAME [Month] 
--into PRE_POST_HOLIDAY
from
(
Select @gdate full_date,
        maX(case when FULL_DATE =  DATEADD(day, -1,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_1_Day_Back,
        maX(case when FULL_DATE =  DATEADD(day, -2,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_2_Day_Back,
        maX(case when FULL_DATE =  DATEADD(day, -3,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_3_Day_Back,
        maX(case when FULL_DATE =  DATEADD(day, -4,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_4_Day_Back,
        maX(case when FULL_DATE =  DATEADD(day, -5,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_5_Day_Back,
        maX(case when FULL_DATE =  DATEADD(day, -6,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_6_Day_Back,
        maX(case when FULL_DATE =  DATEADD(day, -7,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_7_Day_Back,
 
        maX(case when FULL_DATE =  DATEADD(day, 1,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_1_Day_Forward,
        maX(case when FULL_DATE =  DATEADD(day, 2,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_2_Day_Forward,
        maX(case when FULL_DATE =  DATEADD(day, 3,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_3_Day_Forward,
        maX(case when FULL_DATE =  DATEADD(day, 4,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_4_Day_Forward,
        maX(case when FULL_DATE =  DATEADD(day, 5,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_5_Day_Forward,
        maX(case when FULL_DATE =  DATEADD(day, 6,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_6_Day_Forward,
        maX(case when FULL_DATE =  DATEADD(day, 7,  DATEADD(day, +2,  @gdate)) then Holiday_count else 0 end ) Holiday_status_7_Day_Forward,
        DATEADD(day, +2,  @gdate) FORCASTT_DATE
        from     holiday_status
 
        where   FULL_DATE  between   DATEADD(day, -7,  @gdate)  and  DATEADD(day, 8,  @gdate)
        --group by day_short_name,DAY_MONTH,[MONTH]
        )b inner join [dim_date ] c on b.FORCASTT_DATE=c.FULL_DATE


insert into  Analytics_cash_MGT_Deposit_B
Select a.transaction_date AS REFERENCE_DATE,
Branch_code,
tran_crncy_code,
CREDIT_1_Day_Back,
CREDIT_2_Day_Back,
CREDIT_3_Day_Back,
CREDIT_4_Day_Back,
CREDIT_5_Day_Back,
CREDIT_6_Day_Back,
CREDIT_7_Day_Back,
CREDIT_8_Day_Back,
CREDIT_9_Day_Back,
CREDIT_10_Day_Back,
CREDIT_11_Day_Back,
CREDIT_12_Day_Back,
CREDIT_13_Day_Back,
CREDIT_14_Day_Back,
MAX_CREDIT_14_Day_Back,
Min_CREDIT_14_Day_Back,
Sum_CREDIT_14_Day_Back,
Avg_CREDIT_14_Day_Back,
Target_Credit,
b.*,
case when c.date is  null then 'NO Holiday'Else 'Holiday' End Holiday_status,
isNUll(c.day,'NO Holiday') Holiday_day, isnull(c.Holiday,'NO Holiday') Holiday_type,
c.day,c.holiday,gg.*,  DATEADD(day, +2,  @gdate) FORCAST_DATE
--into  Analytics_cash_MGT_Deposit_B
from Cash_mamangement_deposit_B a
left join lead_lag b on a.FORCAST_DATE =b.full_date_period
left  Join dim_holidays c   on c.date =a.FORCAST_DATE
left  join PRE_POST_HOLIDAY gg on gg.FORCASTT_DATE=a.FORCAST_DATE



insert INTO  Analytics_cash_MGT_withdrawer_B
Select a.transaction_date AS REFERENCE_DATE,
Branch_code,
tran_crncy_code,
DEBIT_1_Day_Back,
DEBIT_2_Day_Back,
DEBIT_3_Day_Back,
DEBIT_4_Day_Back,
DEBIT_5_Day_Back,
DEBIT_6_Day_Back,
DEBIT_7_Day_Back,
DEBIT_8_Day_Back,
DEBIT_9_Day_Back,
DEBIT_10_Day_Back,
DEBIT_11_Day_Back,
DEBIT_12_Day_Back,
DEBIT_13_Day_Back,
DEBIT_14_Day_Back,
MAX_DEBIT_14_Day_Back,
Min_DEBIT_14_Day_Back,
Sum_DEBIT_14_Day_Back,
Avg_DEBIT_14_Day_Back,
Target_Debit,
b.*,
case when c.date is  null then 'NO Holiday'Else 'Holiday' End Holiday_status,
isNUll(c.day,'NO Holiday') Holiday_day, isnull(c.Holiday,'NO Holiday') Holiday_type,
c.day,c.Holiday,gg.*,  DATEADD(day, +2,  @gdate) FORCAST_DATE
--INTO  Analytics_cash_MGT_withdrawer_B
from Cash_mamangement_withdrawer_B a
left join lead_lag b on a.FORCAST_DATE =b.full_date_period
left Join dim_holidays c   on c.date =a.FORCAST_DATE
left  join PRE_POST_HOLIDAY gg on gg.FORCASTT_DATE=a.FORCAST_DATE

end



exec [dbo].[prc_analytics_cash_MGT_3] '2022-06-21'

select max(reference_date) from [dbo].[Analytics_cash_MGT_withdrawer_B]

