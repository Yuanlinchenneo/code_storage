# Databricks notebook source


# COMMAND ----------

# MAGIC %md # 1. Getting Payment Details from Statements

# COMMAND ----------

pay1 = spark.sql('''
select
    accountId, userId, Statement_Open_Date, Statement_Close_Date, Statement_Opening_Month, Statement_Closing_Month, Statement_Year, Statement_Month, Payment, Purchase, Previous_Amount_Owing, Credit_Limit, New_Statement_Balance, General_Interest_Amount,
    case when Previous_Amount_Owing=0 and New_Statement_Balance=0 and Purchase=0 then 0 else 1 end as Active_Status,ADB
  from( select
        userId, accountId,
        to_date(openingDate) as Statement_Open_Date,
        to_date(closingDate) as Statement_Close_Date,
        concat(year(to_date(openingDate)), '-r', lpad(month(to_date(openingDate)), 2, '0')) as Statement_Opening_Month,
        concat(year(to_date(closingDate)), '-', lpad(month(to_date(closingDate)), 2, '0')) as Statement_Closing_Month,
        substr(closingDate, 1, 4) as Statement_Year,
        substr(closingDate, 6, 2) as Statement_Month,
        (coalesce(summary.account.paymentsCents, 0) + coalesce(summary.account.paymentsAndCreditsCents, 0)) * 0.01 as Payment,
        coalesce(summary.account.purchasesCents, 0) * 0.01 as Purchase,
        coalesce(summary.account.previousBalanceCents, 0) * 0.01 as Previous_Amount_Owing,
        coalesce(summary.account.creditLimitCents, 0) * 0.01 as Credit_Limit,
        coalesce(summary.account.newBalanceCents) / coalesce(summary.account.creditLimitCents) as Utilization,
        coalesce(summary.account.generalInterestCents, 0) * 0.01 as General_Interest_Amount,
        summary.account.newBalanceCents * 0.01 as New_Statement_Balance,
        t2.ADB/100 as ADB
      from neo_raw_production.statement_service_credit_card_statements as t1
      left join
    neo_trusted_credit_risk.adb as t2 
  on
    t1._id=t2.statementId)
  order by accountId, Statement_Closing_Month
''') 
pay1.createOrReplaceTempView("pay1")

# COMMAND ----------

pay2 = spark.sql('''
select
    accountId, userId, Statement_Open_Date, Statement_Close_Date, Statement_Opening_Month, Statement_Closing_Month, Statement_Year, Statement_Month, Payment, Purchase, Previous_Amount_Owing, Credit_Limit, New_Statement_Balance,Active_Status,
    General_Interest_Amount,
    ADB,
    (New_Statement_Balance / Credit_Limit) as Current_Month_Utilization1,
    (Previous_Amount_Owing / Credit_Limit) as Previous_Month_Utilization1,
    row_number() over(partition by accountId, Active_Status order by statement_close_Date asc) as Active_Order
  from pay1
  order by accountId, Statement_Closing_Month
''') 
pay2.createOrReplaceTempView("pay2")


pay = spark.sql('''
select
    accountId, userId, Statement_Open_Date, Statement_Close_Date, Statement_Opening_Month, Statement_Closing_Month, Statement_Year, Statement_Month, Payment, Purchase, Previous_Amount_Owing, Credit_Limit, New_Statement_Balance,Active_Order,Active_Status,
    General_Interest_Amount,
    ADB,
    (Credit_Limit - New_Statement_Balance) as Open_To_Buy,
    case
      when Current_Month_Utilization1 is null then 0
      when Current_Month_Utilization1 >= 0  then Current_Month_Utilization1
      when Current_Month_Utilization1 < 0   then Current_Month_Utilization1
    end as Current_Month_Utilization,
    case
      when Previous_Month_Utilization1 is null then 0
      when Previous_Month_Utilization1 >= 0 then Previous_Month_Utilization1
      when Previous_Month_Utilization1 < 0 then Previous_Month_Utilization1
    end as Previous_Month_Utilization  
    from pay2
  order by accountId, Statement_Closing_Month
''') 
pay.createOrReplaceTempView("pay")
# pay.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Getting MOB and Account Closing Information

# COMMAND ----------

accountopening = spark.sql('''
select
    _id as accountId,
    to_date(createdAt)                               as Acquisition_Date      ,
    to_date(closeDate)                               as Date_Of_Closing       ,
    row_number() over(partition by _id order by _id) as Observation_Count
    
  from neo_raw_production.credit_account_service_credit_accounts
  order by userId
''') 
accountopening.createOrReplaceTempView("accountopening")


Base_Info1 = spark.sql('''
select
    pay.*,
    accountopening.Acquisition_Date,
    accountopening.Date_Of_Closing,
    months_between(pay.Statement_Close_Date,accountopening.Acquisition_Date) as MOB
  from pay
  left outer join accountopening on pay.accountId = accountopening.accountId
  order by accountId
''') 
Base_Info1.createOrReplaceTempView("Base_Info1")

# COMMAND ----------

# MAGIC %md #3. Capturing Credit Score

# COMMAND ----------

BureauPull = spark.sql('''
select user_id as userId, createdat, credit_report_date,
substr(credit_report_date,1,4) as Score_Year,
substr(credit_report_date,5,2) as Score_Month,
CVSC100 as Current_Credit_Score,
CVSC101 as Segment_Indicator,
datediff(month, createdat, now()) as Months_Since_Last_Pull,
cast(GO06 as int), GO26, GO91, PR44, PR45, PR124, PR116, PR15, PR50, PR74, PR75, PR120, PR97, GO81,AM36,BC33,RE33,RE34,BC34/100 as BC34,BC91,
row_number() over(partition by user_id,substr(credit_report_date,1,4),substr(credit_report_date,5,2) order by user_id,substr(credit_report_date,1,4),substr(credit_report_date,5,2)) as BureauIndicator,
row_number() over(partition by user_id order by substr(credit_report_date,1,4),substr(credit_report_date,5,2) desc) as BureauIndicator_2
from neo_raw_production.transunion_creditreport_creditvision
''') 
BureauPull.createOrReplaceTempView("BureauPull")

BureauPull3 = spark.sql('''
select *,
row_number() over(partition by userid order by createdat desc) as Score_Count
 from BureauPull
where BureauIndicator = 1 
order by userId, Score_Year, Score_Month
''') 
BureauPull3.createOrReplaceTempView("BureauPull3")


Table_With_Score_v1 = spark.sql('''
select Base_Info1.*,
BureauPull3.credit_report_date,
BureauPull3.Score_Year, 
BureauPull3.Score_Month,
BureauPull3.Current_Credit_Score,
BureauPull3.Segment_Indicator,
BureauPull3.GO06, BureauPull3.GO26, BureauPull3.GO91, BureauPull3.PR44, BureauPull3.PR45, BureauPull3.PR124, BureauPull3.PR116, BureauPull3.PR15, BureauPull3.PR50, BureauPull3.PR74, BureauPull3.PR75, BureauPull3.PR120, BureauPull3.PR97, BureauPull3.GO81,BureauPull3.AM36,BureauPull3.BC33,
BureauPull3.RE33,
BureauPull3.RE34,
BureauPull3.BC34,
BureauPull3.BC34,
BureauPull3.BC91,
row_number() over(partition by accountId order by accountId, Statement_Closing_Month desc) as B_Count
  from Base_Info1
     left join BureauPull3 on Base_Info1.userId = BureauPull3.userId
    and (Base_Info1.Statement_Year = BureauPull3.Score_Year and 
        Base_Info1.Statement_Month = BureauPull3.Score_Month )
''') 
Table_With_Score_v1.createOrReplaceTempView("Table_With_Score_v1")

Table_With_Score = spark.sql('''
  select 
          Table_With_Score_v1.accountId,
          Table_With_Score_v1.userId,
          Table_With_Score_v1.Statement_Open_Date,
          Table_With_Score_v1.Statement_Close_Date,
          Table_With_Score_v1.Statement_Opening_Month,
          Table_With_Score_v1.Statement_Closing_Month,
          Table_With_Score_v1.Statement_Year,
          Table_With_Score_v1.Statement_Month,
          Table_With_Score_v1.Payment,
          Table_With_Score_v1.Purchase,
          Table_With_Score_v1.Previous_Amount_Owing,
          Table_With_Score_v1.Credit_Limit,
          Table_With_Score_v1.New_Statement_Balance,
          Table_With_Score_v1.Active_Order,
          Table_With_Score_v1.Active_Status,
          Table_With_Score_v1.General_Interest_Amount,
          Table_With_Score_v1.ADB,
          Table_With_Score_v1.Open_To_Buy,
          Table_With_Score_v1.Current_Month_Utilization,
          Table_With_Score_v1.Previous_Month_Utilization,
          Table_With_Score_v1.Acquisition_Date,
          Table_With_Score_v1.Date_Of_Closing,
          Table_With_Score_v1.MOB,
          Table_With_Score_v1.credit_report_date,
          Table_With_Score_v1.Score_Year,
          Table_With_Score_v1.Score_Month,
    case when Table_With_Score_v1.Current_Credit_Score is NULL then t2.credit_report_date else Table_With_Score_v1.credit_report_date end as TU_PULL_DATE,
    case when Table_With_Score_v1.Current_Credit_Score is NULL then t2.Current_Credit_Score else Table_With_Score_v1.Current_Credit_Score end as Current_Credit_Score,
    case when Table_With_Score_v1.Segment_Indicator is NULL then t2.Segment_Indicator else Table_With_Score_v1.Segment_Indicator end  as Segment_Indicator,
    case when Table_With_Score_v1.GO06 is NULL then t2.GO06 else Table_With_Score_v1.GO06 end as GO06,
    case when Table_With_Score_v1.GO26 is NULL then t2.GO26 else Table_With_Score_v1.GO26 end as GO26,
    case when Table_With_Score_v1.GO91 is NULL then t2.GO91 else Table_With_Score_v1.GO91 end as GO91,
    case when Table_With_Score_v1.PR44 is NULL then t2.PR44 else Table_With_Score_v1.PR44 end as PR44,
    case when Table_With_Score_v1.PR45 is NULL then t2.PR45 else Table_With_Score_v1.PR45 end as PR45,
    case when Table_With_Score_v1.PR124 is NULL then t2.PR124 else Table_With_Score_v1.PR124 end as PR124,
    case when Table_With_Score_v1.PR116 is NULL then t2.PR116 else Table_With_Score_v1.PR116 end as PR116,
    case when Table_With_Score_v1.PR15 is NULL then t2.PR15 else Table_With_Score_v1.PR15 end as PR15,
    case when Table_With_Score_v1.PR50 is NULL then t2.PR50 else Table_With_Score_v1.PR50 end as PR50,
    case when Table_With_Score_v1.PR74 is NULL then t2.PR74 else Table_With_Score_v1.PR74 end as PR74,
    case when Table_With_Score_v1.PR75 is NULL then t2.PR75 else Table_With_Score_v1.PR75 end as PR75,
    case when Table_With_Score_v1.PR120 is NULL then t2.PR120 else Table_With_Score_v1.PR120 end as PR120,
    case when Table_With_Score_v1.PR97 is NULL then t2.PR97 else Table_With_Score_v1.PR97 end as PR97,
    case when Table_With_Score_v1.GO81 is NULL then t2.GO81 else Table_With_Score_v1.GO81 end as GO81,
    case when Table_With_Score_v1.AM36 is NULL then t2.AM36 else Table_With_Score_v1.AM36 end as AM36,
    case when Table_With_Score_v1.BC33 is NULL then t2.BC33 else Table_With_Score_v1.BC33 end as BC33,
    case when Table_With_Score_v1.RE33 is NULL then t2.RE33 else Table_With_Score_v1.RE33 end as RE33,
    case when Table_With_Score_v1.RE34 is NULL then t2.RE34 else Table_With_Score_v1.RE34 end as RE34,
    case when Table_With_Score_v1.BC34 is NULL then t2.BC34 else Table_With_Score_v1.BC34 end as BC34,
    case when Table_With_Score_v1.BC91 is NULL then t2.BC91 else Table_With_Score_v1.BC91 end as BC91,
    B_Count
  from 
    Table_With_Score_v1 
  left join 
BureauPull3 as t2 
on 
  Table_With_Score_v1.userId=t2.userid and Score_Count=1
''') 
Table_With_Score.createOrReplaceTempView("Table_With_Score")
# Table_With_Score.display()

# COMMAND ----------

Table_With_Apps1 = spark.sql('''
select  Table_With_Score.*,
        1 as count_indicator,
        round(MOB,0) as MOB_Rounded,
        coalesce(round(MOB,0) - 1) as MOB_Previous_1Month_Rounded,
        coalesce(round(MOB,0) - 2) as MOB_Previous_2Months_Rounded,
        coalesce(round(MOB,0) - 3) as MOB_Previous_3Months_Rounded,
        coalesce(round(Active_Order,0) - 1) as Active_Order_Previous_1Month_Rounded,
        coalesce(round(Active_Order,0) - 2) as Active_Order_Previous_2Months_Rounded,
        coalesce(round(Active_Order,0) - 3) as Active_Order_Previous_3Months_Rounded,
            case
            when (round(MOB,0)) is null                        then 'A. Missing'
            when (round(MOB,0)) < 1                            then 'B. LT 1'
            when (round(MOB,0)) >= 1  and (round(MOB,0)) < 4   then 'C. 1 - 3'
            when (round(MOB,0)) >= 4  and (round(MOB,0)) < 7   then 'D. 4 - 6'
            when (round(MOB,0)) >= 7  and (round(MOB,0)) < 13  then 'E. 7 - 12'
            when (round(MOB,0)) >= 13 and (round(MOB,0)) < 25  then 'F. 13 - 24'
            when (round(MOB,0)) >= 25                          then 'G. GT 24'
        end as MOB_Band,
                       
        case
            when (Segment_Indicator)=1  then 'A. No Trades'
            when (Segment_Indicator)=2  then 'B. Recent BK/90+ DPD'
            when (Segment_Indicator)=3  then 'C. Past BK/90+ DPD'
            when (Segment_Indicator)=4  then 'D. Recent Delinquency'
            when (Segment_Indicator)=5  then 'E. New to Credit'
            when (Segment_Indicator)=6  then 'F. No CC/Inactive'
            when (Segment_Indicator)=7  then 'F. Missing Payment'
            when (Segment_Indicator)=8  then 'F. Transactor'
            when (Segment_Indicator)=9  then 'F. Revolver'
            when (Segment_Indicator)=10 then 'F. Thin File'
        end as Segment_Indicator_category,
        
        case
            when (General_Interest_Amount) > 0                                       then 'Revolver'
            when (General_Interest_Amount) <= 0 or (General_Interest_Amount) is NULL then 'Transactor'
        end as Transactor_Revolver_Flag,
        
        row_number() over(partition by accountId, Statement_Closing_Month order by accountId, Statement_Closing_Month) as Counts_Var

        from Table_With_Score
    ''') 
Table_With_Apps1.createOrReplaceTempView("Table_With_Apps1")


Table_With_Apps = spark.sql('''
select * from Table_With_Apps1 where Counts_Var = 1
''') 
Table_With_Apps.createOrReplaceTempView("Table_With_Apps")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Getting Balances Tagged as Prior 1-3 Months

# COMMAND ----------

Keeping_Relevant_Information = spark.sql('''
select
accountId, MOB_Rounded, General_Interest_Amount, MOB_Previous_1Month_Rounded, MOB_Previous_2Months_Rounded, MOB_Previous_3Months_Rounded, Current_Month_Utilization, New_Statement_Balance, Payment, Previous_Amount_Owing,Active_Order_Previous_1Month_Rounded,Active_Order_Previous_2Months_Rounded,Active_Order_Previous_3Months_Rounded,Active_Status,ADB
from Table_With_Apps

''') 
Keeping_Relevant_Information.createOrReplaceTempView("Keeping_Relevant_Information")

Data_With_Last_Month = spark.sql('''
select 
Table_With_Apps.accountId,
Table_With_Apps.MOB_Rounded,
Table_With_Apps.Active_Status,
Table_With_Apps.ADB as ADB_1M,
Table_With_Apps.Active_Order as Active_Order_Previous_1Month_Rounded,
Table_With_Apps.New_Statement_Balance as New_Statement_Balance_1M_Prior,
Table_With_Apps.Payment               as Payment_1M_Prior,
Table_With_Apps.Previous_Amount_Owing as Previous_Amount_Owing_1M_Prior,
case when (Table_With_Apps.General_Interest_Amount)  > 0                                                      then 'Revolver'
     when (Table_With_Apps.General_Interest_Amount) <= 0 or (Table_With_Apps.General_Interest_Amount) is NULL then 'Transactor'
     end as Prior1M_Transactor_Revolver_Flag,
        
case when (Table_With_Apps.Current_Month_Utilization) >= .90 then 'High Util'
     when (Table_With_Apps.Current_Month_Utilization) <  .90 then 'Low Util'
     end as Prior1M_Util_Flag        

from Keeping_Relevant_Information
left join Table_With_Apps on Keeping_Relevant_Information.accountId = Table_With_Apps.accountId
    and Keeping_Relevant_Information.Active_Order_Previous_1Month_Rounded = Table_With_Apps.Active_Order
    and Keeping_Relevant_Information.Active_Status=Table_With_Apps.Active_Status
''') 
Data_With_Last_Month.createOrReplaceTempView("Data_With_Last_Month")


Data_With_Last_2Month = spark.sql('''
select 
Table_With_Apps.accountId,
Table_With_Apps.MOB_Rounded,
Table_With_Apps.Active_Status,
Table_With_Apps.ADB as ADB_2M,
Table_With_Apps.Active_Order as Active_Order_Previous_2Months_Rounded,
Table_With_Apps.New_Statement_Balance as New_Statement_Balance_2M_Prior,
Table_With_Apps.Payment as Payment_2M_Prior,
Table_With_Apps.Previous_Amount_Owing as Previous_Amount_Owing_2M_Prior,
case
            when (Table_With_Apps.General_Interest_Amount) > 0 then 'Revolver'
            when (Table_With_Apps.General_Interest_Amount) <= 0 or (Table_With_Apps.General_Interest_Amount) is NULL then 'Transactor'
        end as Prior2M_Transactor_Revolver_Flag,
        
case
            when (Table_With_Apps.Current_Month_Utilization) >= .90 then 'High Util'
            when (Table_With_Apps.Current_Month_Utilization) <  .90 then 'Low Util'
        end as Prior2M_Util_Flag        

from Keeping_Relevant_Information
left join Table_With_Apps on Keeping_Relevant_Information.accountId = Table_With_Apps.accountId
    and Keeping_Relevant_Information.Active_Order_Previous_2Months_Rounded = Table_With_Apps.Active_Order
    and Keeping_Relevant_Information.Active_Status=Table_With_Apps.Active_Status
''') 
Data_With_Last_2Month.createOrReplaceTempView("Data_With_Last_2Month")


Data_With_Last_3Month = spark.sql('''
select 
Table_With_Apps.accountId,
Table_With_Apps.MOB_Rounded,
Table_With_Apps.Active_Status,
Table_With_Apps.ADB  as ADB_3M,
Table_With_Apps.New_Statement_Balance as New_Statement_Balance_3M_Prior,
Table_With_Apps.Payment as Payment_3M_Prior,
Table_With_Apps.Active_Order as Active_Order_Previous_3Months_Rounded,
Table_With_Apps.Previous_Amount_Owing as Previous_Amount_Owing_3M_Prior,
case
            when (Table_With_Apps.General_Interest_Amount) > 0 then 'Revolver'
            when (Table_With_Apps.General_Interest_Amount) <= 0 or (Table_With_Apps.General_Interest_Amount) is NULL then 'Transactor'
        end as Prior3M_Transactor_Revolver_Flag,
        
case
            when (Table_With_Apps.Current_Month_Utilization) >= .90 then 'High Util'
            when (Table_With_Apps.Current_Month_Utilization) <  .90 then 'Low Util'
        end as Prior3M_Util_Flag      
from Keeping_Relevant_Information
left join Table_With_Apps on Keeping_Relevant_Information.accountId = Table_With_Apps.accountId
    and Keeping_Relevant_Information.Active_Order_Previous_3Months_Rounded = Table_With_Apps.Active_Order 
    and Keeping_Relevant_Information.Active_Status=Table_With_Apps.Active_Status
''') 
Data_With_Last_3Month.createOrReplaceTempView("Data_With_Last_3Month")

# COMMAND ----------

data1m = spark.sql('''
select *,
coalesce(MOB_Rounded,0)+1 as MOB_Rounded_P1,
coalesce(Active_Order_Previous_1Month_Rounded,0)+1 as AO_Rounded_P1
from Data_With_Last_Month
''') 
data1m.createOrReplaceTempView("data1m")


data2m = spark.sql('''
select *,
coalesce(MOB_Rounded,0)+2 as MOB_Rounded_P2,
coalesce(Active_Order_Previous_2Months_Rounded,0)+2 as AO_Rounded_P2
from Data_With_Last_2Month
''') 
data2m.createOrReplaceTempView("data2m")


data3m = spark.sql('''
select *,
coalesce(MOB_Rounded,0)+3 as MOB_Rounded_P3,
coalesce(Active_Order_Previous_3Months_Rounded,0)+3 as AO_Rounded_P3
from Data_With_Last_3Month
''') 
data3m.createOrReplaceTempView("data3m")


Master_Data_v1 = spark.sql('''
select Table_With_Apps.*,
data1m.Prior1M_Transactor_Revolver_Flag,
data1m.Prior1M_Util_Flag,
data1m.New_Statement_Balance_1M_Prior,
data1m.Payment_1M_Prior,
data1m.Previous_Amount_Owing_1M_Prior,
data2m.Prior2M_Transactor_Revolver_Flag,
data2m.Prior2M_Util_Flag,
data2m.New_Statement_Balance_2M_Prior,
data2m.Payment_2M_Prior,
data2m.Previous_Amount_Owing_2M_Prior,
data3m.Prior3M_Transactor_Revolver_Flag,
data3m.Prior3M_Util_Flag,
data3m.New_Statement_Balance_3M_Prior,
data3m.Payment_3M_Prior,
data3m.Previous_Amount_Owing_3M_Prior,
data1m.Active_Order_Previous_1Month_Rounded,
data2m.Active_Order_Previous_2Months_Rounded,
data3m.Active_Order_Previous_3Months_Rounded

from Table_With_Apps
left join data1m on Table_With_Apps.accountId = data1m.accountId and Table_With_Apps.Active_Order = data1m.AO_Rounded_P1 and Table_With_Apps.Active_Status=data1m.Active_Status
left join data2m on Table_With_Apps.accountId = data2m.accountId and Table_With_Apps.Active_Order = data2m.AO_Rounded_P2 and Table_With_Apps.Active_Status=data2m.Active_Status
left join data3m on Table_With_Apps.accountId = data3m.accountId and Table_With_Apps.Active_Order = data3m.AO_Rounded_P3 and Table_With_Apps.Active_Status=data3m.Active_Status
order by Table_With_Apps.accountId, Statement_Closing_Month
''') 
Master_Data_v1.createOrReplaceTempView("Master_Data_v1")

Master_Data = spark.sql('''
select *,
coalesce(Payment_1M_Prior,0) * 1 as Payment_1M_Pre,
coalesce(Payment_2M_Prior,0) * 1 as Payment_2M_Pre,  
coalesce(Payment_3M_Prior,0) * 1 as Payment_3M_Pre,  
coalesce(payment,0) * 1 as Last_Payment,
  
coalesce(New_Statement_Balance_1M_Prior,0) * 1 as Bal_1M_Pre,
coalesce(New_Statement_Balance_2M_Prior,0) * 1 as Bal_2M_Pre,  
coalesce(New_Statement_Balance_3M_Prior,0) * 1 as Bal_3M_Pre,  
coalesce(New_Statement_Balance,0) * 1 as Lastest_Balance ,

coalesce(Previous_Amount_Owing_1M_Prior,0) * 1 as Amt_Owing_2M_Pre,
coalesce(Previous_Amount_Owing_2M_Prior,0) * 1 as Amt_Owing_3M_Pre,  
coalesce(Previous_Amount_Owing_3M_Prior,0) * 1 as Amt_Owing_4M_Pre,  
coalesce(Previous_Amount_Owing,0) * 1 as Amt_Owing_1M_Pre 
from master_data_v1 
''') 
Master_Data.createOrReplaceTempView("Master_Data")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Getting Employee Versus Customer Flag

# COMMAND ----------

Cust_Type = spark.sql('''
select _id as userId, type as Cardmember_Type,
concat_ws('"',emails.email) as email_id
from neo_raw_production.user_service_users 
where type!='TEST'
''') 
Cust_Type.createOrReplaceTempView("Cust_Type")


Master_Data2 = spark.sql('''
select Master_Data.*,
ifNULL(((Last_Payment + Payment_1M_Pre + Payment_2M_Pre) / (Amt_Owing_1M_Pre + Amt_Owing_2M_Pre + Amt_Owing_3M_Pre)),1) as Pay_Bal_3M,
Cust_Type.Cardmember_Type,Cust_Type.email_id,
row_number() over(partition by Master_Data.accountId, Master_Data.Statement_Closing_Month order by Master_Data.accountId, Master_Data.Statement_Closing_Month) as Duplicates_Check   
from Master_Data

left join Cust_Type on Master_Data.userId = Cust_Type.userId and Master_Data.userId not in ('5efab473ac9014001ecedf4a','5e4dd39ff6aa5c001c78378d','5efab5a6201d3b001ef010e5','5efab6e9201d3b001ef01107','5efab6adac9014001ecedf91','5efab674ac9014001ecedf87','5efab630ac9014001ecedf7e','5efab5ee201d3b001ef010f2','5efab522ac9014001ecedf62','5efab4b8ac9014001ecedf56','5efab42dac9014001ecedf3d','5efab39f201d3b001ef010be','5efab350ac9014001ecedf25','5efab304ac9014001ecedf19','5efab2baac9014001ecedf0b','5efab27bac9014001ecedeff','5efab239ac9014001ecedeeb','5efab1fcac9014001ecedee3','5efab1c3ac9014001eceded9','5efab17f201d3b001ef01083','5efab13bac9014001ecedec9','5efab0eaac9014001ecedeb5','5efab0a4ac9014001ecedeab','5efab067ac9014001ecedea2','5efab026ac9014001ecede99','5efaafe6ac9014001ecede90','5efaafa0201d3b001ef01040','5efaaf56ac9014001ecede6c','5efaaf03ac9014001ecede61','5efaaea0201d3b001ef01022','5efaa921ac9014001eceddbe','5efa8c1f201d3b001ef00edf','5efa8b24201d3b001ef00ece','5e6c119f67030f001d5d50ee','5e6c112e67030f001d5d50e9','5e6c10c639a1d4001c542dd8','5e6c0fef67030f001d5d50e2','5e6c0dfb39a1d4001c542dc9','603dda1d9aff0d848be741c7','603ddd979aff0d3e66e74210','603de0699aff0d3678e74242','603dd816873dbab668dd6989','603dd8be9aff0dbcece741b4','603ddf4a873dba2a86dd69f9','603de15c9aff0d769fe74254','603ddc0a9aff0dfafde741f9','603dde93873dba1ed3dd69e9','603dd537873dba986cdd695d','603de2f69aff0db7f2e7426c','603dd977873dba503edd699b','603ddb4f873dba4f06dd69be','603ddfdc9aff0dd204e7422f','603dd69e873dba5a31dd6970','603dd75c9aff0dcc1be7419e','603ddac29aff0debd6e741d0','603de25d873dba2571dd6a1b','603ddb4f873dba4f06dd69be')
''') 
Master_Data2.createOrReplaceTempView("Master_Data2")


Master_Data3 = spark.sql('''
select * from Master_Data2 where Duplicates_Check = 1
''') 
Master_Data3.createOrReplaceTempView("Master_Data3")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Getting Current Delinquency

# COMMAND ----------

delinquency_info = spark.sql('''
select
*
from( 
  select
accountId,
date_sub(referenceDate,cast(daysPastDue as INT)) as Delinquency_Open_Date,
to_date(referenceDate) as Delinquency_Close_Date,
dayspastdue as days_past_due,
row_number() over(partition by accountId order by referenceDate desc) as Dpd_Order
from neo_trusted_analytics.earl_account
where (dayspastdue>0) and subProductName='UNSECURED')
where 
  Dpd_Order=1
''') 
delinquency_info.createOrReplaceTempView("delinquency_info")


Input_File = spark.sql('''
select accountId, Statement_Close_Date from Master_Data3
''') 
Input_File.createOrReplaceTempView("Input_File")


With_Del = spark.sql('''
select Input_File.*,
days_past_due
from delinquency_info
inner join Input_File on Input_File.accountId = delinquency_info.accountId and ((Input_File.Statement_Close_Date <  delinquency_info.Delinquency_Close_Date) or (delinquency_info.Delinquency_Close_Date is NULL))
order by Input_File.accountId, Input_File.Statement_Close_Date
''') 
With_Del.createOrReplaceTempView("With_Del")


With_Del_Cl_Date = spark.sql('''
select *
from With_Del
''') 
With_Del_Cl_Date.createOrReplaceTempView("With_Del_Cl_Date")


Master_With_Del = spark.sql('''
select With_Del_Cl_Date.*, 
row_number() over(partition by accountId, Statement_Close_Date order by accountId, Statement_Close_Date) as Duplicate_Del_Check
from With_Del_Cl_Date 
order by accountId, Statement_Close_Date
''') 
Master_With_Del.createOrReplaceTempView("Master_With_Del")

Master_With_Del2 = spark.sql('''
select *
from Master_With_Del where Duplicate_Del_Check = 1
''') 
Master_With_Del2.createOrReplaceTempView("Master_With_Del2")


Master_Data5 = spark.sql('''
select Master_Data3.*,
Master_With_Del2.Days_Past_Due from Master_Data3 
left join Master_With_Del2 on 
Master_Data3.accountId = Master_With_Del2.accountId and Master_Data3.Statement_Close_Date = Master_With_Del2.Statement_Close_Date
''') 
Master_Data5.createOrReplaceTempView("Master_Data5")
Master_Data7 = spark.sql('''
select Master_Data5.*,
case when Date_Of_Closing is not null and Date_Of_Closing > Statement_Close_Date then 1 else 0 end as Charge_Off_Flag
from Master_Data5
order by accountId, Statement_Closing_Month  
''') 
Master_Data7.createOrReplaceTempView("Master_Data7")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Getting Delinquencies in the previous 3 months

# COMMAND ----------

delinquency_info3 = spark.sql('''
select *
from( 
  select
accountId,
date_sub(referenceDate,cast(daysPastDue as INT)) as Del_Open_Date_Pre,
to_date(referenceDate)  as Del_Close_Date_Pre,
dayspastdue as credit_delinquency_service_days_past_due,
row_number() over(partition by accountId,to_date(date_sub(referenceDate,cast(daysPastDue as INT))) order by referenceDate desc) as Dpd_Order
from neo_trusted_analytics.earl_account
where (dayspastdue>0) and subProductName='UNSECURED')

''') 
delinquency_info3.createOrReplaceTempView("delinquency_info3")


Input_Pre = spark.sql('''
select
accountId,
Statement_Close_Date,
add_months(Statement_Close_Date,-3) as Pre_3M_Date
from Master_Data7
''') 
Input_Pre.createOrReplaceTempView("Input_Pre")


With_Del4 = spark.sql('''
select
Input_Pre.*,
Del_Open_Date_Pre,
Del_Close_Date_Pre,
credit_delinquency_service_days_past_due as Pre_DPD
from (
  select 
    *,
    row_number() over(partition by accountId order by credit_delinquency_service_days_past_due desc) as Max_Dpd
  from 
    delinquency_info3) as t1
inner join Input_Pre on Input_Pre.accountId = t1.accountId and Max_Dpd=1
where 
     Del_Open_Date_Pre>=Pre_3M_Date                            
order by Input_Pre.accountId, Input_Pre.Statement_Close_Date

''') 
With_Del4.createOrReplaceTempView("With_Del4")

Max_Pre_Del = spark.sql('''
select
With_Del4.*,
row_number() over(partition by accountId, Statement_Close_Date order by accountId, Statement_Close_Date, Pre_DPD desc) as Max_Pre_DPD_Counting
from With_Del4
''') 
Max_Pre_Del.createOrReplaceTempView("Max_Pre_Del")


Max_Pre_Del_v2 = spark.sql('''
select 
Max_Pre_Del.accountId,
Max_Pre_Del.Statement_Close_Date,
Max_Pre_Del.Pre_DPD as Max_Pre_3M_DPD,
Max_Pre_Del.Del_Open_Date_Pre,
Max_Pre_Del.Del_Close_Date_Pre
from Max_Pre_Del where Max_Pre_DPD_Counting = 1
''') 
Max_Pre_Del_v2.createOrReplaceTempView("Max_Pre_Del_v2")


Master_Data8 = spark.sql('''
select
Master_Data7.*,
Max_Pre_Del_v2.Max_Pre_3M_DPD,
Max_Pre_Del_v2.Del_Open_Date_Pre,
Max_Pre_Del_v2.Del_Close_Date_Pre,
add_months(Master_Data7.Statement_Open_Date,-3) as Pre_3M_Date,
case when Current_Credit_Score is null then 0
     when Current_Credit_Score = 0     then 0
     when Current_Credit_Score > 0     then Current_Credit_Score
     when Current_Credit_Score < 0     then 0
end as Bureau_Score_Clean
from Master_Data7 left join Max_Pre_Del_v2 on Master_Data7.accountId = Max_Pre_Del_v2.accountId
                                          and Master_Data7.Statement_Close_Date = Max_Pre_Del_v2.Statement_Close_Date
''') 
Master_Data8.createOrReplaceTempView("Master_Data8")
                                          
  
Master_Data9 = spark.sql('''
select *,
substr(Pre_3M_Date, 1, 4) as Pre_3M_Year,
substr(Pre_3M_Date, 6, 2) as Pre_3M_Month
from Master_Data8
''') 
Master_Data9.createOrReplaceTempView("Master_Data9")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md # 8. Account Status

# COMMAND ----------

FrozenFlag = spark.sql('''
select _id as userId,
createdAt as Frozen_Flag_Created_At,
status as User_Status,
frozenReason as Frozen_Reason,
physicalAddress.province,
case when employmentInfo.employmentStatus in ('STUDENT') and employmentInfo.fieldOfStudy in (
'Hospitality, Food and Tourism',
'Trades, Construction and Manufacturing'
) then 1 else 0 end as Field_Of_Study,
row_number() over(partition by _id order by _id, createdAt desc) as Duplicate_Frozen
from neo_raw_production.user_service_users
''') 
FrozenFlag.createOrReplaceTempView("FrozenFlag")

FrozenFlag1 = spark.sql('''
select * from FrozenFlag where Duplicate_Frozen = 1
''') 
FrozenFlag1.createOrReplaceTempView("FrozenFlag1")

Master_Data10 = spark.sql('''
select Master_Data9.*,
FrozenFlag1.User_Status,
FrozenFlag1.Field_Of_Study,
FrozenFlag1.Frozen_Reason,
province
from Master_Data9 
left join FrozenFlag1 on 
Master_Data9.userId = FrozenFlag1.userId
''') 
Master_Data10.createOrReplaceTempView("Master_Data10")

Master_Data12 = spark.sql('''
select *,
case when (Credit_Limit) is null                             then 'Missing Credit Limit'
     when (Credit_Limit) <= 0                                then '0. Less Than Equal To Zero'
      when (Credit_Limit) > 0    and (Credit_Limit) <= 100   then '1. 1 - 100'
      when (Credit_Limit) > 100  and (Credit_Limit) <= 500   then '2. 101 - 500'
      when (Credit_Limit) > 500  and (Credit_Limit) <= 1500  then '3. 501 - 1500'
      when (Credit_Limit) > 1500 and (Credit_Limit) <= 2500  then '4. 1501 - 2500'
      when (Credit_Limit) > 2500 and (Credit_Limit) <= 5000  then '5. 2501 - 5000'
      when (Credit_Limit) > 5000 and (Credit_Limit) <= 7500  then '6. 5001 - 7500'
      when (Credit_Limit) > 7500 and (Credit_Limit) <= 10000 then '7. 7501 - 10000'
      when (Credit_Limit) > 10000                            then '8. GT 10K'
    end as Credit_Limit_Band,
    
case when New_Statement_Balance > 0 then 1 else 0 end as Balance_Active,

((New_Statement_Balance + New_Statement_Balance_1M_Prior + New_Statement_Balance_2M_Prior) / (Credit_Limit + Credit_Limit + Credit_Limit)) as Avg_3M_Util
from Master_Data10
''') 
Master_Data12.createOrReplaceTempView("Master_Data12")
      

Pre_Final_Version = spark.sql('''
select *,
case
when ((GO26!= 'H') or ((Bureau_Score_Clean is null) or (Bureau_Score_Clean <= 0)))  then '1. No-Hit'
when GO91 = 'Y'                                                                     then '2. Deceased'
when (PR44 >=1) or (PR45 >=1) or (PR124 >=1)                                        then '3. Undischarged/Discharged Bankruptcies'
when (PR116 >=1)                                                                    then '4. Consumer Proposals'
when (PR15 >=1) or (PR50 >=1)                                                       then '5. Foreclosures'
when (PR74 >=1)                                                                     then '6. Unsatisfied Judgements'
when (PR75 >=1) or (PR120 >=1) or (PR97>=1)                                         then '07. Unpaid Collections'
when (GO81 >=1)                                                                     then '8. Chargeoffs'
end as policy_exclusions,

case
when Pay_Bal_3M                          is null then '1. Missing'
when Pay_Bal_3M = 0                              then '2. Zero'
when Pay_Bal_3M < .10                            then '3. LT 10%'
when Pay_Bal_3M >=.10  and Pay_Bal_3M <.25       then '4. 10 - 25%'
when Pay_Bal_3M >= .25 and Pay_Bal_3M < .50      then '5. 25 - 50%'
when Pay_Bal_3M >= .50 and Pay_Bal_3M < .75      then '6. 50 - 75%'
when Pay_Bal_3M >= .75 and Pay_Bal_3M < .90      then '7. 75 - 90'
when Pay_Bal_3M >= .90                           then '8. GT 90'
end as Pay_Bal_3M_Band,

case
            when (Bureau_Score_Clean) is null                               then 'A. Missing'
            when (Bureau_Score_Clean) < 600                                 then 'B. LT 600'
            when (Bureau_Score_Clean) >= 600 and (Bureau_Score_Clean) < 620 then 'C. 600 - 619'
            when (Bureau_Score_Clean) >= 620 and (Bureau_Score_Clean) < 639 then 'D. 620 - 639'
            when (Bureau_Score_Clean) >= 640 and (Bureau_Score_Clean) < 660 then 'E. 640 - 659'
            when (Bureau_Score_Clean) >= 660 and (Bureau_Score_Clean) < 690 then 'F. 660 - 689'
            when (Bureau_Score_Clean) >= 690 and (Bureau_Score_Clean) < 740 then 'G. 690 - 739'
            when (Bureau_Score_Clean) >= 740 and (Bureau_Score_Clean) < 800 then 'H. 740 - 799'
            when (Bureau_Score_Clean) >= 800                                then 'I. 800+'
        end as Latest_Credit_Score_Band2,
        
case 
when Transactor_Revolver_Flag = 'Transactor' and Prior1M_Transactor_Revolver_Flag = 'Transactor' and Prior2M_Transactor_Revolver_Flag = 'Transactor' then 'Consistent Transactor'
when Transactor_Revolver_Flag = 'Revolver' and Prior1M_Transactor_Revolver_Flag = 'Revolver' and Prior2M_Transactor_Revolver_Flag = 'Revolver' then 'Consistent Revolver'
end as Consistent_Transactor_Revolver_3M

from Master_Data12
''') 
Pre_Final_Version.createOrReplaceTempView("Pre_Final_Version")

# COMMAND ----------

CLD_Block_V2 = spark.sql('''
select 
  t1.*,
  case when t2.creditAccountId is NULL then 1 else 0 end as CLD_Block
from 
  Pre_Final_Version as t1
left join 
  (select 
        creditAccountId as creditAccountId,
        previousCreditLimitCents as newcreditLimitCents,
        initiatedAt as InitiatedAt
      from 
        neo_raw_production.credit_limit_service_credit_limit_decrease_offers
      where 
        creditLimitOfferStrategy in (
          'High Risk:[CLDHRM.v4.240422a]',
          'High Risk:[CLDHRM.v5.241001]',
          -- KG 2025-02-16
          'DELINQUENT_CLD',
          'CLD_HIGH_RISK',   -- Judge
          'CLD_DELINQUENCY'  -- Judge
          )
        and timestampdiff(day, appliedAt, current_date + interval 8 hours) <= 180 -- KG June 2025
  ) as t2   -- KG: added new Code 2024-09-19
on 
  t1.accountId=t2.creditAccountId 
  
''') 
CLD_Block_V2.createOrReplaceTempView("CLD_Block_V2")

# COMMAND ----------

CLD_Block = spark.sql('''
select 
  t1.*,
  -- KG 2024-08-06: added div 100
  t2.previousCreditLimitCents div 100 as Old_Limit,
  case when t2.creditAccountId is not NULL then 1 else 0 end as CLD_Population
from 
  CLD_Block_V2 as t1
left join 
-- KG 2024-11-26
  (select * 
  from neo_raw_production.credit_limit_service_credit_limit_decrease_offers decr
  where decr.creditLimitOfferStrategy='INACTIVE_CLD'
    and not exists
      (select 1 
      from neo_raw_production.credit_limit_service_credit_limit_offers c
      where c.creditAccountId = decr.creditAccountId and c.acceptedAt > decr.initiatedAt
      )
  ) as t2 
on 
  t1.accountId=t2.creditAccountId and creditLimitOfferStrategy='INACTIVE_CLD'
''') 
CLD_Block.createOrReplaceTempView("CLD_Block")

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Getting Previous Line Changes

# COMMAND ----------

# # KG June 2025 


# Line_Changes = spark.sql('''
# select creditAccountId,
# to_date(initiatedAt) as Line_Offer_Created_Date,
# acceptedAt,
# declinedAt,
# completedAt
# from neo_raw_production.credit_limit_service_credit_limit_offers
# ''') 
# Line_Changes.createOrReplaceTempView("Line_Changes")


# Line_Changes_v2 = spark.sql('''
# select creditAccountId,
# months_between(Line_Offer_Created_Date,current_date()) as Months_Since_CLI,
# Line_Offer_Created_Date,
# acceptedAt -- KG June 20254
# -- case when acceptedAt is not NULL then '05. Offer accepted'
# --           when acceptedAt is NULL and completedAt is NULL and declinedAt is NULL  then '01. Offer waiting for Customer action' 
# --           when declinedAt is not NULL then '04. Offer rejected by Customer'
# --           end as CLIOfferFlag
# from Line_Changes
# where
#   Line_Offer_Created_Date<='2023-07-01'
# union
# select 
#   creditAccountId, 
#   months_between(to_date(initiatedAt),current_date()) as Months_Since_CLI,
#   to_date(initiatedAt) as Line_Offer_Created_Date,
#   acceptedAt -- KG June 20254
#   --  case when acceptedAt is not NULL then '05. Offer accepted'
#   --         when acceptedAt is NULL and completedAt is NULL and declinedAt is NULL  then '01. Offer waiting for Customer action' 
#   --         when declinedAt is not NULL then '04. Offer rejected by Customer'
#   --         end as CLIOfferFlag
#   from
#     neo_raw_production.credit_limit_service_credit_limit_offers
#   where 
#   to_date(initiatedAt)>'2023-07-01'
# ''') 
# Line_Changes_v2.createOrReplaceTempView("Line_Changes_v2")


# Line_Changes_a = spark.sql('''
# select *,
# row_number() over(partition by creditAccountId order by acceptedAt desc) as Offer_Count 
# from Line_Changes_v2
# where acceptedAt is not null  -- KG June 2025
# ''') 
# Line_Changes_a.createOrReplaceTempView("Line_Changes_a")


# Line_Changes2 = spark.sql('''
# select *
# from Line_Changes_a where Offer_Count = 1
# ''') 
# Line_Changes2.createOrReplaceTempView("Line_Changes2")



# KG June 2025
Line_Changes2 = spark.sql('''
select
  creditAccountId,
  to_date(initiatedAt) as Line_Offer_Created_Date
from
  neo_raw_production.credit_limit_service_credit_limit_offers
qualify 
row_number() over (partition by creditAccountId order by initiatedAt desc) = 1
''') 
Line_Changes2.createOrReplaceTempView("Line_Changes2")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 10. Creating Final Dataset

# COMMAND ----------

Final_Version = spark.sql('''
select CLD_Block.*,
Line_Changes2.Line_Offer_Created_Date,
(months_between(current_date(),Line_Changes2.Line_Offer_Created_Date)) as Last_CLI_Months,
months_between(current_date(),Line_Changes2.Line_Offer_Created_Date) as Months_Since_CLI_Offer,
case when months_between(current_date(),Line_Changes2.Line_Offer_Created_Date) in (0,1,2,3) then 1 else 0 end as CLI_Offered_In_Last_3_Months, -- useless since months_between returns DOUBLE
-- case when (months_between(current_date(),Line_Changes2.Line_Offer_Created_Date)) < 3 and Line_Changes2.CLIOfferFlag in ('05. Offer accepted') then 1 else 0 end as Recent_CLI_Accept, 
-- case when (months_between(current_date(),Line_Changes2.Line_Offer_Created_Date)) < 3 and Line_Changes2.CLIOfferFlag in ('04. Offer rejected by Customer') then 1 else 0 end as Recent_CLI_Decline, 
-- case when (months_between(current_date(),Line_Changes2.Line_Offer_Created_Date)) < 3 and Line_Changes2.CLIOfferFlag in ('02. Expired after 30 days with no action') then 1 else 0 end as Recent_CLI_Expiry, 
--Line_Changes2.CLIOfferFlag,
case when Consistent_Transactor_Revolver_3M = 'Consistent Transactor' and New_Statement_Balance > 0 and New_Statement_Balance_1M_Prior > 0 and New_Statement_Balance_2M_Prior > 0 then 1 else 0 end as Transactor_Flag,
case when Consistent_Transactor_Revolver_3M = 'Consistent Transactor' then 'Transactor' else 'Revolver' end as User_Type_Flag,
case when Bureau_Score_Clean>=600 and Bureau_Score_Clean<700 then 1 else 0 end as Sub_Prime_Flag,
case when User_Status = 'ACTIVE' then 1 else 0 end as Not_Frozen_Indicator,
case when policy_exclusions in ('2. Deceased','3. Undischarged/Discharged Bankruptcies','4. Consumer Proposals','5. Foreclosures','6. Unsatisfied Judgements','07. Unpaid Collections','8. Chargeoffs') then 1 else 0 end as Policy_Exclusion_Flag,
case when days_past_due>0 then 1 else 0 end as Currently_Delinquent_Flag,
case when Max_Pre_3M_DPD >=30  then 1 else 0 end as Last_3M_Delinquent_Flag,
case when (Pay_Bal_3M >=.05 or Pay_Bal_3M<0 or (((Last_Payment + Payment_1M_Pre + Payment_2M_Pre)=0 and (Amt_Owing_1M_Pre + Amt_Owing_2M_Pre + Amt_Owing_3M_Pre)<0)))   then 1 else 0 end as Payment_Bal_Flag,
case when GO06 >= 3           then 1 else 0 end as Inquiry_Flag,
case when ((AM36>=12) or (AM36 is NULL)) then 1 else 0 end as AM36_Flag,
case when (Bureau_score_clean<700 and Credit_Limit>3000) then 0 else 1 end as SP_Limit_Flag,
case when (Bureau_Score_Clean >=600 and Bureau_Score_Clean<= 900) then 1 else 0 end as Bureau_Score_Flag,
case when Segment_Indicator = 2 or Segment_Indicator = 3 or Segment_Indicator = 4 then 1 else 0 end as Bureau_Segment_Flag,
row_number() over(partition by accountId order by accountId, Statement_Closing_Month desc) as Account_Latest_Entry
from CLD_Block
left join Line_Changes2 on Line_Changes2.creditAccountId = CLD_Block.accountId -- KG June 2025 changed to accountId
''') 
Final_Version.createOrReplaceTempView("Final_Version")
# Final_Version.display()
Grace_Data = spark.sql('''
select accountId, subProductName as credit_account_type,
row_number() over(partition by accountId order by accountId) as grace_count
from neo_trusted_analytics.earl_account
where 
    subProductName='UNSECURED'
''') 
Grace_Data.createOrReplaceTempView("Grace_Data")

Grace_Data2 = spark.sql('''
select accountId, credit_account_type from Grace_Data where grace_count = 1
''') 
Grace_Data2.createOrReplaceTempView("Grace_Data2")


Master_For_CLI = spark.sql('''
select Final_Version.*,
case when /*Last_CLI_Months >= 0 and */ Last_CLI_Months <= 3 then 1 else 0 end as CLI_In_Last_3_Months, -- KG June 2025
Grace_Data2.credit_account_type
from Final_Version
left join Grace_Data2 on Final_Version.accountId = Grace_Data2.accountId 
''') 
Master_For_CLI.createOrReplaceTempView("Master_For_CLI")

Final_Version_With_Flag = spark.sql('''
select *
from Master_For_CLI where Account_Latest_Entry = 1 and credit_account_type = 'UNSECURED'
and months_between(current_date(),Statement_Close_Date) <= 1
''')  
Final_Version_With_Flag.createOrReplaceTempView("Final_Version_With_Flag")

Grace_Data_Daily = spark.sql('''
select accountId, 
creditLimit as credit_limit_yesterday,
accountStatus as day_end_status_yesterday,
t2.frozenReason as user_frozen_reason_yesterday,
daysPastDue as credit_delinquency_service_days_past_due,
chargedOffReason as credit_delinquency_service_reporting_charge_off_type,
row_number() over(partition by accountId order by accountId, referenceDate desc) as grace_count_daily
from neo_trusted_analytics.earl_account as t1
left join 
neo_raw_production.user_service_users as t2 
on 
    t1.userId=t2._id
where 
subProductName='UNSECURED'

''') 
Grace_Data_Daily.createOrReplaceTempView("Grace_Data_Daily")

Grace_Data_Daily2 = spark.sql('''
select accountId, credit_limit_yesterday, day_end_status_yesterday, user_frozen_reason_yesterday, credit_delinquency_service_days_past_due, credit_delinquency_service_reporting_charge_off_type from Grace_Data_Daily where grace_count_daily = 1
''') 
Grace_Data_Daily2.createOrReplaceTempView("Grace_Data_Daily2")


Final_Version_With_Flag_Checks = spark.sql('''
select Final_Version_With_Flag.*, credit_limit_yesterday, day_end_status_yesterday, user_frozen_reason_yesterday, credit_delinquency_service_days_past_due, credit_delinquency_service_reporting_charge_off_type,
case when day_end_status_yesterday = 'OPEN' and user_frozen_reason_yesterday is NULL and credit_delinquency_service_days_past_due is NULL and credit_delinquency_service_reporting_charge_off_type='N/A' then 'Account_OPEN_Not_Frozen' else  'Account_Not_In_Good_Standing' end as Current_Account_Standing
from Final_Version_With_Flag 


left join Grace_Data_Daily2 on Final_Version_With_Flag.accountId = Grace_Data_Daily2.accountId 
''') 
Final_Version_With_Flag_Checks.createOrReplaceTempView("Final_Version_With_Flag_Checks")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 11. Getting Latest Product Information

# COMMAND ----------

dfcard_brand1 = spark.sql('''
select
creditAccountId as accountId,
cardBrandCompany as Latest_Product_Category,
row_number() over(partition by creditAccountId order by creditAccountId, createdAt desc) as latest_prod_uniq
from neo_raw_production.bank_service_cards
''')
dfcard_brand1.createOrReplaceTempView("dfcard_brand1")


dfcard_brand = spark.sql('''
select * from dfcard_brand1 where latest_prod_uniq = 1
''')
dfcard_brand.createOrReplaceTempView("dfcard_brand")


Final_CLI_Eligible1 = spark.sql('''
select
Final_Version_With_Flag_Checks.*,
dfcard_brand.Latest_Product_Category
from Final_Version_With_Flag_Checks
left join dfcard_brand on dfcard_brand.accountId = Final_Version_With_Flag_Checks.accountId
''')
Final_CLI_Eligible1.createOrReplaceTempView("Final_CLI_Eligible1")

# COMMAND ----------

# MAGIC %md
# MAGIC # 12. Getting Facility Data

# COMMAND ----------

credit_facility1 = spark.sql('''
select accountId, creditFacilityId as _id
from neo_raw_production.credit_facility_service_credit_facility_accounts
''')
credit_facility1.createOrReplaceTempView("credit_facility1")


credit_facility2 = spark.sql('''
select _id, name as Credit_Facility_Name
from neo_raw_production.credit_facility_service_credit_facilities
''')
credit_facility2.createOrReplaceTempView("credit_facility2")
 
  
credit_facility = spark.sql('''
select
credit_facility1.accountId,
credit_facility2.Credit_Facility_Name
from credit_facility1
join credit_facility2 on credit_facility1._id = credit_facility2._id
''')
credit_facility.createOrReplaceTempView("credit_facility")


Final_CLI_Eligible2 = spark.sql('''
select
Final_CLI_Eligible1.*,
credit_facility.Credit_Facility_Name
from Final_CLI_Eligible1
left join credit_facility on credit_facility.accountId = Final_CLI_Eligible1.accountId

''')
Final_CLI_Eligible2.createOrReplaceTempView("Final_CLI_Eligible2")

# COMMAND ----------

# MAGIC %md
# MAGIC # 13. Adding Latest Balance of Account

# COMMAND ----------

Latest_Balance_Info1 = spark.sql('''
select accountRef, 
totalBalanceCents * 0.01 as Latest_Balance_Of_Account,
row_number() over(partition by accountRef order by accountRef, createdAt desc) as latest_bal_entry_info
from neo_raw_production.ledger_service_credit_account_balance_summaries
''')
Latest_Balance_Info1.createOrReplaceTempView("Latest_Balance_Info1")


Latest_Balance_Info2 = spark.sql('''
select * from Latest_Balance_Info1 where latest_bal_entry_info = 1
''')
Latest_Balance_Info2.createOrReplaceTempView("Latest_Balance_Info2")


Final_CLI_Eligible = spark.sql('''
select
Final_CLI_Eligible2.*,
Latest_Balance_Of_Account,
case when Latest_Balance_Of_Account > Credit_Limit then 1 else 0 end as account_is_overlimit,
case when Latest_Balance_Of_Account > (Credit_Limit*1.2) then 1 else 0 end as account_is_overlimit_over_20p
from Final_CLI_Eligible2
left join Latest_Balance_Info2 on Latest_Balance_Info2.accountRef = Final_CLI_Eligible2.accountId

''')
Final_CLI_Eligible.createOrReplaceTempView("Final_CLI_Eligible")



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 14. Ensuring Non Del. In Last 3 Months

# COMMAND ----------

Grace_Data_Del_Last30 = spark.sql('''
select accountId, 
dayspastdue as dpd_value_100_days_prior,
row_number() over(partition by accountId order by accountId, dayspastdue desc) as count_100_days
from neo_trusted_analytics.earl_account
where referenceDate <= current_date and referenceDate > date_add(current_date, -100 ) and subProductName='UNSECURED'
''') 
Grace_Data_Del_Last30.createOrReplaceTempView("Grace_Data_Del_Last30")


Grace_Data_Del_Last30_v2 = spark.sql('''
select dpd_value_100_days_prior,
accountId
from Grace_Data_Del_Last30
where count_100_days = 1
''') 
Grace_Data_Del_Last30_v2.createOrReplaceTempView("Grace_Data_Del_Last30_v2")

Final_CLI_Eligible_Master_Data = spark.sql('''

select

Final_CLI_Eligible.*,
case when dpd_value_100_days_prior >= 30 then 1 else 0 end as Delinquent_Past_100Days
from Final_CLI_Eligible
left join Grace_Data_Del_Last30_v2 on Grace_Data_Del_Last30_v2.accountId = Final_CLI_Eligible.accountId
''')
Final_CLI_Eligible_Master_Data.createOrReplaceTempView("Final_CLI_Eligible_Master_Data")

# COMMAND ----------

CLI_Eligible_Master_Data = spark.sql('''
select
t1.*,
case when t2.accountId is NULL then 0 else 1 end as Prior_Control_Group
from Final_CLI_Eligible_Master_Data as t1
left join 
  neo_views_credit_risk.cli_revolver_transactor_tracking_v2 as t2
ON
  t1.accountId=t2.accountId and t2.type='CONTROL'
left join 
  neo_views_credit_risk.CLI_Accounts_Sept_ROC_2023 as t3
on
  t1.accountId=t3.accountId and t3.TEST_CONTROL='CONTROL'

''')
CLI_Eligible_Master_Data.createOrReplaceTempView("CLI_Eligible_Master_Data")

# COMMAND ----------

prov_1 = spark.sql('''
select
    accountId, to_date(closingDate) as Statement_Close_Date,
    summary.cardholder.physicalAddress.province as Province,
    row_number() over(partition by accountId order by accountId,closingDate desc) as Last_Statement
      from neo_raw_production.statement_service_credit_card_statements
  ''') 
prov_1.createOrReplaceTempView("prov_1")


prov_2 = spark.sql('''
select * from prov_1 where Last_Statement = 1
''') 
prov_2.createOrReplaceTempView("prov_2")


CLI_Eligible_Master_Data4 = spark.sql('''
select CLI_Eligible_Master_Data.*, case when CLI_Eligible_Master_Data.Province = 'QC' then 'Quebec' else 'Other' end as Province_of_Account
from CLI_Eligible_Master_Data
left join prov_2 on prov_2.accountId = CLI_Eligible_Master_Data.accountId
''')
CLI_Eligible_Master_Data4.createOrReplaceTempView("CLI_Eligible_Master_Data4")

# COMMAND ----------

# KG June 2025

# CLR = spark.sql('''
# select accountId,
# asoftimestamp as Date,
# type, 
# reason,
# row_number() over(partition by accountId order by asoftimestamp desc) as CLR_COUNT
# from neo_raw_production.bank_service_credit_limit_adjustments where type = 'CREDIT_LIMIT' and reason in('CREDIT_LIMIT_RESTRICTION',
# 'CREDIT_LIMIT_RESTRICTION_REVERSED')
# ''') 
# CLR.createOrReplaceTempView("CLR")

# CLR_V2 = spark.sql('''
# select *,
# case when reason='CREDIT_LIMIT_RESTRICTION_REVERSED' then datediff(Month, Date, Now()) else NULL end as Time_Since_CLR_Removed from CLR 
# where CLR_COUNT = 1
# ''') 
# CLR_V2.createOrReplaceTempView("CLR_V2")

CLR_V3 = spark.sql('''
select
  l.accountId
   , count(*) filter (where l.reason = 'CREDIT_LIMIT_RESTRICTION') as clrNumRestrictions
   , count(*) filter (where l.reason = 'CREDIT_LIMIT_RESTRICTION_REVERSED') as clrNumReversed
   , max(l.asOfTimestamp) filter (where l.reason = 'CREDIT_LIMIT_RESTRICTION_REVERSED') as clrLastReversedAt
   , count(*) filter (where l.reason = 'HRAM_CREDIT_LIMIT_RESTRICTION') as clrNumHramRestrictions
   , count(*) filter (where l.reason = 'HRAM_CREDIT_LIMIT_RESTRICTION_REVERSED') as clrNumHramReversed
   , max(l.asOfTimestamp) filter (where l.reason = 'HRAM_CREDIT_LIMIT_RESTRICTION_REVERSED') as clrLastHramReversedAt
from
  neo_raw_production.bank_service_credit_limit_adjustments l 
where
  l.type = 'CREDIT_LIMIT'
group by all
''')
CLR_V3.createOrReplaceTempView("CLR_V3")


CLI_Eligible_Master_Data5 = spark.sql('''
select
  CLI_Eligible_Master_Data4.*
  , ( case 
        when CLR_V3.clrNumRestrictions > CLR_V3.clrNumReversed or CLR_V3.clrNumHramRestrictions > CLR_V3.clrNumHramReversed
          then 1
        when timestampdiff(day, CLR_V3.clrLastHramReversedAt, current_date + interval 8 hours)  <= 180
             or timestampdiff(day, CLR_V3.clrLastReversedAt,  current_date + interval 8 hours)  <= 180
          then 2
        else 0
      end
  ) as CLR_Flag
  --, case when CLR_COUNT = 1 and reason='CREDIT_LIMIT_RESTRICTION' then 1
  --  when CLR_COUNT = 1 and reason='CREDIT_LIMIT_RESTRICTION_REVERSED' and Time_Since_CLR_Removed<6 then 1 else 0 end as CLR_Flag,
  , now() as adjudicatedAt
from
  CLI_Eligible_Master_Data4
  left join CLR_V3
    on CLR_V3.accountId = CLI_Eligible_Master_Data4.accountId
''')
CLI_Eligible_Master_Data5.createOrReplaceTempView('CLI_Eligible_Master_Data5')

# COMMAND ----------

# MAGIC %sql  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Active CLD Pass Through

# COMMAND ----------

Active_CLD_Pass = spark.sql('''
    SELECT 
        t1.*,
        newcreditLimitCents AS New_Limit_Cents,
        CASE 
            WHEN Latest_Product_Category = 'NEO' AND Province_of_Account = 'Other' THEN 'NEO-ROC-ACLD-RCTV'
            WHEN Latest_Product_Category = 'NEO' AND Province_of_Account = 'Quebec' THEN 'NEO-QC-ACLD-RCTV'
            WHEN Latest_Product_Category = 'HBC' AND Province_of_Account = 'Other' THEN 'HBC-ROC-ACLD-RCTV'
            WHEN Latest_Product_Category = 'HBC' AND Province_of_Account = 'Quebec' THEN 'HBC-QC-ACLD-RCTV'
            WHEN Latest_Product_Category = 'HBC' AND Province_of_Account = 'Other' THEN 'HBC-ROC-ACLD-RCTV' 
        END AS Offer_Code
    FROM 
        CLI_Eligible_Master_Data5 AS t1
    INNER JOIN 
        (SELECT 
            creditAccountId AS creditAccountId,
            ROUND(previousCreditLimitCents / 100.0, -2) * 100 AS newcreditLimitCents,
            initiatedAt AS InitiatedAt
        FROM 
            neo_raw_production.credit_limit_service_credit_limit_decrease_offers decr
        WHERE 
            creditLimitOfferStrategy IN ('LOW-UTIL')
            -- KG 2024-11-26
            and not exists
                (select 1 
                from neo_raw_production.credit_limit_service_credit_limit_offers c
                where c.creditAccountId = decr.creditAccountId and c.acceptedAt > decr.initiatedAt
                )
        ) AS t2
    ON 
        t1.accountId = t2.creditAccountId 
        AND InitiatedAt >= NOW() - INTERVAL 30 DAY
    WHERE 
        Not_Frozen_Indicator = 1  AND Policy_Exclusion_Flag = 0 AND Currently_Delinquent_Flag = 0  AND Bureau_Score_Clean >= 600 AND GO26 = 'H' AND Current_Account_Standing = 'Account_OPEN_Not_Frozen' AND (Months_Since_CLI_Offer IS NULL OR Months_Since_CLI_Offer >= 3)
''')
Active_CLD_Pass.createOrReplaceTempView('Active_CLD_Pass')


# COMMAND ----------

# MAGIC %md # 14. Line Assigments and Segments

# COMMAND ----------

Segment_One = spark.sql('''
select 
t1.*
from(
select 
  distinct CLI_Eligible_Master_Data5.*,
  case
    when  User_Type_Flag in ('Revolver','Transactor')
      and Not_Frozen_Indicator = 1
      and Policy_Exclusion_Flag = 0
      and Currently_Delinquent_Flag = 0
      and Last_3M_Delinquent_Flag = 0
      and Payment_Bal_Flag = 1
      and Inquiry_Flag = 0
      and Bureau_Score_Clean>=600
      and Bureau_Score_Clean<700
      and MOB_Rounded >=6
      and CLI_In_Last_3_Months = 0 -- (Last_CLI_Months>=3 or Last_CLI_Months is NULL) KG June 2025
      and Bureau_Segment_Flag = 0
      and Credit_Limit > 100
      and Credit_Limit <= 3000
      and AM36_Flag = 1
      and CLD_Block=1
      -- and CLI_Offered_In_Last_3_Months = 0 KG June 2025
      and months_between(current_date(), Statement_Close_Date) < 2
      -- and Prior_Control_Group = 0
      -- and Field_Of_Study=0
      and credit_account_type = 'UNSECURED'
      and CLR_Flag = 0
      and GO26 = 'H'
      and Current_Account_Standing='Account_OPEN_Not_Frozen'
    then 'Sub_Prime_Segement'

    when  User_Type_Flag ='Transactor'
      --and Transactor_Flag=1
      and Not_Frozen_Indicator = 1
      and Policy_Exclusion_Flag = 0
      and Currently_Delinquent_Flag = 0
      and Last_3M_Delinquent_Flag = 0
      and Payment_Bal_Flag = 1
      and Inquiry_Flag = 0
      and Bureau_Score_Flag = 1
      and Bureau_Score_Clean >= 700
      and Bureau_Score_Clean <= 900
      and MOB_Rounded >= 3
      and CLI_In_Last_3_Months = 0 -- (Last_CLI_Months>=3 or Last_CLI_Months is NULL) KG June 2025
      and Bureau_Segment_Flag = 0
      and Credit_Limit > 100
      and Credit_Limit <= 9000
      and AM36_Flag = 1
      and CLD_Block=1
      -- and CLI_Offered_In_Last_3_Months = 0 KG June 2025
      and months_between(current_date(), Statement_Close_Date) < 2
      -- and Prior_Control_Group = 0
      -- and Field_Of_Study=0
      and credit_account_type = 'UNSECURED'
      and CLR_Flag = 0
      and GO26 = 'H'
      and Current_Account_Standing='Account_OPEN_Not_Frozen'
    then 'Transactor_Segement'
    
  when  User_Type_Flag='Revolver'
      and Not_Frozen_Indicator = 1
      and Policy_Exclusion_Flag = 0
      and Currently_Delinquent_Flag = 0
      and Last_3M_Delinquent_Flag = 0
      and Payment_Bal_Flag = 1
      and Inquiry_Flag = 0
      and Bureau_Score_Flag = 1
      and Bureau_Score_Clean>=700
      and Bureau_Score_Clean<=900
      and MOB_Rounded >=3
      and CLI_In_Last_3_Months = 0 -- (Last_CLI_Months>=3 or Last_CLI_Months is NULL) KG June 2025
      and Bureau_Segment_Flag = 0
      and Credit_Limit > 100
      and Credit_Limit <=9000
      and AM36_Flag = 1
      and CLD_Block=1
      -- and CLI_Offered_In_Last_3_Months = 0 KG June 2025
      and months_between(current_date(), Statement_Close_Date) < 2
      -- and Prior_Control_Group = 0
      -- and Field_Of_Study=0
      and credit_account_type = 'UNSECURED'
      and CLR_Flag = 0
      and GO26 = 'H'
      and Current_Account_Standing='Account_OPEN_Not_Frozen'
    then 'Revolver_Segment'
    else
      NULL
  end as CLI_Segment_Indicator
from
  CLI_Eligible_Master_Data5) as t1
left join 
  Active_CLD_Pass as t2
on
  t1.accountId=t2.accountId
where 
  t1.Account_Latest_Entry = 1 
  and t2.accountId is NULL
''')
Segment_One.createOrReplaceTempView("Segment_One")

Segment_Two = spark.sql('''
  select 
    *,
    case 
       -- KG 2024-12-12
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit > 100 and Credit_Limit <= 500 then Credit_Limit + 500 -- KG
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit <= 1500 and Bureau_Score_Clean <  800 then (Credit_Limit * 2)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit <= 1500 and Bureau_Score_Clean >= 800 then (Credit_Limit * 3)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  1500 and Credit_Limit <= 5000 and Bureau_Score_Clean <  800 then (Credit_Limit * 1.5)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  1500 and Credit_Limit <= 5000 and Bureau_Score_Clean >= 800 then (Credit_Limit * 2)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  5000 and Credit_Limit <= 7500 and Bureau_Score_Clean <  800 then (Credit_Limit * 1.25)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  5000 and Credit_Limit <= 7500 and Bureau_Score_Clean >= 800 then (Credit_Limit * 1.5)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  7500 then (Credit_Limit + 1000)

      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=1 then Old_Limit
      
      -- KG 2024-12-12
      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=0 and (Credit_Limit) > 100   and  (Credit_Limit) <= 500   then Credit_Limit + 500 -- KG * 1.50
      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=0 and (Credit_Limit) > 500  and  (Credit_Limit) <= 1000  then Credit_Limit + 1000
      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=0 and (Credit_Limit) > 1000  and  (Credit_Limit) <= 2000  then Credit_Limit + 1500
      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=0 and (Credit_Limit) > 2000  and  (Credit_Limit) <= 3000  then Credit_Limit + 2000

      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=1 then Old_Limit

      -- KG 2024-12-12
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit > 100 and Credit_Limit <= 500 then Credit_Limit + 500 -- KG
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit <= 1500 and Bureau_Score_Clean <  800 then (Credit_Limit * 2)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit <= 1500 and Bureau_Score_Clean >= 800 then (Credit_Limit * 3)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and credit_Limit >  1500 and Credit_Limit <= 5000 and Bureau_Score_Clean <  800 then (Credit_Limit * 1.5)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  1500 and Credit_Limit <= 5000 and Bureau_Score_Clean >= 800 then (Credit_Limit * 2)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  5000 and Credit_Limit <= 7500 and Bureau_Score_Clean <  800 then (Credit_Limit * 1.25)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  5000 and Credit_Limit <= 7500 and Bureau_Score_Clean >= 800 then (Credit_Limit * 1.5)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  7500 then (Credit_Limit + 1000)

      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=1 then Old_Limit
    end as Cons_Credit_Limit,

    case
      -- KG 2024-12-12
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit > 100 and Credit_Limit <= 500 then Credit_Limit + 500 -- KG
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit <= 1500 and Bureau_Score_Clean <  800 then (Credit_Limit * 2.5)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit <= 1500 and Bureau_Score_Clean >= 800 then (Credit_Limit * 3.5)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  1500 and Credit_Limit <= 5000 and Bureau_Score_Clean <  800 then (Credit_Limit * 2)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  1500 and Credit_Limit <= 5000 and Bureau_Score_Clean >= 800 then (Credit_Limit * 2.5)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  5000 and Credit_Limit <= 7500 and Bureau_Score_Clean <  800 then (Credit_Limit * 1.25)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  5000 and Credit_Limit <= 7500 and Bureau_Score_Clean >= 800 then (Credit_Limit * 1.5)
      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=0 and Credit_Limit >  7500 then (Credit_Limit + 1000)

      when CLI_Segment_Indicator='Transactor_Segement' and CLD_Population=1 then Old_Limit
      
      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=0 and (Credit_Limit) > 100   and  (Credit_Limit) <= 500   then Credit_Limit + 500 -- KG * 2
      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=0 and (Credit_Limit) > 500  and  (Credit_Limit) <= 1000  then Credit_Limit + 1500
      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=0 and (Credit_Limit) > 1000  and  (Credit_Limit) <= 2000  then Credit_Limit + 2000
      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=0 and (Credit_Limit) > 2000  and  (Credit_Limit) <= 3000  then Credit_Limit + 2500

      when CLI_Segment_Indicator='Sub_Prime_Segement' and CLD_Population=1 then Old_Limit

      -- KG 2024-12-12
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit > 100 and Credit_Limit <= 500 then Credit_Limit + 500 -- KG
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit <= 1500 and Bureau_Score_Clean <  800 then (Credit_Limit * 2.5)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit <= 1500 and Bureau_Score_Clean >= 800 then (Credit_Limit * 3.5)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  1500 and Credit_Limit <= 5000 and Bureau_Score_Clean <  800 then (Credit_Limit * 2)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  1500 and Credit_Limit <= 5000 and Bureau_Score_Clean >= 800 then (Credit_Limit * 2.5)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  5000 and Credit_Limit <= 7500 and Bureau_Score_Clean <  800 then (Credit_Limit * 1.25)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  5000 and Credit_Limit <= 7500 and Bureau_Score_Clean >= 800 then (Credit_Limit * 1.5)
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=0 and Credit_Limit >  7500 then (Credit_Limit + 1000)
      
      when CLI_Segment_Indicator='Revolver_Segment' and CLD_Population=1 then Old_Limit
      
       else 0 end as Agg_Credit_Limit
  from
    Segment_One 


''')
Segment_Two.createOrReplaceTempView("Segment_Two")


# COMMAND ----------

# MAGIC %md #15.Adding In Line Assignment Strategy

# COMMAND ----------



# COMMAND ----------

Limit_Offer = spark.sql('''
  select 
    *,
    case when Segment=1 then 'Aggressive' when Segment=2 then 'Conservative' end as Strategy_Type,
    Case when segment=1 then (ceiling(Agg_Credit_Limit/100)*100)*100 when Segment=2 then (ceiling(Cons_Credit_Limit/100)*100)*100 end as New_Limit_Cents
  from (
    select 
      *,
      NTILE (2) OVER (
		ORDER BY accountId) As Segment
    from 
      Segment_Two)
''')
Limit_Offer.createOrReplaceTempView("Limit_Offer")




# COMMAND ----------

# MAGIC %md #17. Final Table 

# COMMAND ----------

Final_Table = spark.sql('''
  select 
    distinct accountId,
    userId,
    Months_Since_CLI_Offer,
    CLI_Offered_In_Last_3_Months,
    Transactor_Flag,
    User_Type_Flag,
    Sub_Prime_Flag,
    Not_Frozen_Indicator,
    Policy_Exclusion_Flag,
    Currently_Delinquent_Flag,
    Last_3M_Delinquent_Flag,
    Payment_Bal_Flag,
    Inquiry_Flag,
    AM36_Flag,
    SP_Limit_Flag,
    Bureau_Score_Flag,
    Bureau_Segment_Flag,
    Account_Latest_Entry,
    CLI_In_Last_3_Months,
    credit_account_type,
    credit_limit_yesterday,
    Current_Credit_Score,
    day_end_status_yesterday,
    user_frozen_reason_yesterday,
    credit_delinquency_service_days_past_due as day_end_days_past_due,
    credit_delinquency_service_reporting_charge_off_type as day_end_charge_off_category,
    Current_Account_Standing,
    Latest_Product_Category,
    Credit_Facility_Name,
    Latest_Balance_Of_Account,
    account_is_overlimit,
    account_is_overlimit_over_20p,
    Delinquent_Past_100Days,
    Prior_Control_Group,
    credit_account_type,
    GO26,
    Field_Of_Study,
    Province_of_Account,
    CLR_Flag,
    Active_Status,
    CLI_Segment_Indicator,
    Strategy_Type,
    New_Limit_Cents,
    case when (CLI_Segment_Indicator is NULL or right(t2.tenantId,1)=1) then 0 else 1 end as Elg_Account,
    -- KG June 2025
    MOB_Rounded,
    Credit_Limit,
    CLD_Block,
    Bureau_Score_Clean
  FROM
    Limit_Offer as t1
  left join
    neo_raw_production.user_service_users as t2
  on
    t1.userId=t2._id
  
  ''')
Final_Table.createOrReplaceTempView("Final_Table")
# Final_Table.display()

# COMMAND ----------

# MAGIC %md # 18. Populate Final Table

# COMMAND ----------

Push_Table = spark.sql('''
 -- KG 2024-08-06, 2024-12-06, 2024-12-12: no multicredit, no World / World Elite
select
  t.accountId
  , t.userId
  , t.Months_Since_CLI_Offer
  , t.CLI_Offered_In_Last_3_Months
  , t.Transactor_Flag
  , t.User_Type_Flag
  , t.Sub_Prime_Flag
  , t.Not_Frozen_Indicator
  , t.Policy_Exclusion_Flag
  , t.Currently_Delinquent_Flag
  , t.Last_3M_Delinquent_Flag
  , t.Payment_Bal_Flag
  , t.Inquiry_Flag
  , t.AM36_Flag
  , t.SP_Limit_Flag
  , t.Bureau_Score_Flag
  , t.Bureau_Segment_Flag
  , t.Account_Latest_Entry
  , t.CLI_In_Last_3_Months
  , t.credit_account_type
  , t.credit_limit_yesterday
  , t.Current_Credit_Score
  , t.day_end_status_yesterday
  , t.user_frozen_reason_yesterday
  , t.day_end_days_past_due
  , t.day_end_charge_off_category
  , t.Current_Account_Standing
  , t.Latest_Product_Category
  , t.Credit_Facility_Name
  , t.Latest_Balance_Of_Account
  , t.account_is_overlimit
  , t.account_is_overlimit_over_20p
  , t.Delinquent_Past_100Days
  , t.Prior_Control_Group
  , t.GO26
  , t.Field_Of_Study
  , t.Province_of_Account
  , t.CLR_Flag
  , t.Active_Status
  -- KG 2024-08-06, 2024-12-06, 2024-12-12: no multicredit, no World / World Elite, no International
  -- KG 2025-03-8 HBC
  , (case
      when t.latest_product_category = 'HBC'
        then 'HBC'
      when acc.numUnsecuredAccounts > 1
        then 'MultiCredit'
      when acc.numWorldAccounts > 0
        then 'World / World Elite'
      when docs.issuedBy not like 'CA%' -- null (no info on identity docs) will not trigger the rule!
        then 'International'
      else
        t.CLI_Segment_Indicator
    end
    ) as CLI_Segment_Indicator
  , t.Strategy_Type
  -- KG 2024-08-06, 2024-12-06, 2024-12-12: no multicredit, no World / World Elite, no International
  -- KG 2025-03-8 HBC
  , (case
      when t.latest_product_category = 'HBC'
        then 0
      when acc.numUnsecuredAccounts > 1
        then 0
      when acc.numWorldAccounts > 0
        then 0
      when docs.issuedBy not like 'CA%' -- null (no info on identity docs) will not trigger the rule!
        then 0
      else
        t.Elg_Account
    end
    ) as Elg_Account
  -- KG 2024-08-06, 2024-12-06, 2024-12-12: no multicredit, no World / World Elite, no International
  -- KG 2025-03-8 HBC
  , (case
      when t.latest_product_category = 'HBC'
        then 0
      when acc.numUnsecuredAccounts > 1
        then 0
      when acc.numWorldAccounts > 0
        then 0
      when docs.issuedBy not like 'CA%' -- null (no info on identity docs) will not trigger the rule!
        then 0
      else
        t.New_Limit_Cents
    end
    ) as New_Limit_Cents
  -- KG 2024-08-06, 2024-12-06, 2024-12-12: no multicredit, no World / World Elite, no International
  -- KG 2025-03-8 HBC
  , (case
      when t.latest_product_category = 'HBC'
        then null
       when acc.numUnsecuredAccounts > 1
        then null
      when acc.numWorldAccounts > 0
        then null
      when docs.issuedBy not like 'CA%' -- null (no info on identity docs) will not trigger the rule!
        then null
      else
        t.Offer_Code
    end
    ) as Offer_Code
  , t.AdjudicatedAt
  -- KG June 2025
  , t.MOB_Rounded
  , t.Credit_Limit
  , t.CLD_Block
  , t.Bureau_Score_Clean 
from
  (select 
    accountId,
    userId,
    Months_Since_CLI_Offer,
    CLI_Offered_In_Last_3_Months,
    Transactor_Flag,
    User_Type_Flag,
    Sub_Prime_Flag,
    Not_Frozen_Indicator,
    Policy_Exclusion_Flag,
    Currently_Delinquent_Flag,
    Last_3M_Delinquent_Flag,
    Payment_Bal_Flag,
    Inquiry_Flag,
    AM36_Flag,
    SP_Limit_Flag,
    Bureau_Score_Flag,
    Bureau_Segment_Flag,
    Account_Latest_Entry,
    CLI_In_Last_3_Months,
    credit_account_type,
    cast(credit_limit_yesterday as decimal(23,2)) as credit_limit_yesterday,
    Current_Credit_Score,
    day_end_status_yesterday,
    user_frozen_reason_yesterday,
    day_end_days_past_due,
    day_end_charge_off_category,
    Current_Account_Standing,
    Latest_Product_Category,
    Credit_Facility_Name,
    Latest_Balance_Of_Account,
    account_is_overlimit,
    account_is_overlimit_over_20p,
    Delinquent_Past_100Days,
    Prior_Control_Group,
    GO26,
    Field_Of_Study,
    Province_of_Account,
    CLR_Flag,
    Active_Status,
    CLI_Segment_Indicator,
    Strategy_Type,
    Elg_Account,
    ifNULL(case when New_Limit_Cents>1000000  and Elg_Account=1 then 1000000
    when  Elg_Account=1 then New_Limit_Cents else 0 end,0) as New_Limit_Cents,
    case when Latest_Product_Category='NEO' and Province_of_Account='Other' and CLI_Segment_Indicator='Sub_Prime_Segement' then 'NEO-ROC-LWGR-RCTV'
    when Latest_Product_Category='NEO' and Province_of_Account='Quebec' and CLI_Segment_Indicator='Sub_Prime_Segement' then 'NEO-QC-LWGR-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Other' and CLI_Segment_Indicator='Sub_Prime_Segement' then 'HBC-ROC-LWGR-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Quebec' and CLI_Segment_Indicator='Sub_Prime_Segement' then 'HBC-QC-LWGR-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Other' and CLI_Segment_Indicator='Sub_Prime_Segement' then 'HBC-ROC-LWGR-RCTV'

    when Latest_Product_Category='NEO' and Province_of_Account='Other' and CLI_Segment_Indicator='Transactor_Segement' then 'NEO-ROC-TRNS-RCTV'
    when Latest_Product_Category='NEO' and Province_of_Account='Quebec' and CLI_Segment_Indicator='Transactor_Segement' then 'NEO-QC-TRNS-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Other' and CLI_Segment_Indicator='Transactor_Segement' then 'HBC-ROC-TRNS-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Quebec' and CLI_Segment_Indicator='Transactor_Segement' then 'HBC-QC-TRNS-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Other' and CLI_Segment_Indicator='Transactor_Segement' then 'HBC-ROC-TRNS-RCTV'

    when Latest_Product_Category='NEO' and Province_of_Account='Other' and CLI_Segment_Indicator='Revolver_Segment' then 'NEO-ROC-RVLV-RCTV'
    when Latest_Product_Category='NEO' and Province_of_Account='Quebec' and CLI_Segment_Indicator='Revolver_Segment' then 'NEO-QC-RVLV-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Other' and CLI_Segment_Indicator='Revolver_Segment' then 'HBC-ROC-RVLV-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Quebec' and CLI_Segment_Indicator='Revolver_Segment' then 'HBC-QC-RVLV-RCTV'
    when Latest_Product_Category='HBC' and Province_of_Account='Other' and CLI_Segment_Indicator='Revolver_Segment' then 'HBC-ROC-RVLV-RCTV' end as Offer_Code,
    now() as AdjudicatedAt,
    -- KG June 2025
    MOB_Rounded,
    Credit_Limit,
    CLD_Block,
    Bureau_Score_Clean 
  from  
    Final_Table
  union 
  select 
  accountId,
    userId,
    Months_Since_CLI_Offer,
    CLI_Offered_In_Last_3_Months,
    Transactor_Flag,
    User_Type_Flag,
    Sub_Prime_Flag,
    Not_Frozen_Indicator,
    Policy_Exclusion_Flag,
     Currently_Delinquent_Flag,
     Last_3M_Delinquent_Flag,
     Payment_Bal_Flag,
     Inquiry_Flag,
     AM36_Flag,
     SP_Limit_Flag,
     Bureau_Score_Flag,
     Bureau_Segment_Flag,
     Account_Latest_Entry,
     CLI_In_Last_3_Months,
     credit_account_type,
     cast(credit_limit_yesterday as decimal(23,2)) as credit_limit_yesterday,
     Current_Credit_Score,
     day_end_status_yesterday,
     user_frozen_reason_yesterday,
    credit_delinquency_service_days_past_due as day_end_days_past_due,
    credit_delinquency_service_reporting_charge_off_type as day_end_charge_off_category,
     Current_Account_Standing,
     Latest_Product_Category,
     Credit_Facility_Name,
     Latest_Balance_Of_Account,
     account_is_overlimit,
     account_is_overlimit_over_20p,
     Delinquent_Past_100Days,
     Prior_Control_Group,
     GO26,
     Field_Of_Study,
     Province_of_Account,
     CLR_Flag,
     Active_Status,
     'Active CLD' as CLI_Segment_Indicator,
     'Active CLD' as Strategy_Type,
      1 as Elg_Account,
      case when cast(New_Limit_Cents as decimal(32,0))<50000 then 50000 else cast(New_Limit_Cents as decimal(32,0)) end as New_Limit_Cents,
      Offer_Code,
      AdjudicatedAt,
      -- KG June 2025
      MOB_Rounded,
      Credit_Limit,
      CLD_Block,
      Bureau_Score_Clean 
      from 
    Active_CLD_Pass
  ) t
 -- KG 2024-08-06, 2024-12-06, 2024-12-12: no multicredit, no World / World Elite, no International
 -- KG 2024-03-08 HBC
  left outer join 
    (
      select
        ra.userId as UserId
        , count(*) /*filter (where ra.securedCreditDepositAccountId is null) */ as numUnsecuredAccounts -- unsecured only
        , count(*) filter (where tier like 'WORLD%') as numWorldAccounts
        -- , count(*) filter (where size(completedProductSwaps) > 0)
      from
        neo_raw_production.credit_account_service_credit_accounts ra
      where
        ra.status = 'OPEN' and ra.securedCreditDepositAccountId is null -- we don't need secured accounts
      group by all  
    ) acc 
      on t.userId = acc.userId
  left outer join
    ( 
      select 
        vd.userId
        -- , vd.idIssuer
        -- , vd.idType
        -- , vd.issuingCountry
        , nvl(vd.issuingCountry, vd.idIssuer) as issuedBy
        -- , vd.issuingJurisdiction
        -- , vd.completedAt
      from
        hive_metastore.neo_raw_production.identity_service_id_verification_documents vd 
      qualify
        row_number() over (partition by vd.userId order by vd.completedAt desc) = 1
    ) docs
      on docs.userId = t.userId
''')
Push_Table.createOrReplaceTempView("Push_Table") 

# COMMAND ----------

   # Push_Table.write.mode("append").saveAsTable("neo_views_credit_risk.Reactive_Test_Table")

# COMMAND ----------

Push_Table.write.format("delta").mode("append").saveAsTable("neo_views_credit_risk.Reactive_CLI_Eligibility_V2")
