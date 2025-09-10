
--join request table and offer table
create or replace temporary table sandbox.yuanlin_chen.rCLI as (
select
request_tb._id
,request_tb.userid
,request_tb.CREDITACCOUNTID
,request_tb.CREDITLIMITOFFERID
,request_tb.createdat
,left(request_tb.createdat,7) as CLI_vintage
,case when offer_tb._id is not null then 'Approved'
        else 'Not Approved' end as approve_status
,offer_tb.CREDITLIMITOFFERSTRATEGY
,case when approve_status = 'Not Approved' then 'Not Approved'
when CREDITLIMITOFFERSTRATEGY like '%QC%' or CREDITLIMITOFFERSTRATEGY like '%PRTV%' then 'not in V2 strategy'
when declinedat is not null then 'Declined'
when acceptedat is not null then 'Accepted'
when datediff(day,offer_tb.createdat,GETDATE()) < 31 then 'In progress'
else 'Expired' end as accept_status
,offer_tb.DECLINEDAT
,offer_tb.ACCEPTEDAT
,offer_tb.ACTIONEDBY
,offer_tb.CREDITLIMITCENTSATOFFERTIME / 100 as ICL
-- ,(offer_tb.CREDITLIMITCENTS - offer_tb.CREDITLIMITCENTSATOFFERTIME) / 100 as CLI_OFFER 
,(offer_tb.OFFEREDCREDITLIMITCENTS - offer_tb.CREDITLIMITCENTSATOFFERTIME) / 100 as CLI_OFFER 
from 
REFINED.CREDITLIMITSERVICE.CREDITLIMITCHANGEREQUESTS request_tb
left join
REFINED.CREDITLIMITSERVICE.CREDITLIMITOFFERS offer_tb
on request_tb.CREDITLIMITOFFERID = offer_tb._id
---join cli table for TU, MOB, DQ status
where 1=1
and request_tb.createdat < '2025-08-01'
order by createdat desc
)
;

create or replace temporary table sandbox.yuanlin_chen.rCLI_PMMO as (
select
*
,coalesce(ICL, PMMO.CL) as ICL_adj
,DATEDIFF(month, coalesce(acceptedat, declinedat,createdat), TO_DATE(STMT_MTH || '-01', 'YYYY-MM-DD')) AS CLI_STMT
,DATEDIFF(month, BOOKING_DATE, TO_DATE(STMT_MTH || '-01', 'YYYY-MM-DD')) AS MOB
, case when ICL_adj <= 1500 then '1, <=1.5k ICL'
else '2,>1.5k ICL'
end as ICL_BUCKET
, case when APP_TU_BAND in ('1. SUBPRIME','2. NEAR_PRIME','3. PRIME') then '1, Low TU'
when APP_TU_BAND in ('4. PRIME_PLUS', '5. SUPER_PRIME') then '2, High TU'
end as TU_BUCKET
from
sandbox.yuanlin_chen.rCLI rCLI
LEFT JOIN 
(
    SELECT
    ACCOUNTID as ACCOUNTID_PMMO
    ,VINTAGE
    ,STMT_NUM
    ,STMT_MTH
    ,CLI_ACCEPTED_FLAG
    ,earl_originalbrand as BRAND
    ,ORIGINAL_CARD_TYPE
    ,APP_TU_BAND
    ,APP_TU_SCORE
    ,APP_SEG
    ,BFC - COF - GUCO as RAIM
    ,BOOKING_DATE
    ,DPD_BUCKET
    ,CL
    FROM 
    SANDBOX.MARKWANG.PM_MONITORING 
    where 1=1
    and secure_type = 'UNSECURED'
)PMMO
ON rCLI.CREDITACCOUNTID = PMMO.ACCOUNTID_PMMO
)
;

-----
--- expired deepdive
select
-- left(createdat,7) as CLI_vintage
-- CREDITLIMITOFFERSTRATEGY
case when ICL_adj <= 100 then '1, <=100'
when ICL_adj <=1500  then '2, 100-1500'
when ICL_adj <=3000 then '3,1500-3000'
when ICL_adj <=7000 then '4,3000-7000'
else '5,>7000'
end as ICL_BUCKET
-- APP_TU_BAND
-- DPD_BUCKET
-- case when MOB <=3 then '1,0-3'
-- when MOB <=6 then '2,4-6'
-- when MOB <=12 then '3,7-12'
-- when MOB <=24 then '4,13-24'
-- when MOB <=36 then '5,25-36'
-- else '6,>36' end as MOB_BUCKET
,count(case when accept_status = 'Expired' then _id else null end) as expired_count
,count(_id) as total_count
from
sandbox.yuanlin_chen.rCLI_PMMO
where 1=1
and CLI_STMT = 0
and approve_status = 'Approved'
group by 1
having expired_count > 0
order by 1
;

--- grid
select
case when ICL_adj <= 1500 then '1, <=1.5k ICL'
else '2,>1.5k ICL'
end as ICL_BUCKET
, case when APP_TU_BAND in ('1. SUBPRIME','2. NEAR_PRIME','3. PRIME') then '1, Low TU'
when APP_TU_BAND in ('4. PRIME_PLUS', '5. SUPER_PRIME') then '2, High TU'
end as TU_BUCKET
, case when MOB <=12 then '1, <= 1yr MOB'
when MOB <= 24 then '2, <= 2yr MOB'
else '3, >2yr MOB'
end as MOB_BUCKET
-- ,count(case when accept_status = 'Expired' then _id else null end) as expired_count
-- ,count(_id) as total_count
,count(case when approve_status = 'Approved' then _id else null end) as approved_count
,count(_id) as total_count
 from 
 sandbox.yuanlin_chen.rCLI_PMMO
 where 1=1
 and CLI_STMT = 0
 -- and approve_status = 'Approved'
 group by 1,2,3
 order by 1,2,3
 ;


--- Inactive CLD rCLI deep dive
WITH rcli AS (select
rcli.*
,cld.CREDITLIMITOFFERSTRATEGY
,cld.INITIATEDAT
,CLD2024_flag
from 
sandbox.yuanlin_chen.rCLI_PMMO rcli
left join
REFINED.CREDITLIMITSERVICE.CREDITLIMITDECREASEOFFERS cld
on rcli.CREDITACCOUNTID = cld.creditaccountid
left join 
(select
*, 1 as CLD2024_flag
from
SANDBOX.MARKWANG.INACTIVE_CLD_MW
) CLD_MW
on rcli.CREDITACCOUNTID = CLD_MW.accountid
where 1=1
and CLI_STMT = 0
and ICL <= 1500
and CLI_OFFER > 4000
and APPROVE_STATUS = 'Approved'
and cld.CREDITLIMITOFFERSTRATEGY is Null
and CLD2024_FLAG is Null
)
SELECT
rcli._ID
,rcli.userid
,rcli.CREDITACCOUNTID
,rcli.CREDITLIMITOFFERID
,rcli.STMT_MTH as rCLI_MTH
,rcli.ICL
,rCLI.CLI_OFFER
,CLI.OFFER_MTH
,CLI.CURRENT_CL
,CLI.NEW_CL
,CLI.INC_EXPOSURE
,CLI.CLIPROGRAM
FROM rcli
left join
(
select
*
from
SANDBOX.MARKWANG.VW_CLI_
where 1=1
and accepted_flag =1
)CLI
on rcli.CREDITACCOUNTID = CLI.ACCOUNTID
--- 
where 1=1
-- and creditaccountid = '60972466fe128c4d8649956a'
order by 1,2,3,4,offer_mth
;

-- check no CLD record number
select
rcli.*
,cld.CREDITLIMITOFFERSTRATEGY
,cld.INITIATEDAT
,CLD2024_flag
from 
sandbox.yuanlin_chen.rCLI_PMMO rcli
left join
REFINED.CREDITLIMITSERVICE.CREDITLIMITDECREASEOFFERS cld
on rcli.CREDITACCOUNTID = cld.creditaccountid
left join 
(select
*, 1 as CLD2024_flag
from
SANDBOX.MARKWANG.INACTIVE_CLD_MW
) CLD_MW
on rcli.CREDITACCOUNTID = CLD_MW.accountid
where 1=1
and CLI_STMT = 0
and ICL <= 1500
and CLI_OFFER > 4000
and APPROVE_STATUS = 'Approved'
and cld.CREDITLIMITOFFERSTRATEGY is Null
and CLD2024_FLAG is Null
-- and rcli.CREDITLIMITOFFERSTRATEGY like '%PRTV%'
;

-- check if rCLI number = pre inactive CLD, according to CLD table
select
rcli._ID
,rcli.userid
,rcli.CREDITACCOUNTID
,rcli.CREDITLIMITOFFERID
,rcli.STMT_MTH as rCLI_MTH
,rcli.ICL
,rCLI.CLI_OFFER
,ICL+CLI_OFFER as rCLI_new_CL
,cld.INITIATEDAT
,cld.PREVIOUSCREDITLIMITCENTS/100 as CLD_prev_cl
,cld.NEWCREDITLIMITCENTS/100 as CLD_new_cl
from 
sandbox.yuanlin_chen.rCLI_PMMO rcli
left join
REFINED.CREDITLIMITSERVICE.CREDITLIMITDECREASEOFFERS cld
on rcli.CREDITACCOUNTID = cld.creditaccountid
where 1=1
and CLI_STMT = 0
and ICL <= 1500
and CLI_OFFER > 4000
and APPROVE_STATUS = 'Approved'
and initiatedat is not Null
and (ICL+CLI_OFFER) > CLD_prev_cl
-- and RCLI_MTH >= left(INITIATEDAT,7)
;

-----
-----
-----eligibility deep dive
create or replace temporary table sandbox.yuanlin_chen.rCLI_elig as (
select
*
from
(
    select
    *
    ,row_number() over (partition by _ID order by ADJUDICATEDAT asc) as row_count
    from
        (
        select
        _id
        ,CREDITACCOUNTID
        ,CREDITLIMITOFFERID
        ,createdat
        ,approve_status
        ,CLI_vintage
        ,CREDITLIMITOFFERSTRATEGY
        ,accept_status
        ,DECLINEDAT
        ,ACCEPTEDAT
        ,ACTIONEDBY
        ,ICL_adj
        ,MOB
        ,CLI_OFFER 
        ,ICL_BUCKET
        ,TU_BUCKET
        from
        sandbox.yuanlin_chen.rCLI_PMMO
        where 1=1
        and CLI_STMT = 0
        )rcli
    left join 
        CX.CUSTOMER.REACTIVE_CLI_ELIGIBILITY_V2 elig
    on rcli.creditaccountid = elig.accountid
    and elig.ADJUDICATEDAT > rcli.createdat 
    where 1=1 
    and createdat > '2024-12-31'
)
where row_count = 1
)
;

---detailed
select
case when Not_Frozen_Indicator <> 1 then '01' -- s
when policy_exclusion_flag <> 0 then '02'
when Currently_Delinquent_Flag <> 0 then '03'
when Last_3M_Delinquent_Flag <>0 then '04' -- s
when Payment_Bal_Flag <> 1 then '05'
when Inquiry_Flag <> 0 then '06'
when SUB_PRIME_FLAG = 0 and Bureau_Score_Flag <> 1 then '07'
when (SUB_PRIME_FLAG = 0 and Bureau_Score_Clean < 700) or (SUB_PRIME_FLAG = 1  and Bureau_Score_Clean < 600) then '08'
when (SUB_PRIME_FLAG = 0 and MOB_Rounded < 3) or (SUB_PRIME_FLAG = 1 and MOB_Rounded < 6) then '09'
when CLI_In_Last_3_Months <> 0 then '10'
when Bureau_Segment_Flag <> 0 then '11'
when CREDIT_LIMIT_YESTERDAY is null or CREDIT_LIMIT_YESTERDAY <= 100 or (SUB_PRIME_FLAG = 0 and CREDIT_LIMIT_YESTERDAY > 9000) or (SUB_PRIME_FLAG = 1 and CREDIT_LIMIT_YESTERDAY > 3000) then '12'
-- when Credit_Limit is null or Credit_Limit <= 100 or (SUB_PRIME_FLAG = 0 and Credit_Limit > 9000) or (SUB_PRIME_FLAG = 1 and Credit_Limit > 3000) then '12'
when AM36_Flag <> 1 then '13'
when CLD_Block <> 1 then '14'
when credit_account_type <> 'UNSECURED' then '15'
when CLR_Flag <> 0 then '16'
when GO26 <> 'H' then '17'
when Current_Account_Standing <> 'Account_OPEN_Not_Frozen' then '18'
when LATEST_PRODUCT_CATEGORY not in ('HBC','NEO') then '19, brand'
when CLI_SEGMENT_INDICATOR in ('International','World / World Elite','MultiCredit') then '20, Inter, w/we, multi'
when CLI_SEGMENT_INDICATOR = 'HBC' then '21, recent HBC'
when PROVINCE_OF_ACCOUNT in ('Quebec') then '22,QC'
else 'other'
end as reject_reason
,count(*)
-- ,*
from
sandbox.yuanlin_chen.rCLI_elig
where 1=1
and CLI_VINTAGE > '2024-12'
and approve_status = 'Not Approved'
group by 1
order by 1
-- having reject_reason = 'other'
;


---manual bucketed
select
-- cli_vintage,
ICL_BUCKET,
TU_BUCKET,
case when policy_exclusion_flag <> 0 then '01, Policy exclusion'  --02
when Currently_Delinquent_Flag <> 0 then '02, Delinquency status' --03
when Payment_Bal_Flag <> 1 then '03, Payment Balance' --05
when Inquiry_Flag <> 0 then '04, Inquiry record' --06
when SUB_PRIME_FLAG = 0 and Bureau_Score_Flag <> 1 then '05, Bureau score flag' --07
when CLI_In_Last_3_Months <> 0 then '06, Received CLI in last 3m' --10
when Bureau_Segment_Flag <> 0 then '07, Bureau Segment flag' --11
when CREDIT_LIMIT_YESTERDAY is null or CREDIT_LIMIT_YESTERDAY <= 100 or (SUB_PRIME_FLAG = 0 and CREDIT_LIMIT_YESTERDAY > 9000) or (SUB_PRIME_FLAG = 1 and CREDIT_LIMIT_YESTERDAY > 3000) then '08, ICL restriction' --12
-- when Credit_Limit is null or Credit_Limit <= 100 or (SUB_PRIME_FLAG = 0 and Credit_Limit > 9000) or (SUB_PRIME_FLAG = 1 and Credit_Limit > 3000) then '08, ICL restriction' --12
when CLI_SEGMENT_INDICATOR in ('International','World / World Elite','MultiCredit') then '09, Inter, w/we, multi' --20
else 'other'
end as reject_reason
-- ,case when CREDIT_LIMIT_YESTERDAY <= 100 then 'less or equal to $100'
-- when (SUB_PRIME_FLAG = 0 and CREDIT_LIMIT_YESTERDAY > 9000) then 'transactor/revolver over limit'
-- when (SUB_PRIME_FLAG = 1 and CREDIT_LIMIT_YESTERDAY > 3000) then 'sub prime over limit'
-- when CREDIT_LIMIT_YESTERDAY is null then 'no valid cl'
-- else 'other' end as cl_reason
,count(*)
-- ,credit_limit
-- ,CREDIT_LIMIT_YESTERDAY
-- ,*
from
sandbox.yuanlin_chen.rCLI_elig
where 1=1
and CLI_VINTAGE > '2024-12'
and approve_status = 'Not Approved'
group by 1,2,3
-- having reject_reason = '08, ICL restriction' -- and cl_reason = 'no valid credit_limit'
order by 1,2,3
;

