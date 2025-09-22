---2025 09 10, HBC apr CLI follow up
create or replace temporary table sandbox.yuanlin_chen.hbc_vintage as (
select
CLI.*
,PMMO.*
,DATEDIFF(month, OFFER_DATE, TO_DATE(STMT_MTH || '-01', 'YYYY-MM-DD')) AS CLI_STMT
,left(OFFER_DATE,7) as offer_vintage
FROM
(
    SELECT
    accountid
    ,offer_date
    ,accepted_flag
    from
    SANDBOX.MARKWANG.VW_CLI_
    where 1=1
    and subproductname = 'UNSECURED'
    and accepted_flag = 1
    -- and CLI_MOB >6
) CLI
---attach performance data
LEFT JOIN 
(
    SELECT
    ACCOUNTID as ACCOUNTID_PMMO
    ,VINTAGE
    ,STMT_NUM
    ,STMT_MTH
    ,CLI_ACCEPTED_FLAG
    ,APP_TU_BAND
    ,DPD_BUCKET
    ,earl_originalbrand as BRAND
    ,original_card_type
    ,BFC - COF - GUCO as RAIM
    ,BFC
    ,COF
    ,GUCO
    ,ADB
    FROM 
    SANDBOX.MARKWANG.PM_MONITORING 
    where 1=1
    and secure_type = 'UNSECURED'
)PMMO
ON CLI.ACCOUNTID = PMMO.ACCOUNTID_PMMO
where 1=1
-- and STMT_MTH > '2024-12'
)
;

---------
-----GUCO deep dive
select
STMT_MTH
,brand
,sum(GUCO)
from 
sandbox.yuanlin_chen.hbc_vintage
where 1 = 1
group by 1,2
order by 1,2
;

select
offer_vintage
,sum(case when STMT_MTH = '2025-06' then GUCO else 0 end) as GUCO_SUM_202506
,sum(case when STMT_MTH = '2025-07' then GUCO else 0 end) as GUCO_SUM_202507
,sum(case when STMT_MTH = '2025-08' then GUCO else 0 end) as GUCO_SUM_202508
from
sandbox.yuanlin_chen.hbc_vintage
where 1 = 1
and brand = 'HBC'
group by 1
order by 1
;

select
*
from
sandbox.yuanlin_chen.hbc_vintage
where 1 = 1
and brand = 'HBC'
and offer_vintage = '2025-04'
and STMT_MTH = '2025-08'
and GUCO > 0
order by offer_date
;

---------
----- B2 balance deep dive

select
STMT_MTH
,offer_vintage
-- ,sum(ADB)
,sum(case when brand = 'HBC' and (DPD_BUCKET  like '%B0%' or DPD_BUCKET like '%B1%') then ADB else 0 end) as HBC_B0_B2_Balance
,sum(case when brand = 'HBC' and (DPD_BUCKET  not like '%B0%' and DPD_BUCKET not like '%B1%' and DPD_BUCKET not like '%B7%') then ADB else 0 end) as HBC_B2plus_Balance
-- ,sum(case when brand <> 'HBC' and (DPD_BUCKET  like '%B0%' or DPD_BUCKET like '%B1%') then ADB else 0 end) as Other_B0_B2_Balance
-- ,sum(case when brand <> 'HBC' and (DPD_BUCKET  not like '%B0%' and DPD_BUCKET not like '%B1%' and DPD_BUCKET not like '%B7%') then ADB else 0 end) as Other_B2plus_Balance
from 
sandbox.yuanlin_chen.hbc_vintage
where 1 = 1
and stmt_mth >'2022-12'
and stmt_mth = '2025-08'
-- and offer_vintage = '2025-04'
and cli_stmt >= 0
group by 1,2
order by 1,2
;

select
distinct(DPD_BUCKET)
from 
sandbox.yuanlin_chen.hbc_vintage
;
