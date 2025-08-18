---Back test HRAM

create or replace temporary table sandbox.yuanlin_chen.backHRAM as (
with temp_table as
(select
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
    and CLI_MOB >6
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
    ,earl_originalbrand as BRAND
    ,original_card_type
    ,BFC - COF - GUCO as RAIM
    FROM 
    SANDBOX.MARKWANG.PM_MONITORING 
    where 1=1
    and secure_type = 'UNSECURED'
)PMMO
ON CLI.ACCOUNTID = PMMO.ACCOUNTID_PMMO
where 1=1
and BRAND in ('NEO','SIENNA')
)
select
temp_table.*
,skuld_snr
,'Bucket' || floor(skuld_snr*10) as SKULD_SNR_SCORE_BUCKET
from temp_table
---attach skuld senior score, on each PMMO day
LEFT JOIN 
(
    SELECT 
    ACCOUNTID
    ,prediction as skuld_snr
    ,REFERENCEDATE
    FROM 
    EXTERNAL_INGESTION.BACKFILLS.RISK_SKULD_SNR_V2_1_BACKFILL_2022_10_01_TO_2025_01_01
    WHERE
    REFERENCEDATE <= '2025-01-01'
    UNION ALL
    SELECT
    ACCOUNTID
    ,prediction as skuld_snr
    ,REFERENCEDATE
    FROM
    ADVANCED_ANALYTICS.BATCH.RISK_SKULD_SNR
    WHERE
    REFERENCEDATE > '2025-01-01'
)SNR
ON temp_table.ACCOUNTID = SNR.ACCOUNTID
AND DATE(temp_table.STMT_MTH || '-01') = DATE(SNR.REFERENCEDATE)
where 1=1
and skuld_snr is not NULL
);


-- attached bt_HRAM flag for the first month that meet HRAM criteria
create or replace temporary table sandbox.yuanlin_chen.backHRAM_w_STMT as (
select * from (
select
*
, case when original_card_type = 'WORLD_ELITE' and skuld_snr >=1 then True 
when original_card_type = 'WORLD' and skuld_snr >=0.9 then True 
when original_card_type = 'STANDARD' and APP_TU_BAND = '1. SUBPRIME' and skuld_snr >=0.7 then True 
else False end as bt_HRAM_FLAG
, min(case when bt_HRAM_FLAG = True then STMT_MTH end) over (partition by accountid,offer_date) as HRAM_MTH
, DATEDIFF(month, TO_DATE(HRAM_MTH || '-01', 'YYYY-MM-DD'), TO_DATE(STMT_MTH || '-01', 'YYYY-MM-DD')) as HRAM_STMT
from 
sandbox.yuanlin_chen.backHRAM
where 1=1
order by accountid, offer_date,stmt_mth
)
where HRAM_STMT > 0
)
;
