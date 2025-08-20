

select
*
from
SANDBOX.MARKWANG.PM_MONITORING 
-- order by accountid, stmt_mth
limit 5
;


select
distinct(original_card_type)
from
SANDBOX.MARKWANG.PM_MONITORING 
-- order by accountid, stmt_mth
limit 5
;

select
*
from SANDBOX.MARKWANG.VW_CLI_ 
where 1=1
and accountid = '62323ddf6c13482c95d9a203'
;

select
left(id_date,7) as HRAM_VINTAGE     
,*
from SANDBOX.MARKWANG.HRAM_ACCTS HRAM
where 1=1
and HRAM.type = 'LMOB'
ORDER BY 1
;

---to check
select
*
from
(
select 
accountid
,id_date
from SANDBOX.MARKWANG.HRAM_ACCTS
where accountid = '626461bfa8b93ecafb9d749d'
-- where accountid in ('639f60e0db3b1cb53c0272d4','6522e4186118410cabc9b21e','624dd0981f35ca60f8578739','61a17e87733ba78018444398','63293966cfd43378f634a6f4','631f98903220d18248f7f1b8','6349a2e5f48ea999b1569471')
order by id_date asc
)HRAM
LEFT JOIN 
(
SELECT
ACCOUNTID as ACCOUNTID_PMMO
,VINTAGE
,STMT_NUM
,STMT_MTH
,CLI_ACCEPTED_FLAG
,APP_TU_BAND
,APP_SEG
,earl_originalbrand as BRAND
,ICL
,prev_cl
,CL
,AM_TU_BAND
,BFC, COF, GUCO
,BFC - COF - GUCO as RAIM
FROM 
SANDBOX.MARKWANG.PM_MONITORING 
where 1=1
and secure_type = 'UNSECURED'
)PMMO
ON HRAM.ACCOUNTID = PMMO.ACCOUNTID_PMMO
;


select 
*
from
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
where SNR.accountid = '613193cf7378240d8a8b8207'
order by referencedate asc
;

select 
max(referencedate)
from EXTERNAL_INGESTION.BACKFILLS.RISK_SKULD_SNR_V2_1_BACKFILL_2022_10_01_TO_2025_01_01
limit 5;

select 
*
from ADVANCED_ANALYTICS.BATCH.RISK_SKULD_SNR
where accountid = '5e6965bc058598001e1c52da'
order by referencedate asc
limit 5;


--- select BOTH responder and non responder, unsecured accounts
select
CLI.*
-- ,SNR.skuld_snr
,PMMO.APP_TU_BAND
,APP_SEG
,BFC,
COF
,GUCO
FROM
(SELECT
accountid
,offer_date
,accepted_flag
,CURRENTCREDITSCOREBAND
from
SANDBOX.MARKWANG.VW_CLI_
where 1=1
-- and accepted_flag = 1
and offer_date <'2025-01-01'
and subproductname = 'UNSECURED'
) CLI
-- ---attach skuld senior score, on the day of CLI offer
-- LEFT JOIN 
-- (SELECT 
-- ACCOUNTID
-- ,prediction as skuld_snr
-- ,REFERENCEDATE
-- FROM 
-- EXTERNAL_INGESTION.BACKFILLS.RISK_SKULD_SNR_V2_1_BACKFILL_2022_10_01_TO_2025_01_01
-- )SNR
-- ON CLI.ACCOUNTID = SNR.ACCOUNTID
-- AND CLI.OFFER_DATE = DATE(SNR.REFERENCEDATE)
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
,APP_SEG
,earl_originalbrand as BRAND
,ICL
,prev_cl
,CL
,AM_TU_BAND
,BFC, BFC_CUML, COF, COF_CUML, GUCO, GUCO_CUML
FROM 
SANDBOX.MARKWANG.PM_MONITORING 
where 1=1
and secure_type = 'UNSECURED'
)PMMO
ON CLI.ACCOUNTID = PMMO.ACCOUNTID_PMMO
where CLI.accountid = '628a9215a95ff933cdecdab3'
order by offer_date, stmt_mth desc
;


---- analyze newer tranch behavior

create or replace temporary table sandbox.yuanlin_chen.newtranch as (
select
CLI.*
,SNR.skuld_snr
,PMMO.*
,'Bucket' || floor(skuld_snr*10) as SKULD_SNR_SCORE_BUCKET
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
---attach skuld senior score, on the day of CLI offer
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
ON CLI.ACCOUNTID = SNR.ACCOUNTID
AND CLI.OFFER_DATE = DATE(SNR.REFERENCEDATE)
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
    ,APP_SEG
    ,earl_originalbrand as BRAND
    ,AM_TU_BAND
    ,BFC - COF - GUCO as RAIM
    FROM 
    SANDBOX.MARKWANG.PM_MONITORING 
    where 1=1
    and secure_type = 'UNSECURED'
)PMMO
ON CLI.ACCOUNTID = PMMO.ACCOUNTID_PMMO
where 1=1
and skuld_snr is not NULL
and BRAND in ('NEO','SIENNA')
);


---high risk ratio
select 
offer_vintage
,count(case when skuld_snr < 0.7 then accountid else Null end) as low_risk_accts
,count(case when skuld_snr >=0.7 then accountid else Null end) as high_risk_accts
,high_risk_accts/(high_risk_accts + low_risk_accts) as high_risk_ratio
from sandbox.yuanlin_chen.newtranch
where 1=1
and ACCEPTED_FLAG = 1
and CLI_STMT = 1
and BRAND = 'NEO'
group by 1
order by 1
;



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
and CLI_STMT > 0
order by accountid, offer_date,stmt_mth
)
where HRAM_STMT > 0
)
;

-- performance (1yr, trend) after HRAM mark
select
*
from sandbox.yuanlin_chen.BACKHRAM_W_STMT
where 1=1
and accountid = '622a7dad55aada416658a119'
order by accountid, offer_date,stmt_mth
-- limit 20
;


select
*
from sandbox.yuanlin_chen.backHRAM
where 1=1
and accountid = '622a7dad55aada416658a119'
order by accountid, offer_date,stmt_mth
-- limit 20
;
