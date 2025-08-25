-- to find out why march 2024 and later behave differently
create or replace temporary table sandbox.yuanlin_chen.skulddeepdive as (
select
    CLI.*
    ,SNR.skuld_snr AS SKULD_SNR_SCORE
    ,BEH.MODELSCORE AS BEH_SCORE
    ,BKO.BANKRUPTCYSCORE AS BKO_SCORE
    ,PMMO.*
    ,OFFER_userid
    ,CREDITLIMITOFFERSTRATEGY
    ,'Bucket' || floor(skuld_snr*10) as SKULD_SNR_SCORE_BUCKET
    ,DATEDIFF(month, OFFER_DATE, TO_DATE(STMT_MTH || '-01', 'YYYY-MM-DD')) AS CLI_STMT
    , case when BKO_SCORE > 680 then 'Pass'
            else 'Fail'
            end as BKO_cut
    , case when Brand = 'HBC' and BEH_SCORE > 594 then 'Pass'
            when Brand = 'NEO' and BEH_SCORE > 620 then 'Pass'
            when Brand = 'SIENNA' and BEH_SCORE > 620 then 'Pass'
            when Brand = 'CATHAY' and BEH_SCORE > 680 then 'Pass'
            else 'Fail'
            end as BEH_cut
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
    and CLI_MOB > 6
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
    -- ADVANCED_ANALYTICS.BATCH.RISK_SKULD_SNR
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
    ,earl_originalbrand as BRAND
    ,APP_TU_BAND
    ,APP_SEG
    ,BFC - COF - GUCO as RAIM
    FROM 
    SANDBOX.MARKWANG.PM_MONITORING 
    where 1=1
    and secure_type = 'UNSECURED'
)PMMO
ON CLI.ACCOUNTID = PMMO.ACCOUNTID_PMMO
--- attach BEH score
LEFT JOIN
(
SELECT
REFERENCEDATE
,ACCOUNTID AS ACCOUNTID_BEH
,MODELSCORE
FROM 
EXTERNAL_INGESTION.BACKFILLS.CREDIT_RISK_BEHAVIOR_PD_V2_1
) BEH
ON CLI.ACCOUNTID = BEH.ACCOUNTID_BEH
AND TRUNC(BEH.referencedate, 'month') = TRUNC(DATEADD(month, -1, CLI.offer_date), 'month')
--- attach userid
LEFT JOIN
(
    SELECT
    distinct(creditaccountid) as offer_acct_id
    ,userid AS OFFER_userid
    ,CREDITLIMITOFFERSTRATEGY
    FROM
    REFINED.CREDITLIMITSERVICE.CREDITLIMITOFFERS
)OFFER_tb
ON CLI.ACCOUNTID = OFFER_tb.offer_acct_id
--- attach BKO score
LEFT JOIN
(
SELECT
REFERENCEDATE
,userid
,BANKRUPTCYSCORE
FROM 
ANALYTICS.CREDIT_RISK.BANKRUPTCY_SCORES
) BKO
ON OFFER_userid = BKO.userid
AND LEFT(CLI.OFFER_DATE,7) = LEFT(BKO.REFERENCEDATE,7)
WHERE CLI_STMT between 0 and 12
)
;

-----
select 
left(offer_Date,7) as offer_vintage
,case when SKULD_SNR_SCORE >= 0.7 then 'high risk'
else 'low risk' end as high_risk_flag
,avg(BEH_SCORE)
,avg(BKO_score)
from 
sandbox.yuanlin_chen.skulddeepdive
where 1=1
AND SKULD_SNR_SCORE IS NOT NULL
and cli_stmt = 1
and accepted_flag = 1
and high_risk_flag = 'high risk'
group by 1,2
order by 1,2
;

select
*
, case when BKO_SCORE > 680 then 'Pass'
else 'Fail'
end as BKO_cut
, case when Brand = 'HBC' and BEH_SCORE > 594 then 'Pass'
when Brand = 'NEO' and BEH_SCORE > 620 then 'Pass'
when Brand = 'SIENNA' and BEH_SCORE > 620 then 'Pass'
when Brand = 'CATHAY' and BEH_SCORE > 680 then 'Pass'
else 'Fail'
end as BEH_cut
from 
sandbox.yuanlin_chen.skulddeepdive
limit 5
;

select
*
from 
sandbox.yuanlin_chen.skulddeepdive
limit 5
;

select
distinct(CLI_STMT)
from 
sandbox.yuanlin_chen.skulddeepdive
order by 1
;
