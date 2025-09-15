/*----------------------------------------------------------------------------------------------------------------------------
DETERMINE ELIGIBILITY
----------------------------------------------------------------------------------------------------------------------------*/
create or replace table SANDBOX.MARKWANG.Final_NEO_Tims_Eligible_EMOB_CLI_Campaign_Sep05 as (
    with

    --EXCLUSION: Incremental Fraud Checks (Frozen / Suspended etc.)
    transactions_test_group_fraud_reasons as (
        select 
            accountid,
            sum(case when category = 'CASH_ADVANCE' then 1 else null end) as cash_advance_remove_lm,
            sum(case when declineReason in (
            'ACCOUNT_SUSPENDED',
            'CARD_FROZEN',
            'FRAUD_SUSPECTED',
            'CARD_BLOCKED') then 1 else null end) as fraud_remove
        
        from analytics.earl.earl_transaction
        
        where declineReason is not null and authorizationAt_mt <= current_date() and accountStatus= 'OPEN' and cardtype = 'STANDARD'
        
        group by all
    )

    --EXCLUSION: No Cash Advance Taken
    ,transactions_test_group_for_cash_advance as (
        select
            accountid,
            sum(case when category = 'CASH_ADVANCE' then 1 else 0 end) as cash_advance_remove_apk,
            sum(case when category = 'PURCHASE' and status = 'CONFIRMED' then 1 else 0 end) as made_a_confirmed_purchase_flag

        from analytics.earl.earl_transaction

        where authorizationAt_mt <= current_date() and accountStatus= 'OPEN' and cardtype = 'STANDARD'

        group by 1
    )

    --EXCLUSION: Never been Delinquent
    ,delinquency_test_group as (
        select
            accountid, 
            case when dpd > 0 then 1 else 0 end as delinquency_remove,
            case when dpd > 5 then 1 else 0 end as delinquency_remove_gt5
        
        from(
            select accountid, max(daysPastDue) as dpd
            
            from analytics.earl.earl_account
            
            where referenceDate <= current_date()
            
            group by 1
        )
    )

    --EXCLUSION: SKULD Score Less than 10%
    ,hram_test as (
        select
            accountId
            ,case when pred_v2 >= 0.1 then 1 else 0 end as hram_remove
            ,case when pred_v2 >= 0.15 then 1 else 0 end as hram_remove_15
            ,case when pred_v2 >= 0.2 then 1 else 0 end as hram_remove_20

        from(
            select accountId, max(prediction) as pred_v2
            
            from ADVANCED_ANALYTICS.BATCH.RISK_SKULD_JNR
            
            where
                modelVersion = '2.1'
                and to_date(referenceDate) > (current_date() - 30)
            
            group by all
        )
    )


    ,test_group as (
        select
            distinct f._id as accountid,
            a._id applicationid,
            creditCardReport['AM41'] as AM41_At_Onboarding,
            creditCardReport['strategyName'] as strategyName
        
        from REFINED.CREDITONBOARDINGSERVICE.CREDITAPPLICATIONS as a
        
        left join REFINED.IDENTITYSERVICE.USERREPORTSMETADATASNAPSHOTS as b
        on a.userReportsMetadataSnapshotId = b._id
        
        left join (SELECT _id AS userId, type AS customerType FROM REFINED.USERSERVICE.USERS) e ON a.userId = e.userId
        
        left join REFINED.ADJUDICATIONSERVICE.ADJUDICATIONREPORTS c on c._id = b.adjudicationResult['reportid']
        
        left join REFINED.CREDITACCOUNTSERVICE.CREDITACCOUNTS f on a._id = f.creditApplicationId
        
        where customerType != 'TEST' 
            and a.brand in ('NEO','HBC','SIENNA') 
            and a.type = 'STANDARD'
            and a.status = 'COMPLETED' 
            and a.decision = 'APPROVED'
            and to_date(a.createdAt) >= '2025-02-01'
    )


    ,test_group_master_data as (
        select
            distinct t1.accountid,
            t1.customerId,
            t1.userId,
            t1.productTypeName,
            test_group.applicationId,
            t1.dayOnBook,
            t1.monthOnBook,
            test_group.strategyName,
            test_group.AM41_At_Onboarding,
            case when transactions_test_group_for_cash_advance.made_a_confirmed_purchase_flag > 0 then 1 else 0 end as confirmed_purchase_flag,
            case when transactions_test_group_for_cash_advance.cash_advance_remove_apk >= 1 then 1 else 0 end as cash_advance_remove_flag, -- cash advance flag
            case when delinquency_test_group.delinquency_remove >= 1 then 1 else 0 end as delinquency_pass_flag, -- delinquency flag
            case when delinquency_test_group.delinquency_remove_gt5 >= 1 then 1 else 0 end as delinquency_pass_upto_5days_flag, -- delinquency flag
            case when t1.currentProvince = 'QC' then 1 else 0 end as QC_Flag, -- QC Flag
            case when t1.latestCreditScoreBand in ('c. Near-Prime (640 to 719)', 'd. Prime (720 to 759)', 'e. Prime+ (760 to 799)', 'f. Super Prime (800+)') then 1 else 0 end as Latest_Score_Not_Sub_Prime, -- Remove current subprimes

            case when t1.originalCreditScoreBand in ('c. Near-Prime (640 to 719)', 'c. Near-Prime (640 to 719)', 'd. Prime (720 to 759)', 'e. Prime+ (760 to 799)', 'f. Super Prime (800+)') then 1 else 0 end as Original_Score_Not_Sub_Prime, -- Remove original subprimes

            case when transactions_test_group_fraud_reasons.fraud_remove is null then 1 else 0 end as Fraud_Checks_Passed, -- Fraud Declines at Transaction
            case when test_group.strategyName in ('thin-prime-neo-brand-tuning-baseline', 'neo-card-thin-with-toh') then 1 else 0 end as Is_Thin_File,
            case when hram_test.hram_remove = 1 then 1 else 0 end as hram_remove_flag,
            case when hram_test.hram_remove_15 = 1 then 1 else 0 end as hram_remove_15_flag,
            case when hram_test.hram_remove_20 = 1 then 1 else 0 end as hram_remove_20_flag,


            case when (t1.latestCreditScore - t1.originalCreditScore) = 0 then '1. TU Score Same'
            when (t1.latestCreditScore - t1.originalCreditScore) > 0   then '2. TU Score Better Now'
            when ((t1.latestCreditScore - t1.originalCreditScore) < 0   and (t1.latestCreditScore - t1.originalCreditScore) >= -20) then '3. Dropped Upto 20 Points'
            when (t1.latestCreditScore - t1.originalCreditScore) < -20 and (t1.latestCreditScore - t1.originalCreditScore) >= -50 then '4. Dropped Between 20 to 50 Points'
            when (t1.latestCreditScore - t1.originalCreditScore) < -50 then '5. Dropped More Than 50 Points'
            else                                                            '6. Unknown'
            end as Credit_Score_Movement,

            t1.latestCreditScore - t1.originalCreditScore as TU_Score_Change_Value,

            case when (t1.originalCreditScoreBand in ('c. Near-Prime (640 to 719)', 'c. Near-Prime (640 to 719)')) and t1.creditLimit = 2000 then 1
            when (t1.originalCreditScoreBand in ('e. Prime+ (760 to 799)', 'f. Super Prime (800+)')) and t1.creditLimit = 1000 then 2
            else 0 end as Target_Segment_CL_Accounts,


            t1.latestCreditScore,
            t1.latestCreditScoreBand,
            t1.originalCreditScoreBand,
            t1.originalCreditScore,
            t1.creditLimit,
            case when t1.monthOnBook >= 2 then 1 else 0 end as GT2_MOB_Flag, 

            case 
            when originalCreditScoreBand = 'b. Sub Prime (300 to 639)' then '1. Subprime' 
            when originalCreditScoreBand = 'c. Near-Prime (640 to 719)' then '2. Near Prime'
            when originalCreditScoreBand = 'd. Prime (720 to 759)' then '3. Prime'
            when originalCreditScoreBand in ('e. Prime+ (760 to 799)', 'f. Super Prime (800+)') then '4. >= Prime Plus'
            else null end as original_credit_score_band
        
        from analytics.earl.earl_account as t1
        
        right join test_group on test_group.accountid = t1.accountid

        left join hram_test on hram_test.accountid = t1.accountid
        
        left join delinquency_test_group on delinquency_test_group.accountid = t1.accountid
        
        left join transactions_test_group_for_cash_advance on transactions_test_group_for_cash_advance.accountid = t1.accountid
        
        left join transactions_test_group_fraud_reasons on transactions_test_group_fraud_reasons.accountid = t1.accountid

        where 
            referenceDate = current_date()
            and accountStatus= 'OPEN' 
            and cardtype = 'STANDARD' 
            and productTypeName in ('NEO UNSECURED STANDARD CREDIT', 'HBC UNSECURED STANDARD CREDIT', 'SIENNA UNSECURED STANDARD CREDIT') 
            and t1.monthOnBook > 0 

            and test_group.accountid is not null
        -- group by 3
    )

    ,bureau_data as (
        select
            user_id as userId,
            credit_report_date,
            AM41 as Latest_AM41
        
        from
            EXTERNAL_INGESTION.TRANSUNION.TRANSUNION_CREDITREPORT_CREDITVISION

        qualify row_number() over(partition by user_id order by user_id, credit_report_date desc) = 1
    )

    --EXCLUSION: No increase in bureau delinquency count (AM41)
    ,Master_With_Bureau as (
        select t1.*, Latest_AM41
        
        from test_group_master_data as t1
        
        left join bureau_data
        on bureau_data.userId = t1.userId
    )

    --Grab the latest stmt
    ,statement1 as (
        select
            accountid,
            paymentDueDate as Payment_Due_Date_Full,
            to_date(closingDate) as Statement_Closing_Date, 
            to_date(paymentDueDate) as Payment_Due_Date, 
            concat(year(to_date(closingDate)), '-', lpad(month(to_date(closingDate)), 2, '0')) as Statement_Closing_Month,
            coalesce(['summary']['account']['purchasesCents'], 0) * 0.01 as Purchase,
            coalesce(['summary']['account']['previousBalanceCents'], 0) * 0.01 as Previous_Balance,
            coalesce(['summary']['account']['paymentsCents'], 0) * 0.01 as Current_Payment,
            coalesce(['summary']['account']['creditLimitCents'], 0) * 0.01 as Credit_Limit_In_Latest_Statement,
            coalesce(['summary']['account']['newBalanceCents'], 0) * 0.01 as New_Statement_Balance,
            substr(closingDate, 1, 4) as Statement_Close_Year,
            substr(closingDate, 6, 2) as Statement_Close_Month,
            
        from REFINED.STATEMENTSERVICE.CREDITCARDSTATEMENTS

        qualify row_number() over(partition by accountId order by closingDate desc) = 1
    )

    ,Master_With_Statement_Bureau_Del as (
        select
            t1.*,
            Payment_Due_Date_Full,
            Statement_Closing_Date, 
            Payment_Due_Date, 
            Statement_Closing_Month,
            Purchase,
            Previous_Balance,
            Current_Payment,
            Credit_Limit_In_Latest_Statement,
            New_Statement_Balance,
            Statement_Close_Year,
            Statement_Close_Month,
            case when Purchase = 0 or Purchase is null then 0 else Purchase end as Purchasev2,
            case when Previous_Balance = 0 or Previous_Balance is null then 0 else Previous_Balance end as Previous_Balancev2,
            case when Current_Payment = 0 or Current_Payment is null then 0 else Current_Payment end as Current_Paymentv2,
            case when Credit_Limit_In_Latest_Statement = 0 or Credit_Limit_In_Latest_Statement is null then 0 else Credit_Limit_In_Latest_Statement end as Credit_Limit_In_Latest_Statementv2,
            case when New_Statement_Balance = 0 or New_Statement_Balance is null then 0 else New_Statement_Balance end as New_Statement_Balancev2

        from
            Master_With_Bureau t1
        left join
            statement1 on statement1.accountid = t1.accountid 
    )

    ,all_payments as (
        select
            accountid,
            actualAmountDollars,
            to_date(completedAt_mt) as Payment_Made_Date,
            case when customerMadeTrustedPayment = 'false' then 1 else 0 end as Trusted_Payment,
            case when actualAmountDollars >= 2500 then 1 else 0 end as Payment_GT_2500  
        
        from analytics.earl.earl_transaction
        
        where category = 'PAYMENT' and accountStatus= 'OPEN' and cardtype = 'STANDARD'
    )

    ,trusted_payments_data as (
        select
            distinct accountId,
            case when (SUM(Trusted_Payment) > 0) and (sum(Payment_GT_2500) > 0) then 1 else 0 end as Has_A_Non_Trusted_Payment
        
        FROM all_payments
        
        GROUP BY accountId
    )

    ,relevant_payments as (
        select
            distinct T1.accountId,
            SUM(all_payments.actualAmountDollars) AS Payment_Amount
        
        FROM Master_With_Statement_Bureau_Del as T1
        
        LEFT JOIN all_payments
        ON all_payments.accountId = t1.accountId
        
        GROUP BY T1.accountId
    )

    --EXCLUSION: Payment from Trusted Source
    --EXCLUSION: Payment to Balance Ratio of < 10%
    ,Master_With_Pay_Bal_Ratio as (
        select
            T1.*,
            trusted_payments_data.Has_A_Non_Trusted_Payment,
            relevant_payments.Payment_Amount,
            case when AM41_At_Onboarding < Latest_AM41 then 1 else 0 end as AM41_Increased,
            row_number() over(partition by T1.accountId order by T1.accountId) as Counter

        from Master_With_Statement_Bureau_Del as T1
        
        LEFT JOIN relevant_payments
        ON T1.accountId = relevant_payments.accountId
        
        LEFT JOIN trusted_payments_data
        ON T1.accountId = trusted_payments_data.accountId
    )


    ,Policy_Exclusion_Check_Data as (
        select
            user_id as userId,
            cast(PR44 as int) as PR44_Flag,
            cast(PR45 as int) as PR45_Flag,
            cast(PR124 as int) as PR124_Flag,
            cast(PR116 as int) as PR116_Flag,
            cast(PR15 as int) as PR15_Flag,
            cast(PR50 as int) as PR50_Flag,
            cast(PR74 as int) as PR74_Flag,
            cast(PR75 as int) as PR75_Flag,
            cast(PR120 as int) as PR120_Flag,
            cast(PR97 as int) as PR97_Flag,
            cast(GO81 as int) as GO81_Flag,
            cast(GO91 as char(1)) as GO91_Flag,
            case when (GO91_Flag = 'Y' or PR44_Flag >= 1 or PR45_Flag>= 1 or PR124_Flag>= 1 or PR116_Flag>= 1 or PR15_Flag>= 1 or PR50_Flag>= 1 or PR74_Flag>= 1 or PR75_Flag>= 1 or PR120_Flag>= 1 or PR97_Flag>= 1 or GO81_Flag>= 1) then 1 else 0 end as Policy_Fail_Flag
                    
        from
            EXTERNAL_INGESTION.TRANSUNION.TRANSUNION_CREDITREPORT_CREDITVISION
        
        where
            GO26 = 'H' and GO26 is not null
        
        qualify row_number() over(partition by user_id order by user_id, credit_report_date desc) = 1
    )

    --EXCLUSION: Passes Adjudication Policy Exclusions
    ,Master_With_Final_All as (
        select
            T1.*,
            PR44_Flag,
            PR45_Flag,
            PR124_Flag,
            PR116_Flag,
            PR15_Flag,
            PR50_Flag,
            PR74_Flag,
            PR75_Flag,
            PR120_Flag,
            PR97_Flag,
            GO81_Flag,
            GO91_Flag,
            Policy_Fail_Flag

        from Master_With_Pay_Bal_Ratio as T1
        
        LEFT JOIN
            Policy_Exclusion_Check_Data
        ON
            T1.userId = Policy_Exclusion_Check_Data.userId
    )

    /*----------------------------------------------------------------------------------------------------------------------------
    WATERFALLS & SOME MORE EXCLUSIONS
    ----------------------------------------------------------------------------------------------------------------------------*/

    ,Master_Waterfall as (
        select *,
        case
            when hram_remove_flag = 1 then '1. Fraud SKULD Exclusion'
            when Fraud_Checks_Passed = 0 then '2. Fraud Checks Failed' 
            when Has_A_Non_Trusted_Payment = 1 then '3. Fraud Non Trusted Payment'
            when Policy_Fail_Flag = 1 then '4. Credit Policy Exclusion'
            when cash_advance_remove_flag = 1 then '5. Credit Cash Advance'
            when confirmed_purchase_flag = 0 then '6. Credit Confirmed Purchase'
            when delinquency_pass_flag = 1 then '7. Credit Delinquency'
            when (Original_Score_Not_Sub_Prime = 0) or (Latest_Score_Not_Sub_Prime = 0) then '7. Credit Score Sub Prime'
            when AM41_Increased = 1 then '8. Credit AM41 Increase'
            when Credit_Score_Movement in ('5. Dropped More Than 50 Points') then '9. Credit Score Dropped More Than 50 Points'
            when Target_Segment_CL_Accounts = 0 then '15. Not Target Segment CL Accounts'
            else '16. Criteria Pass'
        end as Waterfall

        from Master_With_Final_All
    )

    ,CLI_Offer_Details as (
        select
            c.creditAccountId as accountId, 
            to_date(c.initiatedAt) as initiatedDate, 
            trunc(c.initiatedAt, 'MM') as initiatedBoM, 
            to_date(c.acceptedAt) as acceptedDate, 
            (c.creditLimitCentsAtOfferTime/100) as creditLimitAtOfferTime,
            case when accountid is null then 0 else 1 end as cli_flag
        
        from REFINED.CREDITLIMITSERVICE.CREDITLIMITOFFERS c

        qualify row_number() over(partition by c.creditAccountId order by c.creditAccountId) = 1
    )

    ,Master_Waterfall_With_CLI_Flag as (
        select 
            Master_Waterfall.*,
            cli_flag
        from Master_Waterfall
        
        LEFT JOIN
            CLI_Offer_Details
        ON
            Master_Waterfall.accountId = CLI_Offer_Details.accountId
    )

    ,Behavior_Score as (
        select 
            accountId, 
            modelscore as Latest_Behavior_Score,
        
        from ADVANCED_ANALYTICS.BATCH.BEHAVIOUR_SCORES

        qualify row_number() over(partition by accountId order by accountId, referenceDate desc) = 1
    )

    ,Master_Waterfall_With_Behavior as (
        select
            Master_Waterfall_With_CLI_Flag.*,
            Latest_Behavior_Score
        
        from
            Master_Waterfall_With_CLI_Flag
        
        LEFT JOIN
            Behavior_Score
        ON
            Behavior_Score.accountId = Master_Waterfall_With_CLI_Flag.accountId
    )

    --Checking if latest strategy will approve
    ,rawdata as (
        select 
            a.customerid as userid, a.brand, a.cardtype, a.iscreditactive, b.cvsc100, b.segment, b.go14, b.at01,
            b.am41, b.am42, b.am43, b.am84, b.go11, b.re34, b.pr51, b.bc147, b.go06, b.go149, b.go141, b.pr11, b.am02,
            b.go81, b.go91, b.pr15, b.pr44, b.pr45, b.pr50, b.pr74, b.pr75, b.pr97, b.pr116, b.pr120, b.pr124,
            c.dti, d.bkoscorev1, d.thickscorev1, d.thinscorev1, d.subprimescorev1
        
        from analytics.earl.earl_account a 
        
        left join sandbox.linchen.latestcrcs b on a.userid = b.userid
        
        left join sandbox.linchen.application c on a.userid = c.userid
        
        left join sandbox.linchen.oldscores_latestdata d on a.userid = d.userid
        
        where
            a.referencedate = (current_date()-1)
            and a.subproductname = 'UNSECURED'
            and a.cardtype is not null
            and a.accountstatus = 'OPEN'
            and b.cvsc100 between 300 and 900
            -- and a.brand = 'NEO'
    )

    ,finaldata as (
        select
        *,
        case 
            when 
                go81>0 or go91>0 or pr15>0 or pr44>0 or pr45>0 or pr50>0 or 
                pr74>0 or pr75>0 or pr97>0 or pr116>0 or pr120>0 or pr124>0
            then 1
            else 0
        end as rpa_reject,
        case
            when segment = 'THICK' and (
                am41>1 or am42>0 or am43>0 or am84>0 or go11<=50 or re34>=100 or dti>200 or pr51>0
            ) then 1
            when segment = 'THIN' and (
                am41>0 or am42>0 or am43>0 or at01<=0 or at01>5 or am84>0 or bc147>=90 or 
                go06>4 or go11<=95 or go149>1 or go14<=2 or re34>=90 or pr51>0 or dti>200
            ) then 1
            when segment = 'SUBPRIME' and (
                am41>1 or am42>0 or am43>0 or go141>4 or pr11>500 or am02<=0 or 
                am84>0 or pr51>0 or re34>95 or go14<3 or dti>100
            ) then 1
            else 0
        end as toh_reject,
        case
            when segment = 'THICK' and bkoScoreV1 < 670 then 1
            when segment = 'THIN' and bkoScoreV1 < 715 then 1
            when segment = 'SUBPRIME' and bkoScoreV1 < 670 then 1
            else 0
        end as bko_reject,
        case 
            when segment = 'THICK' and cvsc100>=640 and (    
                thickscorev1>=521 or 
                (thickscorev1>=489 and cvsc100>=669) or 
                (thickscorev1>=475 and cvsc100>=690)
            ) then 0
            when segment = 'THIN' and cvsc100>=640 and (
                (thinscorev1>=641 and cvsc100>=680) or 
                (thinscorev1>=656 and cvsc100>=669)
            ) then 0
            when segment = 'SUBPRIME' and cvsc100 between 600 and 640 and (
                subprimescorev1>=575
            ) then 0
            else 1
        end as tree_reject_lr,
        case
                when rpa_reject=0 
                and toh_reject=0
                and bko_reject=0
                and tree_reject_lr=0
                then 1
            else 0
            end as approve_lr
        
        from rawdata
        
        qualify row_number() over(partition by userid order by userid) = 1
    )


    ,Master_22 as (
    select
        Master_Waterfall_With_Behavior.*,
        case when approve_lr = 1 then 1 else 0 end as Current_Strategy_Will_Approve

    from Master_Waterfall_With_Behavior

    left join
        finaldata
    on
        finaldata.userid = Master_Waterfall_With_Behavior.userid
    )

    ,Frozen_User as (
        select
            accountId, 
            creditFacility as Facility,
            case when userFrozenReason is not null then 1 else 0 end as User_Is_Frozen
        
        from
            analytics.earl.earl_account
        where referenceDate = current_date()
    )

    ,Master as (
        select
            Master_22.*, 
            Frozen_User.User_Is_Frozen,
            Frozen_User.Facility,
            case when cli_flag = 1 then 1 else 0 end as was_given_cli_already
        
        from Master_22
        
        left join
            Frozen_User
        ON
            Master_22.accountId = Frozen_User.accountId
    )

    /*----------------------------------------------------------------------------------------------------------------------------
    LINE ASSIGNMENTS
    ----------------------------------------------------------------------------------------------------------------------------*/

    ,Master_Waterfall_Final_NEO_HBC_Tims as (
        select *

        ,case
            when Current_Strategy_Will_Approve = 0 then '0. Current Strategy'
            when hram_remove_flag = 1 then '1. Fraud SKULD Exclusion'
            when Fraud_Checks_Passed = 0 then '2. Fraud Checks Failed' 
            when Has_A_Non_Trusted_Payment = 1 then '3. Fraud Non Trusted Payment'
            when Policy_Fail_Flag = 1 then '5. Credit Policy Exclusion'
            when cash_advance_remove_flag = 1 then '6. Credit Cash Advance'
            when confirmed_purchase_flag = 0 then '7. Credit Confirmed Purchase'
            when delinquency_pass_flag = 1 then '8. Credit Delinquency'
            when (Original_Score_Not_Sub_Prime = 0) or (Latest_Score_Not_Sub_Prime = 0) then '9. Credit Score Sub Prime'
            when AM41_Increased = 1 then '10. Credit AM41 Increase'
            when Credit_Score_Movement in ('5. Dropped More Than 50 Points') then '11. Credit Score Dropped More Than 50 Points'
            when Latest_Behavior_Score >= 0 and Latest_Behavior_Score < 600 then '12. High Risk Behavior Score'
            when Payment_Amount is null then '13. No Payment Amount'
            when User_Is_Frozen = 1 then '14. User Frozen'
            else '15. Criteria Pass'
        end as Waterfall_10_WO_Bunsen
            
        ,case
            when producttypename = 'SIENNA UNSECURED STANDARD CREDIT' and ((originalCreditScoreBand = 'c. Near-Prime (640 to 719)' and creditLimit <= 1200)
            or (originalCreditScoreBand = 'd. Prime (720 to 759)' and creditLimit <= 3000)
            or (originalCreditScoreBand = 'e. Prime+ (760 to 799)' and creditLimit <= 3000)
            or (originalCreditScoreBand = 'f. Super Prime (800+)' and creditLimit <= 4000)) then 1 
            when producttypename = 'NEO UNSECURED STANDARD CREDIT' then 2 else 0
        end as Low_ICL_Sienna_Check

        ,case
            when producttypename = 'SIENNA UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'c. Near-Prime (640 to 719)' and creditLimit < 2000) then 500
            when producttypename = 'SIENNA UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'c. Near-Prime (640 to 719)' and creditLimit >= 2000) then 1000

            when producttypename = 'SIENNA UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'd. Prime (720 to 759)' and creditLimit < 1000) then 1200
            when (originalCreditScoreBand = 'd. Prime (720 to 759)' and creditLimit >= 1000) then 2000

            when producttypename = 'SIENNA UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'e. Prime+ (760 to 799)') then 2000 
            when producttypename = 'SIENNA UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'f. Super Prime (800+)') then 2000

            when producttypename = 'NEO UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'c. Near-Prime (640 to 719)' and creditLimit < 701) then 1000
            when producttypename = 'NEO UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'c. Near-Prime (640 to 719)' and creditLimit >= 701) then 1200

            when producttypename = 'NEO UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'd. Prime (720 to 759)' and creditLimit < 1000) then 1200
            when producttypename = 'NEO UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'd. Prime (720 to 759)' and creditLimit >= 1000 and creditLimit < 1800) then 1500
            when producttypename = 'NEO UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'd. Prime (720 to 759)' and creditLimit >= 1800) then 2000

            when producttypename = 'NEO UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'e. Prime+ (760 to 799)') then 2000 
            when producttypename = 'NEO UNSECURED STANDARD CREDIT' and (originalCreditScoreBand = 'f. Super Prime (800+)') then 2500
            else 0
        end as CLI_Amount

        from
            Master

        where
            DAYONBOOK > 90 and DAYONBOOK < 179
            and was_given_cli_already =0
            and Waterfall_10_WO_Bunsen = '15. Criteria Pass'
    )


    /*----------------------------------------------------------------------------------------------------------------------------
    SEPARATE TEST & CONTROL GROUPS
    ----------------------------------------------------------------------------------------------------------------------------*/
    ,Final_NEO_Tims_Eligible_EMOB_CLI_Campaign_Sep05 as ( 
        select *,
        case when producttypename in ('SIENNA UNSECURED STANDARD CREDIT') then 'Tims'
            when producttypename in ('NEO UNSECURED STANDARD CREDIT') then 'Neo' end as Brand_Product
        from Master_Waterfall_Final_NEO_HBC_Tims
        where producttypename in ('SIENNA UNSECURED STANDARD CREDIT', 'NEO UNSECURED STANDARD CREDIT') and LOW_ICL_SIENNA_CHECK in (1,2)
    )


    select accountid, monthOnBook, originalCreditScoreBand, creditLimit, CLI_Amount, Brand_Product
    from Final_NEO_Tims_Eligible_EMOB_CLI_Campaign_Sep05
    where dayOnBook > 90 and dayOnBook < 179

)
