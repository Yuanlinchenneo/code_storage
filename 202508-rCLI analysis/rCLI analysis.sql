
--join request table and offer table
create or replace temporary table sandbox.yuanlin_chen.rCLI as (
select
request_tb._id
,request_tb.userid
,request_tb.CREDITLIMITOFFERID
,request_tb.createdat
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
from 
REFINED.CREDITLIMITSERVICE.CREDITLIMITCHANGEREQUESTS request_tb
left join
REFINED.CREDITLIMITSERVICE.CREDITLIMITOFFERS offer_tb
on request_tb.CREDITLIMITOFFERID = offer_tb._id
where 1=1
order by createdat desc
)
;


------
--- waterfall distribution
select
accept_status,
count(*)
from
sandbox.yuanlin_chen.rCLI
group by 1
order by 1
;


--- expired deepdive
select
left(createdat,7) as CLI_vintage
,count(*)
from
sandbox.yuanlin_chen.rCLI
where 1=1
and accept_status = 'Expired'
group by 1
order by 1
;
