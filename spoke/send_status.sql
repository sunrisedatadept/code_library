with 

base as (
  select 
	  send_status as status,
    count(send_status) as num,
    ratio_to_report(num::decimal) over() send_status 

  from 
    sunrise_spoke.message 
  left join 
    sunrise_spoke.campaign_contact cc 
    on message.campaign_contact_id = cc.id
  left join 
    sunrise_spoke.campaign_title_separated camp_adj 
    on cc.campaign_id = camp_adj.id
  where  
    [camp_adj.camp_tag=tag]
  group by 
    send_status

), send_status as(
  select 
    status,
    num,
    send_status  as pct
  from 
    base
  order by 
    pct desc  
)

select * from send_status
