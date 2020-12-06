with 

base_response as (
  select 
    mes.campaign_contact_id,
    cc.is_opted_out,
    sum(case when is_from_contact = 'f' then 1 else 0 end) as num_text_sent,
    sum(case when is_from_contact = 't' then 1 else 0 end) as num_text_received

  from 
    sunrise_spoke.message mes 
  
  left join 
    sunrise_spoke.campaign_contact cc on cc.id = mes.campaign_contact_id
  
  left join 
    sunrise_spoke.campaign_title_separated camp_adj on cc.campaign_id = camp_adj.id
  
  left join 
    sunrise_spoke.campaign camp on cc.campaign_id = camp.id

  group by 
    campaign_contact_id, cc.is_opted_out
  
  order by 
    num_text_received asc

), opt_out_rate as (

  select 
    sum(case when is_opted_out ='t' then 1 end)::decimal 
    / count(campaign_contact_id)::decimal as opt_out_rate
   
  from 
    base_response
  
 )

select * from opt_out_rate
