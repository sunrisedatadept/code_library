with 

base_response as (
  select 
    campaign_contact_id,

    sum(case when is_from_contact = 'f' then 1 else 0 end) as num_text_sent,
    sum(case when is_from_contact = 't' then 1 else 0 end) as num_text_received

  from sunrise_spoke.message mes 
 
  left join sunrise_spoke.campaign_contact cc 
   on cc.id = mes.campaign_contact_id
  
  left join sunrise_spoke.campaign_title_separated camp_adj 
   on cc.campaign_id = camp_adj.id
  
  left join sunrise_spoke.campaign camp 
   on cc.campaign_id = camp.id
  
  where 
    cc.is_opted_out = 'f'
  
  group by 
    campaign_contact_id
  
  order by 
    num_text_received asc

), response_rate as (

  select
     sum(case when base_response.num_text_received > 0 then 1 else 0 end)::decimal 
    / sum(case when base_response.num_text_received >= 0 then 1 else 0 end)::decimal  as  response_rate

  from 
    base_response
  )

select * from response_rate
