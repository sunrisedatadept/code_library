with 

base as (
  select count(distinct(cc.external_id)) as num
  
  from sunrise_spoke.message mes 
  
  left join sunrise_spoke.campaign_contact cc 
    on cc.id = mes.campaign_contact_id
 
  left join sunrise_spoke.campaign_title_separated camp_adj 
    on cc.campaign_id = camp_adj.id
  
  left join sunrise_spoke.campaign camp 
    on cc.campaign_id = camp.id
  
  where  
    camp_adj.camp_tag in ('GAEVPTV', 'gavoterreg')    
    and  cc.is_opted_out = 'f'
    and mes.is_from_contact = 't'
  
) select * from base
