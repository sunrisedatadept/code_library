with 

base as (
  select
    interaction_step.question,
    value AS response,
    count(distinct cc.external_id) as num

  from sunrise_spoke.message mes 

  left join sunrise_spoke.campaign_contact cc 
    on cc.id = mes.campaign_contact_id  

  left join sunrise_spoke.question_response qr 
    on qr.campaign_contact_id = cc.id

  left join 
    sunrise_spoke.interaction_step interaction_step 
    on qr.interaction_step_id = interaction_step.id

  left join sunrise_spoke.campaign campaign 
    on cc.campaign_id = campaign.id

  left join sunrise_spoke.campaign_title_separated camp_adj 
    on cc.campaign_id = camp_adj.id

  where camp_adj.camp_tag in ('GAEVPTV')
  
  group by 1, 2

), final as (

  select 
    question,
    response,
    sum(num) as num
  from base
  where question ilike 'Vote?'
  group by 1, 2
  
) select * from final
