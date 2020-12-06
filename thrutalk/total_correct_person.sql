with

results as (
  
  select 
  	*
  
  from 
    tmc_thrutalk.pa_sun_call_results
  
), final as (

select 
  count(*)  

from 
  results 

where 
  result ilike 'Talked to Correct Person'

) 

select * from final 
