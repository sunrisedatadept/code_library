with

results as (
  
  select 
  	*
  
  from 
    tmc_thrutalk.pa_sun_call_results
 
)

select count(*)  from results
