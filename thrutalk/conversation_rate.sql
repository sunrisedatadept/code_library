with 

results as (
  select 
  	*
  from 
    tmc_thrutalk.pa_sun_call_results
  where 
    date_called::date in ('2020-11-22'::date, '2020-11-24'::date, '2020-11-29'::date, '2020-12-01'::date, '2020-12-03'::date, '2020-12-06'::date, '2020-12-07'::date)

)

select 
  
  sum(case when result ilike 'Talked to Correct Person' then 1 else 0 end)::decimal
  / count(*)::decimal as pct_convo

from results
