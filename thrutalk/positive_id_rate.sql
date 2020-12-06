with 

results as (
  select 
  	*
  from 
    tmc_thrutalk.pa_sun_call_results
  where 
    date_called::date in ('2020-11-22'::date, '2020-11-24'::date, '2020-11-29'::date, '2020-12-01'::date, '2020-12-03'::date, '2020-12-06'::date, '2020-12-07'::date)

), script_results as (
  
  select 
    scripts.call_result_id,
    case when question ilike 'starting_question' then answer else null end as starting_question, 
    case when question ilike 'maybe_cynical_about_voting_response_1' then answer else null end as maybe_cynical_about_voting, 
    case when question ilike 'will_not_register_to_vote' then answer else null end as will_not_register_to_vote, 
    case when question ilike 'vote_tripling' then answer else null end as vote_tripling, 
    case when question ilike 'will_you_register_and_vote_for_ossoff_and_warnock_' then answer else null end as register_and_vote_id
    
  from 
    tmc_thrutalk.pa_sun_script_results scripts 

  
), final as (  
  
  select 
    *
  from 
    results 
 
  left join 
    script_results 
    on results.id = script_results.call_result_id
  
 ) 

select 

  sum(case when register_and_vote_id ilike 'yes' then 1 else 0 end)::decimal / 
  sum(case when result ilike '%correct person%' then 1 else 0 end)::decimal as pct_positive_id
from final
