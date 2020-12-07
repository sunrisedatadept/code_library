WITH results as (
  select 
  	id,
  	max(voter_phone) as voter_phone,
  	max(voter_id) as voter_id,
  	max(date_called) as date_called,
  	max(service_account) as service_account,
  	max(caller_login) as caller_login,
  	max(result) as result
  from tmc_thrutalk.sun_call_results
  group by 1
  ),

-- get script responses to first ID question. Original script had checkbox questions, hence lines 12 through 21
scale1 as (
  select call_result_id, max(answer) as answer 
  from tmc_thrutalk.sun_script_results 
  where question in(
    'trump_to_biden_scale',
    'trump_to_biden_scale_checkbox_value_1',
    'trump_to_biden_scale_checkbox_value_2',
    'trump_to_biden_scale_checkbox_value_3',
    'trump_to_biden_scale_checkbox_value_4',
    'trump_to_biden_scale_checkbox_value_5',
    'trump_to_biden_scale_checkbox_value_6',
    'trump_to_biden_scale_checkbox_value_7',
    'trump_to_biden_scale_checkbox_value_8',
    'trump_to_biden_scale_checkbox_value_9')
  group by 1
  ),

-- get script responses for ballot ready question
ballot_ready as (
  select call_result_id, max(answer) as answer 
  from tmc_thrutalk.sun_script_results 
  where question like 'ballot_ready'
  group by 1
  ),
  
-- get answers to the vote tripple question where people provide 3 names. If not null, then they provided 3 names for vote trippling. 
votetrip as (
  select call_result_id, max(answer) as answer 
  from tmc_thrutalk.sun_script_results 
  where question ilike 'if_yes_to_vt_3_friends_names'
  group by 1
  ),
  
-- get script responses for start question. This is to make sure we exclude everyone who has gotten a call, wrong numbers, refused, ect. We need this because sometimes callers enter call results wrong.
results1 as (
  select call_result_id, max(answer) as answer 
  from tmc_thrutalk.sun_script_results
  where 
  	answer ilike '%wrong%'
  	or answer ilike '%moved%'
  	or answer ilike '%talking%'
    or answer ilike '%refused%'
    or answer ilike '%deceased%' 
    or answer ilike '%disconnected%' 
    or answer ilike '%spanish%'
  group by 1
  ),

--bring it all together in the final base from which we will pull call metrics from 
final_base as (
  select
  	results.id as id,
  results.voter_id as voter_id,
 	results.date_called as date_called,
  	results.service_account as service_account,
  	results.caller_login as caller_login,
  	results.result as result,
  	scale1.answer as trump_to_biden,
  	ballot_ready.answer as ballot_ready,
  	votetrip.answer as votetrip,
  	results1.answer as first_question
  from results
  left join scale1 on scale1.call_result_id = results.id
  left join ballot_ready on ballot_ready.call_result_id = results.id
  left join votetrip on votetrip.call_result_id = results.id
  left join results1 on results1.call_result_id = results.id

), correct as (
	select
    final_base.*,  
  	case
        when voter_id ilike '%mi%' then 'MI'
        when voter_id ilike '%az%' then 'AZ'
        when voter_id ilike '%pa%' then 'PA'
        when voter_id ilike '%fl%' then 'FL'
        when voter_id ilike '%wi%' then 'WI'
        when voter_id ilike '%nc%' then 'NC'
        when voter_id ilike '%tx%' then 'TX'
        -- add SEIU numbers that don't have state in the ID
        when date_called like '2020-10-13' and service_account ilike '%1%' then 'WI'
        else 'UNKNOWN' end as state
  from final_base 
  where result ilike '%correct person%'

), cleaned as (
  
  select 
  	voter_id,
  	UPPER(left(voter_id,2))||'-'|| REGEXP_SUBSTR(voter_id,'[[:digit:]].*') as vb_voterbase_id
  
  from correct
  where state in ('MI', 'AZ', 'PA', 'FL', 'WI', 'NC', 'TX')
  
  )
  
  select 
    cleaned.voter_id,
    vf.vb_voterbase_race,
    vf.vb_voterbase_age,
    vf.vb_voterbase_gender,
    vf.vb_vf_g2016

  from cleaned
  
  left join ts.ntl_current vf 
  	on cleaned.vb_voterbase_id = vf.vb_voterbase_id
    
  
