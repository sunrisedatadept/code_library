-- from The Movement Cooperative
-- written by Melissa Woods

with base_info as (
  select distinct session_id
  , last_value(case when right(upper(address),3)='USA' then upper(address) else NULL end ignore nulls) 
  	over (partition by session_id order by created_at_utc 
          rows between unbounded preceding and unbounded following) as address_form_full_address
  , last_value(first_name ignore nulls) over (partition by session_id order by created_at_utc 
    		rows between unbounded preceding and unbounded following) as address_form_first_name
  , last_value(last_name ignore nulls) over (partition by session_id order by created_at_utc 
        rows between unbounded preceding and unbounded following) as address_form_last_name
  , last_value(phone ignore nulls) over (partition by session_id order by created_at_utc 
        rows between unbounded preceding and unbounded following) as address_form_phone
  , last_value(email ignore nulls) over (partition by session_id order by created_at_utc 
        rows between unbounded preceding and unbounded following) as address_form_email
  from tmc_ballotready.sunrise_address_forms
)
  
, parse_address as (
  select * 
  , split_part(address_form_full_address,', ',1) as address_form_parsed_address
  , split_part(address_form_full_address,', ',2) as address_form_parsed_city
  , case when len(split_part(address_form_full_address,', ',3))=2 then split_part(address_form_full_address,', ',3) else NULL end as address_form_parsed_state
  , replace(replace(replace(address_form_phone,'-',''),'(',''),')','') as clean_phone
  from base_info
  )

, ballots_base as (
  select b.ballot_id -- one per "ballot"
  , b.session_id
  , b.created_at_utc
  , b.tenant_id -- this is per instance. Members using the shared instance are all under 431, members with whitelabeled have their own
  , b.utm_source -- this is used for segmentation if the member is on the shared instance. It should be their member code. 
  , b.utm_medium
  , b.utm_campaign
  , b.utm_content
  , b.utm_term
  , b.reminder_step_complete
  , b.ballot_state
  , b.ballot_request_mailed_state
  , b.check_registration_voter_id 
  , b.check_registration_voterbase_id
  , b.intro_get_started_clicked
  , b.ask_registration_choice              
  , b.check_registration_found
  , b.check_registration_failed_choice
  , b.verify_registration_first_name
  , b.ballot_request_downloaded
  , b.ballot_request_emailed
  , b.election_portal_clicked
  , b.ballot_request_mailed 
  , b.final_page_reached              
  , b.ballot_submitted 
  , b.ballot_submitted_accepts_digital_signature -- this is for people that submit directly to the local election authority! super important for knowing which people have had their ballot requests actually submitted there
            
  , CASE WHEN b.ballot_id IS null then null
 		ELSE 1
		END AS entered_widget
  
  , b.address_form_notification_opt_in
  from tmc_ballotready.sunrise_ballots b
	) 

, ballots as (
  select b.*
  , a.address_form_full_address
  , a.address_form_first_name
  , a.address_form_last_name
  , a.address_form_email
  , a.address_form_parsed_address
  , a.address_form_parsed_city
  , a.address_form_parsed_state 
  , a.clean_phone as address_form_clean_phone

  
  , CASE WHEN coalesce(ballot_state, address_form_parsed_state) in ('CA', 'CO', 'NJ', 'DC', 'VT', 'UT', 'NV', 'WA', 'OR', 'HI') THEN TRUE -- these are auto-vbm states
  	WHEN check_registration_failed_choice = 'Help me register.' THEN TRUE -- these are people who were not registered
  	WHEN ballot_request_downloaded = 'TRUE' THEN TRUE
    WHEN ballot_request_emailed = 'TRUE' THEN TRUE
    WHEN election_portal_clicked = 'TRUE' THEN TRUE
    WHEN ballot_request_mailed = 'TRUE' THEN TRUE
    WHEN ballot_submitted = 'TRUE' THEN TRUE
  	else NULL end as Completed_flow
  
  , CASE WHEN coalesce(ballot_state, address_form_parsed_state) in ('CA', 'CO', 'NJ', 'DC', 'VT', 'UT', 'NV', 'WA', 'OR', 'HI') THEN TRUE -- these are auto-vbm states
  	WHEN ballot_request_downloaded = 'TRUE' THEN TRUE
    WHEN ballot_request_emailed = 'TRUE' THEN TRUE
    WHEN election_portal_clicked = 'TRUE' THEN TRUE
    WHEN ballot_request_mailed = 'TRUE' THEN TRUE
    WHEN ballot_submitted = 'TRUE' THEN TRUE
  	else NULL end as Completed_flow_vbm --removed the failed registration flow piece to have a completed flow column to use for the main_strikelist
  
  , CASE WHEN coalesce(ballot_state, address_form_parsed_state) in ('CA', 'CO', 'NJ', 'DC', 'VT', 'UT', 'NV', 'WA', 'OR', 'HI') THEN 'VBM State' -- these are auto-vbm states
  	WHEN ballot_request_downloaded = 'TRUE' THEN 'Downloaded'
    WHEN ballot_request_emailed = 'TRUE' THEN 'Emailed'
    WHEN election_portal_clicked = 'TRUE' THEN 'State Portal'
    WHEN ballot_request_mailed = 'TRUE' THEN 'Mailed'
    WHEN ballot_submitted = 'TRUE' THEN 'Fillable Form'
  	else NULL end as ballot_method
  
,case when address_form_email is not null then lower(address_form_email)
  						else ballot_id end as p_join
  from ballots_base b
	left join parse_address a using (session_id)
)

select 
*
from ballots
