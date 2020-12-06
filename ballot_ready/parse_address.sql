-- from The Movement Cooperative
-- written by Melissa Woods

-- BallotReady data parsing address and pulling voterbase_id from communal data - using the same code that feeds the BR dashboard
with 

base_info as (
    select 
      distinct session_id
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
    , last_value(notifications_opt_in ignore nulls) over (partition by session_id order by created_at_utc 
            rows between unbounded preceding and unbounded following) as notifications_opt_in
    from tmc_ballotready.sunrise_address_forms
	
 ), parse_address as (
    select 
   			* 
      , split_part(address_form_full_address,', ',1) as address_form_parsed_address
      , split_part(address_form_full_address,', ',2) as address_form_parsed_city
      , case when len(split_part(address_form_full_address,', ',3))=2 then split_part(address_form_full_address,', ',3) else NULL end as address_form_parsed_state
      , REGEXP_REPLACE(address_form_phone, '[^[:digit:]]', '') as clean_phone
    from base_info

), base_ptv_info as (
	select
	
	ballot_id
	, case when action='maptv: voting plan created' and (
		json_extract_path_text(btrim(regexp_replace(properties,'\\\\',''),'"'),'email') is not null OR
	 	json_extract_path_text(btrim(regexp_replace(properties,'\\\\',''),'"'),'phone') is not null
	) then 1 else 0 end as raw_maptv_flow_complete_notification_opt_in
	from tmc_ballotready.sunrise_vbm
	where ballot_id is not null
	
), ptv_info as (
	select
	distinct ballot_id
	, max(raw_maptv_flow_complete_notification_opt_in) over (partition by ballot_id) as maptv_flow_complete_notification_opt_in
	from base_ptv_info
	
	
), person as (
  	select 
  			person_id
      , voterbase_id 
      , row_number() over (partition by person_id) as dup
    from tmc_contacts.sunrise_person 
  	where tool = 'ballotready'
  
), ballotready as (
  	select 
  		CASE WHEN b.ballot_id IS null then null ELSE 1 END AS entered_widget
    , case when b.address_form_notification_opt_in ilike 'true' then 'true'
	   when a.notifications_opt_in ilike 'true' then 'true'
	  -- when in_person_reminder_submitted=1 then 'true'
	   when p.maptv_flow_complete_notification_opt_in =1 then 'true'
	   else 'false' end as notification_opt_in
	
    , b.check_registration_voterbase_id
    , a.address_form_full_address
    , coalesce(a.address_form_parsed_address, b.ballot_request_mailed_address1) as br_address
    , coalesce(a.address_form_parsed_city, b.ballot_request_mailed_city) as br_address_city
    , coalesce(a.address_form_parsed_state, b.ballot_request_mailed_state) as br_address_state
    , case when a.address_form_email is not null then lower(a.address_form_email) else b.ballot_id end as p_join
    from 
  		tmc_ballotready.sunrise_ballots b
    left join 
  		parse_address a 
  		using (session_id)
    left join ptv_info p using (ballot_id)

  
), ballotready_vbid as (
  	select 
  		b.*
    , coalesce(b.check_registration_voterbase_id, p.voterbase_id) as br_voterbase_id
    from ballotready b
    left join 
  		(select * from person where dup=1) p 
  		on p_join=p.person_id

)

select 
    
    *
    
 from ballotready_vbid 
    
