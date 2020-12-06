with 

base as (
    select
      events.title , 
      convert_timezone(events.timezone, timeslots.start_date::timestamp) as start_date ,
      timeslots.end_date , 
      part.user__given_name as first_name , 
      part.user__family_name as last_name, 
      part.user__email_address  as email,
      part.referrer__utm_source as utm_source,
      row_number() over (partition by part.id order by part.created_date::date desc) = 1 as is_most_recent
      
    from sun_mobilize.participations part
    left join sun_mobilize.events events on part.event_id = events.id
    left join sun_mobilize.timeslots timeslots on timeslots.id = part.timeslot_id 
     
), de_dup as (
    select 
      title,
      start_date,
      utm_source,
      first_name,
      last_name,
      email
  from base where is_most_recent

), coordinated_base  as (
    SELECT
      events.title ,  
      convert_timezone(events.timezone, timeslots.start_date::timestamp) as start_date ,
      part.user__given_name as first_name , 
      part.user__family_name as last_name, 
      part.user__email_address  as email,
      part.referrer__utm_source as utm_source,
      row_number() over (partition by part.id order by part.created_date::date desc) = 1 as is_most_recent
  from  sunrise2020_mobilize.participations part
  left join sunrise2020_mobilize.events events on part.event_id = events.id
  left join sunrise2020_mobilize.timeslots timeslots on timeslots.id = part.timeslot_id   

), coord_de_dup as (
    select 
      title,
      start_date,
      utm_source,
      first_name,
      last_name,
      email
  from coordinated_base  
  where is_most_recent

), coord_and_ie as (
  
  select 
      *
  from 
      de_dup
  UNION ALL
  select 
      *
  from 
      coord_de_dup

), final as (

  select 
        utm_source,
        count(*) as num_signups

  from coord_and_ie
  group by utm_source
  order by num_signups desc
  
) 
  
select * from final
