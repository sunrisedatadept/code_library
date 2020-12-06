with base  as (
    SELECT
      events.title , 
      convert_timezone(events.timezone, timeslots.start_date::timestamp) as start_date ,
      timeslots.end_date , 
      part.user__given_name as first_name , 
      part.user__family_name as last_name, 
      part.user__email_address  as email,

      row_number() over (partition by part.id order by part.created_date::date desc) = 1 as is_most_recent
      from  sun_mobilize.participations part
      left join sun_mobilize.events events on part.event_id = events.id
      left join sun_mobilize.timeslots timeslots on timeslots.id = part.timeslot_id 
      
      where 
        (events.title ilike 'Swing State Phonebank to Defeat Trump & Grow the Movement' 
          and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date > '2020-10-28'::date)  
        or (events.title ilike '%Victory Fest Swing State Texting%')
        or (events.title ilike 'BIPOC Swing State Phonebanks and TextBanks!' 
          and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-10-31'::date)
        or (events.title ilike 'WI Halloween GOTV Phone Bank!' 
          and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-10-31'::date)
        or (events.title ilike 'Relational Victory Party' 
          and (convert_timezone(events.timezone, timeslots.start_date::timestamp)::date != '2020-10-26'::date
          and convert_timezone(events.timezone, timeslots.start_date::timestamp)::timestamp != '2020-10-29 19:00:00'::timestamp))
        or (events.title ilike 'National Phonebank for Sunrise NC GND Champions!' 
          and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-11-01'::date) 

), de_dup as (
    
  select 
    title,
    start_date,
    first_name,
    last_name,
    email

  from 
    base 
  where 
    is_most_recent

), by_event as (
 
   select 
    title,
    start_date, 
    count(email) as num_sign_ups
  
  from 
    de_dup
  
  group by 
    title,
    start_date
 ) ,

--- BEGIN COORDINATED APPEND
coordinated_base  as (
  SELECT

    events.title , 
    case 
      when title ilike '%Hoadley%' then 'Hoadley'  
      when title ilike '%Doglio%' then 'Doglio'  
      when title ilike '%Oliver%' then 'Oliver'  
      when title ilike '%Siegel%' then 'Siegel'  
      when title ilike '%Rashid%' then 'Rashid'  
      when title ilike '%Swearengin%' then 'Swearengin' 
      when title ilike '%Kunkel%' then 'Kunkel' 
      when title ilike '%Denney%' then 'Denney' 
      when title ilike '%Bradshaw%' then 'Bradshaw' else null end as title_adj,
    convert_timezone(events.timezone, timeslots.start_date::timestamp) as start_date ,
    part.user__given_name as first_name , 
    part.user__family_name as last_name, 
    part.user__email_address  as email,
    row_number() over (partition by part.id order by part.created_date::date desc) = 1 as is_most_recent
  
  from 
    sunrise2020_mobilize.participations part
  
  left join sunrise2020_mobilize.events events 
    on part.event_id = events.id
  
  left join sunrise2020_mobilize.timeslots timeslots 
    on timeslots.id = part.timeslot_id 
  
  where (events.title ilike '%Rashid%'
         or events.title ilike '%Hoadley%'
         or events.title ilike '%Swearengin%'
         or events.title ilike '%Bradshaw%'
         or events.title ilike '%Oliver%'
         or events.title ilike '%Kunkel%'
         or events.title ilike '%Denney%'
         or events.title ilike '%Doglio%'
         or events.title ilike '%Siegel%')
    and 
        (title ilike 'Flipping Texas - Sunrise X Texas Dem Party Phone Banks w/ Jessica Cisneros and Julián Castro!' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-10-29')
        or (title_adj ilike 'Denney' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-10-29')
        or (title_adj ilike 'Hoadley' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-10-31')
        or (title_adj ilike 'Doglio' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-10-31')
        or (title_adj ilike 'Bradshaw' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::timestamp = '2020-11-01 12:00:00')
        or (title_adj ilike 'Siegel' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-11-01')
        or (title_adj ilike 'Siegel' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-11-02')
        or (title_adj ilike 'Oliver' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-11-02')
        or (title_adj ilike 'Siegel' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-11-03')     
        or (title_adj ilike 'Rashid' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::date = '2020-11-03'::date)     
        or (title_adj ilike 'Denney' and convert_timezone(events.timezone, timeslots.start_date::timestamp)::timestamp = '2020-11-03 19:00:00')     

), coord_de_dup as (
    
  select 
    title,
    start_date,
    first_name,
    last_name,
    email   

  from 
    coordinated_base  
  
  where 
    is_most_recent
 
), coord_by_event as (
 
    select 
      title, 
      start_date,
      count(email) as num_sign_ups
    
    from 
      coord_de_dup
    
    group by 
      title, 
      start_date
), coord_and_ie as (

  select 
      *
  from 
      coord_by_event
  UNION ALL
  select 
      *
  from 
      by_event

), with_goals as (
  
  select  
    start_date as event_date, 
    title as event_name, 
    num_sign_ups,
    case when event_date::date = '2020-10-29'::date and event_name = 'Relational Victory Party' then 40
      when event_date::date = '2020-10-29'::date and event_name = 'Swing State Phonebank to Defeat Trump & Grow the Movement' then 40
      when event_date::date = '2020-10-29'::date and event_name = 'Phonebank for Audrey Denney CA-01!' then 150
      when event_date::date = '2020-10-29'::date and event_name = 'Flipping Texas - Sunrise X Texas Dem Party Phone Banks w/ Jessica Cisneros and Julián Castro!' then 150
      when event_date::date = '2020-10-29'::date and event_name = 'Phonebank for Audrey Denney CA-01!' then 150
      
      when event_date::date = '2020-10-30'::date and event_name = 'Victory Fest Swing State Texting' then 40
      when event_date::date = '2020-10-30'::date and event_name = 'Relational Victory Party' then 50
      
      when event_date::date = '2020-10-31'::date and event_name = 'Relational Victory Party' then 150
      when event_date::date = '2020-10-31'::date and event_name = 'Phonebank for Jon Hoadley: MI-06' then 150
      when event_date::date = '2020-10-31'::date and event_name = 'WI Halloween GOTV Phone Bank!' then 150
      when event_date::date = '2020-10-31'::date and event_name = 'BIPOC Swing State Phonebanks and TextBanks!' then 150
      when event_date::date = '2020-10-31'::date and event_name = 'Phonebank for Beth Doglio WA-10!' then 150
      
      when event_date::date = '2020-11-01'::date and event_name = 'Relational Victory Party' then 150
      when event_date::date = '2020-11-01'::date and event_name = 'Phonebank for Marquita Bradshaw for U.S. Senate with Sunrise Tennessee!' then 150
      when event_date::date = '2020-11-01'::date and event_name = 'National Phonebank for Sunrise NC GND Champions!' then 150
      when event_date::date = '2020-11-01'::date and event_name = 'Phone Bank for Mike Siegel - TX10!' then 150
      
      when event_date::date = '2020-11-02'::date and event_name = 'Relational Victory Party' then 150
      when event_date::date = '2020-11-02'::date and event_name = 'Phone Bank for Mike Siegel - TX10!' then 150
      when event_date::date = '2020-11-02'::date and event_name = 'Victory Fest Swing State Texting' then 150
      when event_date::date = '2020-11-02'::date and event_name = 'Phone Bank for Julie Oliver - TX25!' then 150

      when event_date::date = '2020-11-03'::date and event_name = 'Relational Victory Party' then 100
      when event_date::date = '2020-11-03'::date and event_name = 'Phone Bank for Mike Siegel - TX10!' then 100
      when event_date::date = '2020-11-03'::date and event_name = 'Sunrise for Qasim Rashid Phonebank!' then 200
      when event_date::date = '2020-11-03'::date and event_name = 'Swing State Phonebank to Defeat Trump & Grow the Movement' then 200
      when event_date::date = '2020-11-03'::date and event_name = 'Phonebank for Audrey Denney CA-01!' then 200

      else null end as shift_goal

from coord_and_ie
where 
  event_date::date != '2020-10-26'::date
  and event_name != 'Sunrise Knox Hub: Phonebank for U.S. Senate Candidate Marquita Bradshaw'
  
), final as (

  select 
    event_date ||' '|| event_name,
    event_date::date as e_date,
    case when extract(hour from event_date) > 12 then to_char(event_date, 'HH:MI AM') else to_char(event_date, 'HH:MI PM') end as event_time,
    event_name,
    num_sign_ups,
    shift_goal,
    final.shift_goal -  final.num_sign_ups as remaining

  from with_goals
  order by event_date::timestamp asc
  
  ) 
  
  select * from final
