drop table if exists  sunrise_spoke.campaign_title_separated;
create table sunrise_spoke.campaign_title_separated distkey(id)  as

with 

clean as(
  select 
  	camp.id,
  	camp.title,
  	camp.created_at::date,
  	title::varchar end as clean_title
  
  from 
    sunrise_spoke.campaign camp

), base as (
  
  select
    id,
    clean_title as title,
    split_part(clean_title, '_', 1) as designation,
    split_part(clean_title, '_', 2) as camp_tag,
    split_part(clean_title, '_', 3), 'YYYY-MM-DD')::date end as camp_date,
    split_part(clean_title, '_', 4) as name
  	
  from clean
  where len(camp_tag) > 2 

)

select * from base;

grant select on sunrise_spoke.campaign_title_separated to periscope_sun
