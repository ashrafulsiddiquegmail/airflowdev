-- Challenge 1
-- SQL 1
-- Latest row by ops_loaded_datetime for same content_id 
with ranked_data as 
(
select content_id,
	title,
	primary_category,
	primary_tag,
	ops_loaded_datetime,
	ROW_NUMBER() over (partition by content_id order by ops_loaded_datetime desc) row_num
from Input_table
)
select content_id,
	title,
	primary_category,
	primary_tag,
	ops_loaded_datetime
from ranked_data where row_num = 1;

-- Challenge 2
--SQL 1
-- How many unique users of the 9Now platform watched anything in the series ‘The Block’?
select count(distinct user_id) as user_count
from watch_history
where series_name =  ‘The Block’
;
--SQL 2
-- Out of those users, how many of them have watched the episode named “Julie & John Livingroom Reno”
select count(distinct user_id) as user_count
from watch_history
where series_name =  ‘The Block’ and episode_name = 'Julie & John Livingroom Reno'
;

--SQL 3
-- How many users have watched more than one different episode of The Block?
select count(user_id) as user_count
from(	select  user_id, count(distinct episode_name) as episode_count
	from watch_history
	where series_name =  ‘The Block’ 
	GROUP by user_id
	having count(distinct episode_name) > 1
) as users_multi_episode
;


