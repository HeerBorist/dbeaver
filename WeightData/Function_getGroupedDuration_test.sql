--------------------------------------------------------------------------------
argocount_datas_pbd
materialized_hypertable_231 ==> 1234
pattyntestplcts.pbd_argocount_1 ==> 1237

select add_retention_policy('pattyntestplcts.pbd_argocount_1', interval '7 days')


select * from pattyntestplcts.pbd_argocount_1 
order by starttime desc
limit 100


argocheck_datas_pbd :: compress after 1 day ?

select remove_compression_policy ('pattyntestplcts.argocheck_datas_pbd'); 
select add_compression_policy ('pattyntestplcts.argocheck_datas_pbd', interval '5 days'); --1242

select remove_compression_policy ('pattyntestplcts.actorobjects'); 


SELECT * FROM timescaledb_information.jobs where job_id > 1233;
SELECT * FROM timescaledb_information.job_stats where job_id =1237;
select *  from timescaledb_information.continuous_aggregates
select * from timescaledb_information.chunks c where hypertable_name like '%231%'

select * from timescaledb_information.compression_settings



---------------------------------------------------------------------------



-- Drop table

-- DROP TABLE public."test_general";

CREATE TABLE public."test_general" (
	"time" timestamptz NOT NULL,
	machineserial varchar(255) NULL,
	recipe varchar(255) NULL,
	recipeloadcounter varchar(255) NULL,
	stmachine_emachineprogramstate float8 NULL
);

--SELECT generate_series(timestamp '2021-06-01', now(), '1 hour') AS hour;

select * from public.test_general order by TIME




--delete from public.test_general


-- functie Dieter
select count(duration) as "Alarm count", sum(duration) as "Alarm time" from (
select (time - prev_time) as duration
from (
	SELECT time, progstate,prev_progstate,
		lag(time) over (order by time) as prev_time
	  FROM (
				SELECT time
					, stmachine_emachineprogramstate as progstate
	           		, LAG(stmachine_emachineprogramstate) OVER (ORDER BY time ) AS prev_progstate
	       FROM public.test_general
	       where 
	       	stmachine_emachineprogramstate is not null 
	       	 --and recipeloadcounter = '306'
	       	 --and  $__timeFilter(time)	       	
	       	order by time  DESC
	       	) x
	  WHERE 
	  	progstate <> COALESCE(prev_progstate, progstate) --coalesce only necessary for the very first row, to avoid a 'null' previous value by replacing it with the actual value
	  	and 
	  	(progstate = 600 or prev_progstate = 600)
	  ORDER BY time asc
	  ) x
where prev_time is not null 
	and progstate <> 600
order by time desc
) x

-- herschreven om logica te kunnen volgen : dit kijkt enkel naar het verleden ? laatste waarde wordt niet in rekening gebracht...

with 
a as (
		SELECT time
					, stmachine_emachineprogramstate as progstate
	           		, LAG(stmachine_emachineprogramstate) OVER (ORDER BY time ) AS prev_progstate
	       FROM public.test_general
	       where 
	       	stmachine_emachineprogramstate is not null 
	       	 --and  $__timeFilter(time)	       	
	       	order by time 
	       	),
-- select * from a
b as (
	SELECT time, progstate,prev_progstate,
		lag(time) over (order by time) as prev_time 
		from a 
	WHERE 
	  	progstate <> COALESCE(prev_progstate, progstate) --coalesce only necessary for the very first row, to avoid a 'null' previous value by replacing it with the actual value
	  	and 
	  	(progstate = 600 or prev_progstate = 600)
	  	--ORDER BY time asc --order niet nodig 
	),
--select * from b
c as (
	select (time - prev_time) as duration 
	from b
	where prev_time is not null -- kan niet meer null zijn ? al een coalesce toegepast ??
	and 
	progstate <> 600
	--order by time desc --order niet nodig
)
--select * from c
select count(duration) as "Alarm count", sum(duration) as "Alarm time" from  c 



-- nieuwe test : duration and count per group ???

/*
WITH grouped_progstate AS (
  SELECT g.stmachine_emachineprogramstate ,
         g."time",
         g."machineserial",
         (
           g."recipe",
           DENSE_RANK() OVER (ORDER BY  g.machineserial, date_part('epoch', g.time)) 
         - DENSE_RANK() OVER (PARTITION BY g.stmachine_emachineprogramstate ORDER BY g.machineserial ,date_part('epoch', g.time))
         ) AS recipe_group
         
  FROM public.test_general g 
  --where g."time"> from_time and g."time" < to_time
  --and g."machine"  = machine_name --'%AVL 24%'  -- machines, lijnen, etc...op voorhand uitsplitsen !!!
  -- and g.'line' = line
  )
select * from grouped_progstate
  
		SELECT   
				MIN(gr."recipe")::varchar as recipe, 
		       MIN(time) AS time_from,
		       MAX(time) AS time_to
		FROM grouped_recipes gr
		GROUP BY gr."machine",  gr.recipe_group
		HAVING COUNT(1) > 1
		ORDER BY MIN(time) desc;
*/
-- flodders 	


with intervals as (	
select --*,
--time, 
stmachine_emachineprogramstate ,
--lead(time) over (order by time) as next_time ,
--coalesce(lead(time) over (order by time), now()) as fixed_end_time, --change the now to the end of the time-frame !!
--DENSE_RANK() OVER (order by g.time) as timerank1,
--DENSE_RANK() OVER (PARTITION BY g.stmachine_emachineprogramstate ORDER BY g.time) as state_timerank,
--(DENSE_RANK() OVER (order by g.time)  -
--DENSE_RANK() OVER (PARTITION BY g.stmachine_emachineprogramstate ORDER BY g.time)) as delta,
(g.stmachine_emachineprogramstate , (DENSE_RANK() OVER (order by g.time)  - DENSE_RANK() OVER (PARTITION BY g.stmachine_emachineprogramstate ORDER BY  g.time)) ) as state_delta,
coalesce(lead(time) over (order by time), '2021-06-02 09:00:00')  - time as duration

from public.test_general g 
where stmachine_emachineprogramstate is not null --ignore the null values ! could make a difference, so check how your data is collected !!
order by time asc
)
select stmachine_emachineprogramstate , count(distinct(state_delta)),sum(duration) as total_duration from intervals 
group by stmachine_emachineprogramstate



-- the basic query : 

with intervals as (	
		select 
		time, stmachine_emachineprogramstate ,
		(g.stmachine_emachineprogramstate , (DENSE_RANK() OVER (order by g.time)  - DENSE_RANK() OVER (PARTITION BY g.stmachine_emachineprogramstate ORDER BY  g.time)) ) as state_group, -- create groups per programstate !!
		coalesce(lead(time) over (order by time), now())  - time as duration --fill out a pre-defined time if there is no next record ! Now(), or end of queried time-frame ? 
		from pattyntestplcts.general g 
		where stmachine_emachineprogramstate is not null --ignore the null values ! could make a difference, so check how your data is collected !! - on change / hybrid / ...
		order by time asc
		)
select 
	stmachine_emachineprogramstate 
	,count(distinct(state_group)),sum(duration) as total_duration 
	from intervals 
--	where stmachine_emachineprogramstate = 600
--	where   time <= now() and time > '2021-05-30'
	group by stmachine_emachineprogramstate


	-- kan het nog generieker ? 

	
	
with 
preselected_data as (
		select 
		time, 
		machineserial as group1,
		recipeloadcounter as group2,
		stmachine_emachineprogramstate as status
		from general 
		where stmachine_emachineprogramstate is not null --ignore the null values ! could make a difference, so check how your data is collected !! - on change / hybrid / ...
		order by time asc
	),
intervals as (	
		select 
		time, status , group1, group2, 
		-- create groups per programstate !!
		(status , (DENSE_RANK() OVER (order by group1, group2, time )  - DENSE_RANK() OVER (PARTITION BY status ORDER BY  group1, group2, time)) ) as status_group, 
		--find timestamp for the next record, fill out a pre-defined time if there is no next record ! Now(), or end of queried time-frame ?
		coalesce(lead(time) over (order by time), now())  - time as duration  
		from preselected_data
		)
--	select * from intervals
	select 
	group1, 
	group2, 
	status 
	,count(distinct(status_group)),
	sum(duration) as total_duration 
	from intervals 
--	where status = 600
--	where   time <= now() and time > '2021-05-30'
	group by group1, group2, status
	
	
-- format explained :
--	https://ourtechroom.com/fix/dynamic-column-tablename-in-sql-statement-postgresql-query/
	
select format ('select count(*) from %s WHERE %s = %s', 'mytable', 'mycolumn', 100) 
select exists('general')



-- Create a generic function for grouping status fields



create or replace function 
	
with 
preselected_data as (
		select 
		time, 
--		machineserial as group1,
		recipeloadcounter as group1,
		stmachine_emachineprogramstate as status
		from general 
		where stmachine_emachineprogramstate is not null --ignore the null values ! could make a difference, so check how your data is collected !! - on change / hybrid / ...
		and recipeloadcounter='55'
		order by time asc
	),
intervals as (	
		select 
		time, status, group1,-- group2, 
		-- create groups per programstate !!
--		(status , (DENSE_RANK() OVER (order by group1, group2, time )  - DENSE_RANK() OVER (PARTITION BY status ORDER BY  group1, group2, time)) ) as status_group,
		(status , (DENSE_RANK() OVER (order by group1, time )  - DENSE_RANK() OVER (PARTITION BY status ORDER BY  group1, time)) ) as status_group, 
		--find timestamp for the next record, fill out a pre-defined time if there is no next record ! Now(), or end of queried time-frame ?
		coalesce(lead(time) over (order by time), now())  - time as duration  
		from preselected_data
		)
--	select * from intervals
	select 
	group1, 
--	group2, 
	status 
	,count(distinct(status_group)),
	sum(duration) as total_duration 
	from intervals 
--	where status = 600
--	where   time <= now() and time > '2021-05-30'
	group by group1,status --group1, group2, status
	order by group1, status --group1, group2, status
	


	
-- elaborated	
with 
preselected_data as (
		select 
		time, 
		machineserial as group1,
		recipeloadcounter as group2,
		stmachine_emachineprogramstate as status
		from general 
		where stmachine_emachineprogramstate is not null --ignore the null values ! could make a difference, so check how your data is collected !! - on change / hybrid / ...
		order by time asc
	),
intervals as (	
		select 
		time, status , group1, group2, 
		-- create groups per programstate !!
		(status , (DENSE_RANK() OVER (order by group1, group2, time )  - DENSE_RANK() OVER (PARTITION BY status ORDER BY  group1, group2, time)) ) as status_group, 
		--find timestamp for the next record, fill out a pre-defined time if there is no next record ! Now(), or end of queried time-frame ?
		coalesce(lead(time) over (order by time), now())  - time as duration  
		from preselected_data
		),
groupedresult as (
	select 
	group1, 
	group2, 
	status 
	,count(distinct(status_group)),
	sum(duration) as total_duration 
	from intervals 
	group by group1, group2, status
	order by group1, group2, status
	)
	
select group1, group2, 'alarm' as status , sum(count), sum(total_duration) from groupedresult
where status = 600
group by group1, group2
union
select group1, group2, 'waiting' as status , sum(count), sum(total_duration) from groupedresult
where status in (100,200,300,400,500) 
group by group1, group2




-- create a function : 

create or REPLACE function public.test_general_state(starttime timestampwhithout time zone , endtime timestamp whithout time zone, vRecipeCounter as varchar )
-- filter on 'time bucket'

drop function public.pattyn_general_machinestate( vRecipeLoadCounter varchar )
create or REPLACE function public.pattyn_general_machinestate( vRecipeLoadCounter varchar )
-- filter on recipeloadcounter
RETURNS table(group1 varchar, group2 varchar, machinestate float8, nrofoccurences integer, total_duration interval)

LANGUAGE plpgsql
AS $function$

begin 

	return query 

		with 
		preselected_data as (
				select 
				time, 
				machineserial as group_1,
				recipeloadcounter as group_2,
				stmachine_emachineprogramstate as status
				from general 
				where stmachine_emachineprogramstate is not null --ignore the null values ! could make a difference, so check how your data is collected !! - on change / hybrid / ...
				and recipeloadcounter = '55' --$1
				--and time < endtime and time > starttime
				order by time asc
			),
		intervals as (	
				select 
				time, status, group_1, group_2, 
				-- create groups per programstate !!
				(status , (DENSE_RANK() OVER (order by group_1, group_2, time )  - DENSE_RANK() OVER (PARTITION BY status ORDER BY  group_1, group_2, time)) ) as status_group,
		--		(status , (DENSE_RANK() OVER (order by group1, time )  - DENSE_RANK() OVER (PARTITION BY status ORDER BY  group1, time)) ) as status_group, 
				--find timestamp for the next record, fill out a pre-defined time if there is no next record ! Now(), or end of queried time-frame ?
				coalesce(lead(time) over (order by time), now())  - time as duration  
				from preselected_data
				)
		--	select * from intervals
			select 
			group_1 as group1, 
			group_2 as group2, 
			status as machinestate , 
			count(distinct(status_group))::integer as nrofoccurences,
			sum(duration) as total_duration 
			from intervals 
		--	where status = 600
			group by group1, group2, status
			order by group1, group2, status
			;
end 
$function$
;





select * from  pattyn_general_machinestate('55')


drop  function public.test_ssav_1(recipeloadcounter_in varchar)
create or replace function public.test_ssav_1(recipeloadcounter_in varchar)
returns setof general
language plpgsql
as 
$$
begin 
--	raise notice 'input is %',$1;
	return  query
	select * from general where recipeloadcounter = $1;
end $$;


select * from test_ssav_1('55')
select test_ssav_1('55') 
 
-- test van een kleine functie
drop  function public.test_getlastbatch_general()
create or replace function public.test_getlastbatch_general()
returns setof general
language plpgsql
as 
$$
begin 
return query 
select * from general where recipeloadcounter = (select max(recipeloadcounter) from general);
end $$;




