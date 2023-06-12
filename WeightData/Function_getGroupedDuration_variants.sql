--create UUID :
--https://www.postgresqltutorial.com/postgresql-uuid/
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
SELECT uuid_generate_v4();

--Note: If you only need randomly-generated (version 4) UUIDs, 
--consider using the gen_random_uuid() function from the pgcrypto module instead.

select gen_random_uuid();



---------------------------recipeloadcounter------------------------------------------------------------------------

--drop function public.pattyn_general_machinestate( v_RecipeLoadCounter varchar )

create or REPLACE function public.pattyn_general_machinestate( v_recipeLoadcounter varchar )

RETURNS table(group1 varchar, group2 varchar, machinestate float8, nrofoccurences integer, total_duration interval)

LANGUAGE plpgsql
AS $function$
-- filter on 1 recipeloadcounter

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
				and recipeloadcounter = $1
				--and time < endtime and time > starttime
				order by time asc
			),
		intervals as (	
				select 
				time, status, group_1, group_2, 
				-- create groups per programstate !!
				(status , (DENSE_RANK() OVER (order by group_1, group_2, time asc )  - DENSE_RANK() OVER (PARTITION BY status ORDER BY  group_1, group_2, time asc )) ) as status_group,
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

--select * from pattyn_general_machinestate('56')
-- select * from general where recipeloadcounter in ('55','56')

--drop function public.pattyn_general_machinestate1( v_RecipeLoadCounter varchar )
create or REPLACE function public.pattyn_general_machinestate1( v_recipeLoadcounter varchar )

returns setof RECORD 
LANGUAGE plpgsql
AS $function$
-- filter on 1 recipeloadcounter
declare result 	RECORD

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
				and recipeloadcounter = $1
				--and time < endtime and time > starttime
				order by time asc
			),
		intervals as (	
				select 
				time, status, group_1, group_2, 
				-- create groups per programstate !!
				(status , (DENSE_RANK() OVER (order by group_1, group_2, time asc )  - DENSE_RANK() OVER (PARTITION BY status ORDER BY  group_1, group_2, time asc )) ) as status_group,
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

--select * from pattyn_general_machinestate('56')

-- https://blog.timescale.com/blog/sql-functions-for-time-series-analysis/
-- gap filling and locf

select time, stmachine_emachineprogramstate,* from general where recipeloadcounter in ('55','56')






	select --time_bucket(	'1 minute', g.time) as oneminute,
	time_bucket_gapfill('1 minute', g.time, '2021-07-11 15:27:08', '2021-07-12 05:56:50') as oneminute1,
	recipeloadcounter,
	locf(avg(stmachine_emachineprogramstate)) as forwarded
	from general g 
	where time >= '2021-07-11 15:27:08' and time <= '2021-07-12 05:56:50'
	group by  oneminute1,recipeloadcounter--, stmachine_emachineprogramstate
	order by oneminute1


	
test=*# update bf set title = (select title from bf as prev where title
is not null and prev.rowid < bf.rowid order by prev.rowid desc limit 1)
where title is null;


--drop function public.pattyn_general_machinestatebackfilled( v_recipeLoadcounter varchar )

create or REPLACE function public.pattyn_general_machinestatebackfilled( v_recipeLoadcounter varchar default '0')

returns setof general 
LANGUAGE plpgsql
AS $function$

-- filter on 1 recipeloadcounter
	declare r general%rowtype;
	declare previousval integer := null;
	declare actualval integer := null;

begin 
	drop table if exists  tmp_general ;

		create temp table tmp_general as select * from general 
		where recipeloadcounter = $1
		order by time;
	
		alter table tmp_general add column uuid varchar(255) default gen_random_uuid();

-- sub block !

	declare 
		t tmp_general%rowtype;
	begin --start transaction

		for t in 
			select * from tmp_general 
	--		group by machineserial,time
			order by machineserial, time asc 
			
		loop 
			--raise notice 'selected record : %', t;
			actualval :=  t.stmachine_emachineprogramstate ;
			raise notice 'selected record : %', t.uuid;
		
			if (actualval is null) and (previousval is not null) then --and <> previousval 
			 -- update to previous value ?
				--raise notice 'selected record : %',r;
				--update tmp_general set stmachine_emachineprogramstate = previousval where ctid = r.ctid
			 	raise notice 'updated actual value = % to previous value : %', actualval, previousval;		
	 		end if;
	 		-- condities toevoegen !!
			previousval :=  actualval ;		
		
			-- do some processing here
	
			return next r; --return current row of select...? waarom moet ik r gebruiken ??
			--raise notice '% records have been checked ! ', row_count;
		end loop;
		--return;
		alter table tmp_general drop column uuid;
	--commit;
	end; --$subblock$;


-- hier eerst iets committen misschien ??
	return;
--	return query 
--	select * from tmp_general;

end ;

$function$
;

select * from general where recipeloadcounter = '55'
select * from pattyn_general_machinestatebackfilled('55')

select * from tmp_general
--delete from tmp_general
-- drop table tmp_general


	alter table tmp_general add column uuid varchar(255) default gen_random_uuid();
-- alter table tmp_general add column uuid1 varchar(255) generated always as (gen_random_uuid()) stored;
-- ERROR: generation expression is not immutable

