
--- aggregatie voor general machine status

select 
	time, line, machine, machineserial, recipe, recipeloadcounter, uuid
--	,first(time, time) as batchstart
--	,last(time, time) as batchend
	--,stmachine_emodestate
	,stmachine_emachineprogramstate--, *
--	,date_part('epoch',(last(time, time)) - (first(time, time) ))/3600 as duration_hours
--select * 
--into general_machine_status
from general 
	where line is not null
--	and machineserial is not null 
--	and recipe is not null
	and recipeloadcounter = '1030'
--	and recipeloadcounter <> 'NoReadOut'
--	and (stmachine_emodestate is not null or stmachine_emachineprogramstate is not null)
--	and stinfo_ntotalcases <> null
--	group by line, machine, machineserial, recipe, recipeloadcounter
--	order by recipeloadcounter::float desc
	order by time asc
	limit 100
	
select count(*) from general --6.275.090

-- the basics : 
--forward fill values ?? need to carryforward the progstate and the mode field !!
--	https://dba.stackexchange.com/questions/156068/using-window-function-to-carry-forward-first-non-null-value-in-a-partition
-----------------------------------------------------------------------------------------------------------
-- programstate	
select --time, line, machine, machineserial, recipe, recipeloadcounter, uuid, stmachine_emachineprogramstate , 
*, first_value(stmachine_emachineprogramstate) over w as carryforward_progstate
from ( 
	select * , 
--		sum(case when stmachine_emachineprogramstate is null then 0 else 1 end) over (partition by recipeloadcounter order by time) as value_partition
		count(stmachine_emachineprogramstate) over (partition by recipeloadcounter order by time) as value_partition -- count is faster in this case !!
		from general 
--		where recipeloadcounter = '1030'
		) as q
window w as (partition by recipeloadcounter, value_partition order by time);  --40sec for 8 months of data...
-----------------------------------------------------------------------------------------------------------

-------------------------------------------------------
-- do an update in an extra column to use for later aggregation.
-- do this update only for the last 5 days ?? or, update every hour, and once per day for the last 5 days...
-- or check if there are any NULL values that have come in the range... then need to update the complete range, starting before the null value....
-- update all information from the last 3 days everytime, because it could happen that 'slow syncing' adds information later than the last update, which would lead to mistakes
-- so : find the first NULL value, go one record back, start update from there... !! this will always lead to correct information after every update...
-------------------------------------------------------
--select add_retention_policy('general', interval '3 months') --2121
-------------------------------------------------------	


-- two functions to update the running state transitions !! 


select * from public.pattyn_general_eprogramstate_carry_forward();
select * from public.pattyn_general_emodestate_carry_forward();


select count(*) from general where recipeloadcounter = '1030' order by time desc limit 250  --13694 records
select count(*) from general where carry_fw_programstate is null ---2121655
select time, line, machine, uuid, recipeloadcounter, boxmade, stmachine_emachineprogramstate, carry_fw_programstate from general order by time desc limit 100

	
-----------------------------------------------------------------------------------------------------------
-- aggregation of the different states can now start : (similar to alarms)

-- assume that, if we change the emodestate, that the program state will change too ??

	-- start met programstate... ?? hiervoor komt ook de stmachine_emodestate 
--	raise notice 'duration % : create temp table temp_alarm_table', clock_timestamp()-t ;
--	drop table temp_mach_status_program
	create temp table temp_mach_status_program as   -- fetch all programstate and the state of the next programstate 
		select g.uuid,
		g.stmachine_emodestate, 
--		g.carry_fw_modestate,
--		g.carry_fw_programstate,
		lead(G.UUID,1) over (partition by g.line, g.machineserial, g.recipe, g.recipeloadcounter order by g.time) as n_uuid,
		G.stmachine_emachineprogramstate,
		lead(G.stmachine_emachineprogramstate,1) over (partition by g.line, g.machineserial, g.recipe, g.recipeloadcounter order by g.time) as n_state
		from general as G
	where line is not null
	and machineserial is not null 
	and recipe is not null
	and recipeloadcounter = '1031'
	and recipeloadcounter <> 'NoReadOut'
	and (stmachine_emodestate is not null or stmachine_emachineprogramstate is not null);

	select count(*) from temp_mach_status_program;
	select * from temp_mach_status_program;
select count(*) from general where recipeloadcounter = '1031' --3182
select * from general where recipeloadcounter = '1031' and stmachine_emachineprogramstate is not null--304

	--drop table temp_mach_status_consolidated
--	raise notice 'duration % , % : create temp table temp_consolidated', clock_timestamp() - t, clock_timestamp() - t1;
	create temp table temp_mach_status_consolidated as -- find the lines where the next state is different from the current state (the transitions...) --put into consolidated
		select c.time, c.line, c.machine, c.machineserial, c.recipe, c.recipeloadcounter,  c.stmachine_emodestate,c.stmachine_emachineprogramstate , c.uuid --, t.n_uuid
		,c.carry_fw_modestate 
		from general C left join temp_mach_status_program T on C.uuid = T.n_uuid 
		where T.stmachine_emachineprogramstate <> T.n_state 
		and C.machineserial is not null
		and C.recipe is not null
--		and C.recipeloadcounter = '1031'
--		and C.recipeloadcounter <> 'NoReadOut'
		and (C.stmachine_emodestate is not null or c.stmachine_emachineprogramstate is not null)
--		group by c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.carry_fw_modestate ,c.stmachine_emachineprogramstate 
		order by c.line, c.machine, c.recipe, c.time; --50 sec
		
select * from temp_mach_status_consolidated;



--	drop table temp_endlines
	create temp table temp_endlines as  --
		select 
		time,line, machine, recipe, recipeloadcounter, uuid , 
		stmachine_emodestate,
		stmachine_emachineprogramstate,
		lead(ac.UUID,1) over (partition by ac.line, ac.machineserial, ac.recipe, ac.recipeloadcounter order by ac.time) as endblock_uuid
		from temp_mach_status_consolidated ac 
		order by ac.line, ac.machine, ac.time asc;

select * from temp_endlines order by time desc , line, machine, recipe, recipeloadcounter;

-- delete from temp_endlines where uuid in general_machine_status_lines.... ?? daarna insert van nieuwe lijnen.

-- zoek van elke opeenvolgende e_machineprogramstate groep de start en eindtijd (en de duration) op basis van temp_endlines ==> machine_status_lines


-- Is this the end result ??? leave the rest for analysis ???

-- still with respect of the time-line, show the different statusses by time, duration
--	drop table temp_general_lines
	create temp table temp_general_lines as  
	select  
		e1.time as time,  previousrow.time as end_time,
			date_part('epoch', (previousrow.time) - (e1.time )) as duration_programstate_sec,
			e1.line, e1.machine, e1.recipe, e1.recipeloadcounter, e1.uuid, e1.stmachine_emachineprogramstate, e1.stmachine_emodestate
	from temp_endlines as E1 ,
	 lateral (select * from temp_endlines E2 where E2.uuid = e1.endblock_uuid) as previousrow
	--where e1.stmachine_emachineprogramstate = 700
	 order by e1.time desc;

	select * from temp_general_lines order by time;  --564 lines  (30x minder)
	 

--now get rid of the time-line, make a summary per batch...	 
 -- samenvatten per batch, per state, sum_duration, count_occurrences, uuid of first occurrence , start_time_of_first_occurrence, endtime_of_last_occurrence ==> machine_status aggregated (per batch)	 
-- drop table temp_general_aggregated
	create temp table temp_general_aggregated as  
	select
		line, machine, recipe, recipeloadcounter, stmachine_emodestate,stmachine_emachineprogramstate,
		first(time,time) as time,
		first(time,time) as start_time_of_first_occurrence,
		last(time, end_time) as endtime_of_lastoccurrence,
  		sum(duration_programstate_sec) as total_duration_seconds,
		sum(duration_programstate_sec/60) as total_duration_minutes,
		sum(duration_programstate_sec/3600) as total_duration_hours,
		count(stmachine_emachineprogramstate) as count_distinct_occurrences,
		first(uuid,time) as uuid_of_first_occurrence,
		last (uuid,time) as uuid_of_last_occurrence
	from temp_general_lines		
	group by line, machine, recipe, recipeloadcounter, stmachine_emodestate,stmachine_emachineprogramstate;

select * from  temp_general_aggregated; 






