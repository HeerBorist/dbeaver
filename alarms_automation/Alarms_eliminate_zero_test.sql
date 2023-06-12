
CREATE OR REPLACE FUNCTION pattyntestplcts.pattyn_alarm_lines_speed_test()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$

<<startblock>>

/*
declare 
	_S	ALIAS for schemaname;
 	_safeschema boolean;
	_T varchar := 'alarms';
	_tableexists boolean;
	_dummy text;
*/
declare t timestamptz := clock_timestamp();
declare t1 timestamptz := clock_timestamp();
begin 	
/*
	-- check for special characters or keywords to prevent SQL injection !!
	_safeschema := false;
	select into strict _safeschema (select pattyn_check_string(_S));

	if not _safeschema then 
		raise notice 'exit function due to unsafe schema name';
		return false;
		exit startblock;
		end if;	

	--raise notice 'session_user : %',session_user;
	--  dynamically set the search path, include public !! 
		--	https://stackoverflow.com/questions/21259551/postgresql-how-do-i-set-the-search-path-from-inside-a-function
	_S := _S || ',public'; --pattyntestplcts,public
    perform set_config('search_path', regexp_replace(_S, '[^\w ,]', '', 'g'), false);
	_tableexists := false;
	_tableexists := (select public.pattyn_table_exists('alarms') is not null);

	if (not _tableexists) then
		raise notice 'exit startblock due to unexisting(?) alarms table in schema %', _S;
		return false;
		exit startblock;
	end if;

	-- all underneath code goes here !

-- Doel : alarms table samenvatten tot max 3 lijnen per alarm blok... (of 2 lijnen bij status 0-1)
-- vertrekken vanaf originele alarm tabel ? : direct duration per lijn toevoegen, en unieke code per alarmblok ??

-- origineel om alle tussenliggende nul waarden te verwijderen. (optimized)
-- zonder de edge kolom voor vereenvoudiging en snelheid van volgende stappen !!	    

-- *************************************************************************************************************************
-- select all alarms from the alarm table, look at the state of the next alarm...


 */
	
	drop table if exists temp_alarm_table;
	drop table if exists temp_consolidated;
	drop table if exists temp_consolidated_out;
	drop table if exists temp_endlines;
	drop table if exists temp_unique_blocks;
	drop table if exists temp_columntranspose;
	drop table if exists temp_single_lines;


	raise notice 'duration % : create temp table temp_alarm_table', clock_timestamp()-t ;
	create temp table temp_alarm_table as   -- fetch all alarms and the state of the next alarm 
		select 	A.alarmstate,a.uuid,skey_1,
		lead(A.UUID,1) over (partition by A.machineserial, A.idx order by A.time) as n_uuid,
		lead(A.alarmstate,1) over (partition by A.machineserial, A.idx order by A.time) as n_state
		from alarms as A
--	where time > (now() - interval'12h') and 
	where coalesce(A.skey_1,'') <> 'out'; --42sec

	t1 := clock_timestamp();
	--select count(*) from temp_alarm_table --12602562
	--select * from temp_alarm_table

	--drop table temp_consolidated
	raise notice 'duration % , % : create temp table temp_consolidated', clock_timestamp() - t, clock_timestamp() - t1;
	create temp table temp_consolidated as -- find the alarms where the next state is different from the current state (the transitions...) --put into consolidated
		select c.time, c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.idx, c.skey, c.alarmstate, c.uuid 
		from alarms C left join temp_alarm_table T on C.uuid = T.n_uuid 
		where T.alarmstate <> T.n_state 
		order by machine, idx, time; --50 sec
	
	-- select count(*) from temp_consolidated  --79620
	--	select * from temp_consolidated order by time desc
	

---*************************************************************-
---*************************************************************-
---*************************************************************
---*************************************************************-
	
---test to elimiminate previous zeross....
	--can we do an update to avoid these lines in the next run ???  Check the ends !!
	
	--	drop table temp_consolidated_out
	raise notice 'duration % : create temp table temp_consolidated_out',  clock_timestamp() - t;
	create temp table temp_consolidated_out as -- find the alarms where the next state is different from the current state (the transitions...) --put into consolidated
		select c.time, c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.idx, c.skey, c.alarmstate, c.uuid 
		from alarms C left join temp_alarm_table T on C.uuid = T.n_uuid 
		where T.alarmstate = T.n_state and T.alarmstate = 0 
		and coalesce(C.skey_1,'') <> 'out'
		order by machine, idx, time;

	--	select count(*) from temp_consolidated_out --12416203
 
--	select skey_1, count(skey_1) from alarms group by skey_1 where skey_1 like 'out'	--12479729 versus 12673028
	
--update alarms set skey_1 = 'out' where uuid in (select uuid from temp_consolidated_out )

	raise notice 'duration % : update alarms skey_1 to out ',  clock_timestamp() - t;
	update alarms A set skey_1 = 'out' from temp_consolidated_out T where A.uuid=T.uuid;
--update alarms A set skey_1 = NULL


	--select * from alarms order by machine, idx, time desc limit 1000

	-- take the uuid from the end of the previous block, create a block_uuid as the uuid for the next starting block
	-- this means that the FIRST alarm block will not be included !! even if it is properly finished...
	-- this means that the LAST running (not ended) alarm block will not be included !!
	
	raise notice 'duration % : create temp table temp_endlines', clock_timestamp() - t;
	create temp table temp_endlines as  --
		select 
		time,machine, idx, uuid , lead(ac.UUID,1) over (partition by ac.machineserial, ac.idx order by ac.time) as block_uuid
		from temp_consolidated ac 
		where 	ac.alarmstate = 0
		order by ac.machine,ac.idx, ac.time asc;
	--select * from temp_endlines order by time, machine, idx
	
	-- now... only work and insert the new alarms !!! (not yet materialized in alarm_lines...)
	-- this endlines table should be used to join with the materialized summary ? From this point forward, only new alarms
	
	-- check if table exists !!
	if (select public.pattyn_table_exists('alarm_lines') is not null) then
		raise notice 'duration % : delete from temp_endlines', clock_timestamp() - t;
		delete from temp_endlines T
		using alarm_lines L
		where T.block_uuid::uuid = L.block_uuid::uuid;
	end if ;
	
	raise notice 'duration % :create temp table temp_unique_blocks', clock_timestamp() - t;
	create temp table temp_unique_blocks as  -- use end of previous alarm to get a 'block' with the alarmstates (and take some next -3- lines)
		( select blocks.*,  block_uuid from temp_endlines EL
			left join lateral 
			(select *, row_number() over (partition by machine,idx, alarmstate order by time) blockrows  from temp_consolidated c1
	 		where uuid <> EL.uuid and c1.time >= EL.time
			and machine = EL.machine
	 		and idx = EL.idx 
	 		order by machine, idx, time
		    offset 0 rows fetch first 3 rows only) as blocks 
		    on blocks.idx = EL.idx and blocks.machine = EL.machine
		    );
	--select * from temp_unique_blocks 

	----- finally, keep only alarm-blocks with one unique id per block...
	delete from temp_unique_blocks where blockrows <> 1 or blockrows is null;
	
	--- transpose the different alarmstate lines into columns for reduction
	raise notice 'duration % :create temp table temp_columntranspose', clock_timestamp() - t;
	create temp table temp_columntranspose as
		(
		select  block_uuid, idx, 
		time as alarm_start,
		(case when (alarmstate= 1) or (alarmstate=10) then lead(time,1) over (partition by machineserial, idx order by time) else null end) as end_active_alarm,
		(case alarmstate when 20 then lead(time,1) over (partition by machineserial, idx order by time) else null end) as end_waiting_ackn,
		(case alarmstate when 0 then time else null end) as end_alarm
		from temp_unique_blocks
		order by time, machine, idx
		);
	--select * from temp_columntranspose
	
		-- reduce alarmblocks to a single line
	raise notice 'duration % : create temp table temp_single_lines', clock_timestamp() - t;
	create temp table temp_single_lines as (
		select block_uuid, idx, min(alarm_start) as alarm_start_0, 
		max(end_active_alarm) as end_active_alarm_10, 
		max(end_waiting_ackn) as end_waiting_ackn_20, 
		max(end_alarm) alarm_end
		from temp_columntranspose
		group by block_uuid, idx
		order by min(alarm_start)
		);
			
	--select * from temp_single_lines
	
	-- this table does not contain the first alarm from the alarm-table !!
	-- this table does (not) ? containt the last unfinished alarm == test !!


	raise notice 'duration % : INSERT INTO alarm_lines', clock_timestamp() - t;
	INSERT INTO alarm_lines ("time", machine, machineserial, recipe, recipeloadcounter, idx, skey, max_alarmstate, active_end, waiting_end, alarm_end, duration_active_sec, duration_waiting_sec, duration_alarm_sec, block_uuid) 
	select 
		min(T.alarm_start_0) as "time" , -- alarm_start
		min(B.machine) machine,
		min(B.machineserial) machineserial,
		min(B.recipe) recipe,
		min(B.recipeloadcounter) recipeloadcounter,
		min(B.idx) idx,
		min(B.skey) skey,
		max(B.alarmstate) as max_alarmstate,
		min(T.end_active_alarm_10) active_end,
		min(T.end_waiting_ackn_20) waiting_end,
		min(T.alarm_end) alarm_end,
		date_part('epoch',(min(T.end_active_alarm_10) - min(T.alarm_start_0))) as duration_active_sec,
		date_part('epoch',(min(T.end_waiting_ackn_20) - min(T.end_active_alarm_10))) as duration_waiting_sec,
		date_part('epoch',(min(T.alarm_end) - min(T.alarm_start_0))) as duration_alarm_sec,
		T.block_uuid
	from temp_single_lines T join temp_unique_blocks B on T.block_uuid = B.block_uuid
	group by T.block_uuid, T.idx
	order by min(B.machine), min(T.alarm_start_0);

	raise notice 'duration % :whoohoo.. reached the last line of the block ! ', clock_timestamp() - t;
	return true;

	raise notice 'oops.. we got outside the block ! ';
end;
$function$
;


select * from pattyntestplcts.pattyn_alarm_lines_speed_test()

select  public.pattyn_alarm_lines_aggregation('pattynnovation')

select * from pattyn_consolidate_alarms() --
select count(*) from pattynnovation.alarms_consolidated --145333
select * from alarms_consolidated ac order by time desc limit 10


select * from timescaledb_information.jobs j 
--SELECT add_retention_policy('alarms', INTERVAL '30 days');
select * from weightdata order by time desc limit 10 


select skey_1, count(*) from alarms group by skey_1
select * from alarm_lines order by time desc limit 10 --2022-01-19 08:21:34.478 +0100
select count(*) from alarms --19.124.662  --> 
select count(*) from alarm_lines --40.018


select * from dba_database_details limit 100 

--20220124

-- raw alarm selection
explain
SELECT time, line, machine, machineserial, 'recipe_15kg' as recipe, recipeloadcounter ,skey, alarmstate 
--, * 
FROM alarms
where idx = '1' and recipeloadcounter = '362'
and time >= '2021-09-21 07:25:23.416 +0200' and time <= '2021-09-21 07:36:23.416 +0200'
ORDER BY time ASC LIMIT 1000 

-- alarm lines consolidation
SELECT --time, line, machine, machineserial, 'recipe_15kg' as recipe, recipeloadcounter ,skey, alarmstate 
time, '1' as line,machine, machineserial,  'recipe_15kg' as recipe, recipeloadcounter, skey,  max_alarmstate , active_end , waiting_end ,alarm_end ,duration_alarm_sec 
--* 
FROM alarm_lines
where idx = '1' and recipeloadcounter = '362'
and time >= '2021-09-21 07:25:23.416 +0200' and time <= '2021-09-21 07:36:23.416 +0200'
ORDER BY time ASC LIMIT 1000 

-- weight data raw
select time, recipe, recipeloadcounter, recipientweight_fsetpoint ,recipientweight_fnetweight
from weightdata w 
where recipeloadcounter = '929'
order by time asc 
limit 10

select * from weightdata_hourly_uuid whu  limit 100

select count(*) from alarms


-- one sample
select time, recipe, recipeloadcounter, recipientweight_fsetpoint ,recipientweight_fnetweight, *
from weightdata w 
where machine like 'AVL 14'
--where recipeloadcounter = '543'
order by time desc , 
limit 1000;

select distinct recipe, recipientweight_fsetpoint , count(recipientweight_fsetpoint) from weightdata w
--where recipe = 'Idx 1 Target 25.00'
group by recipe, recipientweight_fsetpoint
order by recipe


-- all samples aggregated
select recipeloadcounter,  RECIPE, recipientweight_fsetpoint, count(recipientweight_fsetpoint)
from weightdata w
group by recipeloadcounter, recipe, recipientweight_fsetpoint
order by recipeloadcounter desc;

-- the starting query
select recipeloadcounter, recipe, count(distinct(recipientweight_fsetpoint)) as different_recipes
from weightdata w
group by recipeloadcounter, recipe
order by recipeloadcounter desc;

-- resulting overview of mistakes
with overview as (
select recipeloadcounter, recipe, count(distinct(recipientweight_fsetpoint)) as different_recipes
from weightdata w
group by recipeloadcounter, recipe
order by recipeloadcounter asc )
select * from overview where different_recipes > 1 order by recipeloadcounter desc;



select * from alarms order by time desc limit 1000
select distinct stmachine_emachineprogramstate  from general 
order by time desc limit 100


select * from weightdata w order by time desc limit 100
select * from pattyntestplcts.argocheck_criterions_pbd limit 100


select bucket,machine, machineserial,recipe, recipeloadcounter,  
setpoint,
recipientweight_imoduleindex,
recipientweight_eweightresult,
nrofweightresult,
maxoverweight,
maxunderweight,
nrofboxes,
totalweight,
minboxweight,
maxboxweight,
avgboxweight,
stdevboxweight,avgtareweight,
fillingfirsttime,
fillinglasttime,
bucketfirsttime,
bucketlasttime,
avgtareweight,
skey
from pattyntestplcts.weightdata_hourly
where --recipe like 'J10' and 
recipeloadcounter = '877' and bucket = '2022-01-11 04:00:00.000 +0100'
--limit (15)
order by machine, machineserial , recipe, recipeloadcounter, setpoint ,recipientweight_imoduleindex,recipientweight_eweightresult,bucket



select * from pattyntestplcts.weightdata 
--where recipe like 'J10' 
where recipeloadcounter = '877'
--and recipientweight_eweightresult ='20'
and time > '2022-01-11 04:00:00.000 +0100' and time < '2022-01-11 05:00:00.000 +0100'
limit (100)


select * from weightdata w order by time desc limit 100


select * from general w order by time desc limit 100

select * from alarms w order by time desc limit 100

select "time",
line,
machine,
machineserial,
recipe,
recipeloadcounter,
idx,
skey,
alarmstate,
uuid,
loggeridentifier,
loggeractivationuid from alarms
--where skey = '_29_1_39_1'
order by time desc limit 1000
 

select * from pattyntestplcts.alarm_lines
where recipe = 'J01'
order by TIME desc limit 100

select * from general w order by time desc limit 100
select distinct stmachine_emachinepowerstate from "general" g
select distinct stmachine_emachineprogramstate from "general" g
select distinct stmachine_emodestate from "general" g

select time, boxmade, stinfo_ntotalcases, * from general w 
order by time desc limit 100







