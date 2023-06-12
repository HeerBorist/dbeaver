-- on top of this function to : 
 -- function schema_loop_alarms ('alarms') ? 


--drop function public.pattyn_alarm_lines_aggregation(text)
CREATE OR REPLACE FUNCTION public.pattyn_alarm_lines_aggregation ( schemaname TEXT DEFAULT NULL::TEXT )

RETURNS boolean  -- gelukt of niet gelukt ? 
 LANGUAGE plpgsql
AS $function$

<<startblock>>
declare 
	_S	ALIAS for schemaname;
 	_safeschema boolean;
	_T varchar := 'alarms';
	_tableexists boolean;
	_dummy text;
begin 	
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

	
	drop table if exists temp_alarm_table;
	drop table if exists temp_consolidated;
	drop table if exists temp_endlines;
	drop table if exists temp_unique_blocks;
	drop table if exists temp_columntranspose;
	drop table if exists temp_single_lines;

	raise notice 'create temp table temp_alarm_table';
	create temp table temp_alarm_table as   -- fetch all alarms and the state of the next alarm 
		select 	A.alarmstate,a.uuid,
		lead(A.UUID,1) over (partition by A.machineserial, A.idx order by A.time) as n_uuid,
		lead(A.alarmstate,1) over (partition by A.machineserial, A.idx order by A.time) as n_state
		from alarms as A;
	--select count(*) from temp_alarm_table

	raise notice 'create temp table temp_consolidated';
	create temp table temp_consolidated as -- find the alarms where the next state is different from the current state (the transitions...) --put into consolidated
		select c.time, c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.idx, c.skey, c.alarmstate, c.uuid 
		from alarms C inner join temp_alarm_table T on C.uuid = T.n_uuid 
		where T.alarmstate <> T.n_state 
		order by machine, idx, time;
	-- select count(*) from temp_consolidated

	-- take the uuid from the end of the previous block, create a block_uuid as the uuid for the next starting block
	-- this means that the FIRST alarm block will not be included !! even if it is properly finished...
	-- this means that the LAST running (not ended) alarm block will not be included !!
	
	raise notice 'create temp table temp_endlines';
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
		raise notice 'delete from temp_endlines';
		delete from temp_endlines T
		using alarm_lines L
		where T.block_uuid::uuid = L.block_uuid::uuid;
	end if ;
	
	raise notice 'create temp table temp_unique_blocks';
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
	raise notice 'create temp table temp_columntranspose';
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
	raise notice 'create temp table temp_single_lines';
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
	-- this table does (not) contain the last unfinished (or active) alarm. !!

	raise notice 'check if alarm_lines exists';
	if (select public.pattyn_table_exists('alarm_lines') is null)
	then 
	raise notice 'CREATE TABLE if not exists alarm_lines';	
	CREATE TABLE if not exists alarm_lines (
		"time" timestamptz NULL,
		machine text NULL,
		machineserial text NULL,
		recipe text NULL,
		recipeloadcounter text NULL,
		idx text NULL,
		skey text NULL,
		max_alarmstate float8 NULL,
		active_end timestamptz NULL,
		waiting_end timestamptz NULL,
		alarm_end timestamptz NULL,
		duration_active_sec float8 NULL,
		duration_waiting_sec float8 NULL,
		duration_alarm_sec float8 NULL,
		block_uuid bpchar not NULL
	);

	ALTER TABLE alarm_lines OWNER TO "PattynAdmin";
	GRANT ALL ON TABLE alarm_lines TO "PattynAdmin";
	GRANT select ON TABLE alarm_lines TO Public; --could we find the legitimate schema user (Grafana user) ??
	
	perform (select create_hypertable('alarm_lines', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => true, migrate_data => TRUE));
	-- GRANT EXECUTE ON AGGREGATE 'public'.'first'(anyelement,any) TO "PattynAdmin";  --werk niet
	end if ;

	raise notice 'INSERT INTO alarm_lines';
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

	raise notice 'whoohoo.. reached the last line of the block ! ';
	return true;

	raise notice 'oops.. we got outside the block ! ';
end;
$function$
;

/*
select  public.pattyn_alarm_lines_aggregation('testschema1')

select * from alarm_lines limit 100


show search_path;
set search_path to pattyntestplcts, public;

select count(*) from testschema1.alarms; --2835574
select count(*) from pattyntestplcts.alarm_lines; --5942

select * from testschema1.alarm_lines order by time desc
select distinct machine, recipe from testschema1.alarm_lines
drop table testschema1.alarm_lines

select public.pattyn_table_exists('alarm_lines') is null
select public.pattyn_table_exists('alarm_lines1') is null

**/

	