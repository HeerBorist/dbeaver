-- on top of this function to : 
  

--20220330 -- SSAV
-- tested on general table on testserver

--drop function public.pattyn_general_boxproduction_aggregation(text)

CREATE OR REPLACE FUNCTION public.pattyn_general_boxproduction_aggregation ( schemaname TEXT DEFAULT NULL::TEXT )


RETURNS boolean  -- failed or not failed ? 
 LANGUAGE plpgsql
AS $function$

<<startblock>>
declare 
	_S	ALIAS for schemaname;
 	_safeschema boolean;
	_T varchar := 'general';
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

	--  dynamically set the search path, include public !! 
	--	https://stackoverflow.com/questions/21259551/postgresql-how-do-i-set-the-search-path-from-inside-a-function
	_S := _S || ',public'; --pattyntestplcts,public
    perform set_config('search_path', regexp_replace(_S, '[^\w ,]', '', 'g'), false);
	_tableexists := false;
	_tableexists := (select public.pattyn_table_exists('general') is not null);

	if (not _tableexists) then
		raise notice 'exit pattyn_general_boxproduction_aggregation due to unexisting(?) or unsafe general table in schema %', _S;
		return false;
		exit startblock;
	end if;

-- Purpose : summarize the box production information from the general table (machine status details)
-- Put all results in table general_boxproduction

-- *************************************************************************************************************************

-- first create the hypertable if it does not exist yet...

	raise notice 'start : check if general_boxproduction exists';
	if (select public.pattyn_table_exists('general_boxproduction') is null)
	then 
	raise notice 'CREATE TABLE if not exists general_boxproduction';	
	CREATE TABLE if not exists general_boxproduction (
		time timestamptz NULL,
		line varchar(255) NULL,
		machine varchar(255) NULL,
		machineserial varchar(255) NULL,
		recipe varchar(255) NULL,
		recipeloadcounter varchar(255) NULL,
		batchstart timestamptz NULL,
		batchend timestamptz NULL,
		duration_hours float8 NULL,
		nrofboxes float8 NULL,
		avg_box_per_hour float8 null,
		first_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
		last_uuid uuid NOT NULL DEFAULT gen_random_uuid()
	);

	ALTER TABLE general_boxproduction OWNER TO "PattynAdmin";
	GRANT ALL ON TABLE general_boxproduction TO "PattynAdmin";
	GRANT select ON TABLE general_boxproduction TO Public; --could we find the legitimate schema user (Grafana user) ??
	
	perform (select create_hypertable('general_boxproduction', 'time', chunk_time_interval => INTERVAL '10 day', if_not_exists => true, migrate_data => TRUE));
	-- GRANT EXECUTE ON AGGREGATE 'public'.'first'(anyelement,any) TO "PattynAdmin";  --werk niet
	end if ;

	drop table if exists temp_boxproduction;

	raise notice 'create temp table temp_boxproduction';
	
 	create temp table temp_boxproduction as
	select 
	first(time, time) as time,
	line, machine, machineserial, recipe, recipeloadcounter
	,first(time, time) as batchstart
	,last(time, time) as batchend
	,date_part('epoch',(last(time, time)) - (first(time, time) ))/3600 as duration_hours
	,(max(stinfo_ntotalcases)-min(stinfo_ntotalcases))+1 as NrOfBoxes
	,(max(stinfo_ntotalcases)-min(stinfo_ntotalcases)) / (date_part('epoch',(last(time, time)) - (first(time, time) ))/3600) as avg_box_per_hour
	,first(uuid,time) as first_uuid
	,last(uuid, time) as last_uuid 
	from general 
	where line is not null
	and machineserial is not null 
	and recipe is not null
--	and recipeloadcounter <> 'NoReadOut'
	group by line, machine, machineserial, recipe, recipeloadcounter;
--	order by recipeloadcounter::float desc;

	-- lastuuid is used to find incomplete records from previous run... 
	if (select public.pattyn_table_exists('general_boxproduction') is not null) then
		-- Delete existing temp lines
		raise notice 'delete existing records from temp_boxproduction';
		-- Delete all existing lines from the temp table
		delete from temp_boxproduction where first_uuid not in (
		select tb.first_uuid from temp_boxproduction TB left join general_boxproduction B on tb.first_uuid = b.first_uuid
		where b.first_uuid is null  or tb.last_uuid <> B.last_uuid );

		-- update the last incomplete records
		raise notice 'update the last existing unfinished records from temp_boxproduction';
		update general_boxproduction B set 
		-- (time = T.time, line = T.line, machine = T.machine, machineserial = T.machineserial, recipe=T.recipe, recipeloadcounter=T.recipeloadcounter, batchstart = T.batchstart, first_uuid= T.first_uuid,
		 (batchend, duration_hours, nrofboxes, avg_box_per_hour, last_uuid) = 
		 (T.batchend, T.duration_hours, T.nrofboxes, T.avg_box_per_hour, T.last_uuid)
		 from temp_boxproduction T where t.first_uuid = b.first_uuid and T.last_uuid <> B.last_uuid ;
	
		-- insert the new records
		raise notice 'insert the new records from temp_boxproduction, including the last unfinished ones';
		insert into general_boxproduction ("time",	line ,machine ,	machineserial ,	recipe , recipeloadcounter ,batchstart ,batchend ,	duration_hours,	nrofboxes ,	avg_box_per_hour ,first_uuid ,	last_uuid )
		select TB.* from temp_boxproduction TB left join general_boxproduction B on tb.first_uuid = b.first_uuid
		where b.first_uuid is null  ;

	end if ;
	
	raise notice 'we.. reached the last line of the block ! ';
	return true;

	raise notice 'oops.. we got outside the block ! ';
	return false;
	
end;
$function$
;

/*

select * from general_boxproduction order by time desc limit 100

select  public.pattyn_general_boxproduction_aggregation ('pattyntestplcts')
select  public.pattyn_general_boxproduction_aggregation ('workshopdb')

select public.pattyn_table_exists('general_boxproduction', 'pattyntestplcts')
select public.pattyn_table_exists('general_boxproduction')
select public.pattyn_table_exists('general')
select pattyn_check_string('general')

select pattyn_check_string('general_boxproduction')


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

	