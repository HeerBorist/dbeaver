-- te bekijken : 
--https://dba.stackexchange.com/questions/143044/optimise-a-lateral-join-query-on-a-big-table

-- Doel : alarms table samenvatten tot max 3 lijnen per alarm blok... (of 2 lijnen bij status 0-1)
-- vertrekken vanaf originele alarm tabel ? : direct duration per lijn toevoegen, en unieke code per alarmblok ??
	    
-- alle blokken van 10-20-0, met tussenliggende 10-20 waarden !! Kunnen we hier een 'edge' kolom aan toevoegen ? 
-- en halverwege enkele bewerkingen uitvoeren op alarmen die nog niet in de consolidated tabel zitten ?
-- opletten .... een alarm kan nog actief zijn op moment van uitvoeren, en dus ergens in status 10- 20 (zonder afsluit 0) !!
-- dus, vertrekken van de alarms tabel kan onvolledige resultaten opleveren... en dus overal eigenlijk.
-- Daarom : het is belangrijk dat we de 'volledige' alarmblokken kunnen uitfilteren en daarop berekeningen loslaten !!
-- Wat is een volledige alarm blok ? start bij 10 , opeenvolgende waarden van 10 of 20, eindigt met de eerstvolgende 0
--( waarbij de 0 een universele waaarde is, maar de status 10, 20 ook kan worden vervangen door bvb. enkel 1 )
-- 

-- origineel om alle tussenliggende nul waarden te verwijderen. (optimized)
-- zonder de edge kolom voor vereenvoudiging en snelheid van volgende stappen !!	    

--https://www.cybertec-postgresql.com/en/understanding-lateral-joins-in-postgresql/
-- logica : selecteer het eindpunt van elke blok, neem daarna de 3 volgende records met grotere tijd...en voeg uuid per blok toe

-- alles samen, met poging om als with te herschrijven : 	werkende versie met uniek uuid per block 
--(misschien beter toch met #temp table schrijven in functie ??) ==> maar daar kan je de select into niet gebruiken !!
-- with statement lives in memory.... for large tables this can be problematic, so to better be safe then sorry... use a 'CREATE TEMP TABLE function !
-- a bit slower than RAM, but zero to no chance of running out of memory error...
	

--Stop going back from here !! the above works ....but would like to use temp tables in a function !!

-- *************************************************************************************************************************
-- select all alarms from the alarm table, look at the state of the next alarm...

show search_path;
--set schema 'public, pattyntestplcts';	-- should be restricted to current session ! can be used in a loop from inside a function ?? need testing.

set search_path to pattyntestplcts, public;

show search_path;

--set schema 'testschema1';	-- should be restricted to current session ! can be used in a loop from inside a function ?? need testing.
-- show search_path;
--"$user", pattyntestplcts, public

drop table if exists temp_alarm_table;
drop table if exists temp_consolidated;
drop table if exists temp_endlines;
drop table if exists temp_unique_blocks;
drop table if exists temp_columntranspose;
drop table if exists temp_single_lines;

create temp table temp_alarm_table as   -- fetch all alarms and the state of the next alarm 
	select 	A.alarmstate,a.uuid,
	lead(A.UUID,1) over (partition by A.machineserial, A.idx order by A.time) as n_uuid,
	lead(A.alarmstate,1) over (partition by A.machineserial, A.idx order by A.time) as n_state
	from alarms as A;
--where machine = '1313';
--select count(*) from temp_alarm_table

create temp table temp_consolidated as -- find the alarms where the next state is different from the current state (the transitions...) --put into consolidated
	select c.time, c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.idx, c.skey, c.alarmstate, c.uuid 
	from alarms C inner join temp_alarm_table T on C.uuid = T.n_uuid 
	where T.alarmstate <> T.n_state 
	order by machine, idx, time;

--	union 
--	select c.time, c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.idx, c.skey, c.alarmstate, c.uuid 
--	from alarms C inner join temp_alarm_table T on C.uuid = T.uuid -- use T.uuid to look for backward edges ! 0- 10 -20 instead of 10-20 -0
--	where T.alarmstate <> T.n_state
-- select count(*) from temp_consolidated

-- take the uuid from the end of the previous block, create a block_uuid as the uuid for the next starting block
-- this means that the FIRST alarm block will not be included !! even if it is properly finished...
-- this means that the LAST running (not ended) alarm block will not be included !!

create temp table temp_endlines as  --
	select 
	time,machine, idx, uuid , lead(ac.UUID,1) over (partition by ac.machineserial, ac.idx order by ac.time) as block_uuid
--	,lag(ac.UUID,1) over (partition by ac.machineserial, ac.idx order by ac.time) as prev_block_uuid -- test this too ??
	from temp_consolidated ac 
	where 	ac.alarmstate = 0
	order by ac.machine,ac.idx, ac.time asc;
--select * from temp_endlines order by time, machine, idx

-- now... only work and insert the new alarms !!! (not yet materialized in alarm_lines...)
-- this endlines table should be used to join with the materialized summary ? From this point forward, only new alarms
-- delete from temp_endlines the already existing lines 
-- pseudo : delete from temp_endlines if temp_endlines E.block_uuid == alarm_lines.block_uuid  !!!

-- check if table exists !!
delete from temp_endlines T
using alarm_lines L
where T.block_uuid::uuid = L.block_uuid::uuid;

	
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
-- drop table temp_unique_blocks
	   -- select count(*) from temp_unique_blocks --10829
	   --select * from temp_unique_blocks --10829 --why do we have a null record ??

----- finally, keep only alarm-blocks with one unique id per block...
delete from temp_unique_blocks where blockrows <> 1 or blockrows is null;

--- transpose the different alarmstate lines into columns for reduction
create temp table temp_columntranspose as
	(
	select  block_uuid, idx, 
	alarmstate,-- moet eruit !
	time as alarm_start,
	(case when (alarmstate= 1) or (alarmstate=10) then lead(time,1) over (partition by machineserial, idx order by time) else null end) as end_active_alarm,
	(case alarmstate when 20 then lead(time,1) over (partition by machineserial, idx order by time) else null end) as end_waiting_ackn,
	(case alarmstate when 0 then time else null end) as end_alarm
	from temp_unique_blocks
	order by time, machine, idx
	);
--select * from temp_columntranspose
	
-- reduce alarmblocks to a single line
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

--drop table testschema1.alarm_lines


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
	block_uuid bpchar not NULL,
	start_uuid bpchar NULL,	-- column is niet echt nodig
	end_uuid bpchar null --column is niet echt nodig
);

-- Permissions

--ALTER TABLE alarm_lines OWNER TO "PattynAdmin";
--GRANT ALL ON TABLE alarm_lines TO "PattynAdmin";
--GRANT select ON TABLE alarm_lines TO Public; --could we find the legitimate schema user (Grafana user) ??

--perform (select create_hypertable('testschema1.alarm_lines', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => true, migrate_data => TRUE));
--perform (select create_hypertable('alarm_lines', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => true, migrate_data => TRUE));
--select create_hypertable('alarm_lines', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => true, migrate_data => TRUE); -- ==> werkt zonder schema !


--** TODO **
-- replace "select into" with " insert statement" !! Table needs to exist beforehand !!


-- op de een of andere manier had pattynadmin geen execute rechten op public.first ???
-- dus naar de functie verwijzen als public.*** !
-- zeker omdat we het schema gaan setten....
-- GRANT EXECUTE ON AGGREGATE 'public'.'first'(anyelement,any) TO "PattynAdmin";  --werk niet


INSERT INTO alarm_lines ("time", machine, machineserial, recipe, recipeloadcounter, idx, skey, max_alarmstate, active_end, waiting_end, alarm_end, duration_active_sec, duration_waiting_sec, duration_alarm_sec, block_uuid, start_uuid, end_uuid) 
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
	T.block_uuid,
	public.first(B.uuid, B.time) as start_uuid,
	public.last(B.uuid, B.time) as end_uuid
from temp_single_lines T join temp_unique_blocks B on T.block_uuid = B.block_uuid
--where T.block_uuid is not null
group by T.block_uuid, T.idx
order by min(B.machine), min(T.alarm_start_0);

select count(*) from alarm_lines;  -- 4755
--delete from testschema1.alarm_lines where block_uuid in ('7b1ed57e-24cc-46a5-950f-22008b3cc02d','25a11824-dc42-4c1d-b56f-66f30cce3dca')

--TODO : create fucnction that accepts schema name !!
-- add some checks on existing tables 



--- Create the table for the alarm_lines

-- testschema1.alarm_lines definition

-- Drop table

-- DROP TABLE testschema1.alarm_lines;



/*	
-- create a test set for end inspection !
insert into alarms
select time, '1313' as machine, '2020P999' as machineserial,
recipe, recipeloadcounter, idx, loggeractivationuid , loggeridentifier , skey, alarmstate, 
gen_random_uuid() as uuid, skey_1 as null
from alarms 
where idx = '12' 
and machine = '1100'
and time >= '2021-10-04 04:07:50'
and time < '2021-10-04 07:30:00'
order by time

select * from alarms where machine = '1313' order by time
*/

--select * from alarms order by time desc limit 100
--select * from alarms order by time limit 100
select * from alarm_lines order by time desc limit 20
	