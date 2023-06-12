CREATE OR REPLACE FUNCTION pattynnovation.pattyn_consolidate_alarms()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
		declare 
		testcount integer;
begin 

raise notice 'Start the pattyn_consolidate_alarms_function at %', now();
-- alle blokken van 10-20-0, met tussenliggende 10-20 waarden !! Kunnen we hier een 'edge' kolom aan toevoegen ? 

with n as(
	select a.uuid, A.idx, A.skey, A.alarmstate, A.time , A.recipeloadcounter,
	lead(A.UUID,1) over (partition by A.machineserial, A.idx order by A.time) as n_uuid,
	lead(A.alarmstate,1) over (partition by A.machineserial, A.idx order by A.time) as n_state
	from pattynnovation.alarms as A 
	where time > (now() - interval'12h')
	order by "time"
	),
x as (
	select --B.* 
	B."time",B.machine, B.machineserial, B.recipe, B.recipeloadcounter, B.idx,B.loggeractivationuid, B.loggeridentifier, B.skey, B.alarmstate
	, B.uuid
	from pattynnovation.alarms B left join n on  B.uuid = n.n_uuid
	where B.alarmstate <> 0 and n.alarmstate = n.n_state
	),
consolidated as 
	(select --C.* , 
	c."time", 	c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.idx,c.loggeractivationuid, c.loggeridentifier, c.skey, c.alarmstate
	, c.uuid
	,true as edge 
	from pattynnovation.alarms C inner join n on C.uuid = n.n_uuid
	where n.alarmstate <> n_state 
	union 
	select x.* , false as edge 
	from x 
	order by idx, time)
--select * from consolidated A left join alarms_consolidated B on A.uuid = B.uuid where B.uuid is null
INSERT INTO pattynnovation.alarms_consolidated ("time", machine, machineserial, recipe, recipeloadcounter, idx, loggeractivationuid, loggeridentifier, skey, alarmstate, uuid, edge) 
select A.* from consolidated A left join pattynnovation.alarms_consolidated B on A.uuid = B.uuid where B.uuid is null;

	return;
	end;
$function$
;



CREATE OR REPLACE PROCEDURE pattynnovation.job_alarm_consolidate(job_id integer, config jsonb)
 LANGUAGE plpgsql
AS $procedure$
		begin 
			perform (select pattynnovation.pattyn_consolidate_alarms());
		end
	$procedure$
;


--SELECT add_job('pattynnovation.job_alarm_consolidate', '1 min', config => '{}'); --1353, 1357

--call pattynnovation.job_alarm_consolidate(job_id integer, config jsonb)
call pattynnovation.job_alarm_consolidate_pn(1353, '{}')
*/


-- Permissions

ALTER FUNCTION pattynnovation.pattyn_consolidate_alarms() OWNER TO "PattynAdmin";
GRANT ALL ON FUNCTION pattynnovation.pattyn_consolidate_alarms() TO "PattynAdmin";
GRANT ALL ON FUNCTION pattynnovation.pattyn_consolidate_alarms() TO "public";

drop table temptest;
-- run the function
select pattynnovation.pattyn_consolidate_alarms_pn()

-- show the result
select * from pattynnovation.alarms_consolidated order by time desc limit 10

select * from timescaledb_information.job_stats where job_id = 1358

--select delete_job(1353)
select alter_job   (1358, scheduled => false)
select alter_job   (1358, scheduled => true)

SELECT *    FROM _timescaledb_config.bgw_job j
     LEFT JOIN _timescaledb_catalog.hypertable ht ON ht.id = j.hypertable_id
     LEFT JOIN _timescaledb_internal.bgw_job_stat js ON js.job_id = j.id
    where application_name like '%User-Defined%' 
--    and j.id = 1358
   order by j.id ;
