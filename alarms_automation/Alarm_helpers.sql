ALTER TABLE pattyntestplcts.alarms ADD uuid uuid NOT NULL DEFAULT gen_random_uuid();



select idx, skey, alarmstate, time , recipeloadcounter from pattyntestplcts.alarms5
where idx = '2' 
--where alarmstate <> 0
--group by idx, skey, alarmstate
order by idx, time, skey, alarmstate 
limit 100

select a.uuid, A.idx, A.skey, B.alarmstate, A.time , A.recipeloadcounter, 
lead(A.UUID,1) over (partition by A.idx order by A.time) as thenext
from pattyntestplcts.alarms5 A inner join pattyntestplcts.alarms5 B on a.uuid = b.uuid
where A.IDX = '1' --and A.alarmstate = 20 
order by idx, time, skey, alarmstate 
limit 100

select * from pattyntestplcts.alarms5 limit 1000

select a.uuid, A.idx, A.skey, B.alarmstate, A.time , A.recipeloadcounter
from pattyntestplcts.alarms5 A

with t(uuid, time) as (select uuid, time from pattyntestplcts.alarms5)
select * from t

select a.uuid, A.idx, A.skey, A.alarmstate, A.time , A.recipeloadcounter, 
lead(A.UUID,1) over (partition by A.idx order by A.time) as thenext, volgende
from pattyntestplcts.alarms5 A cross join lateral 
	(SELECT uuid, idx, skey, alarmstate, time from pattyntestplcts.alarms5 B where B.uuid = thenext ) as volgende
order by A.idx, A.time


-- alle blokken van 10-20-0
with n as(
	select a.uuid, A.idx, A.skey, A.alarmstate, A.time , A.recipeloadcounter,
	lead(A.UUID,1) over (partition by A.machineserial,A.idx order by A.time) as n_uuid,
	lead(A.alarmstate,1) over (partition by A.machineserial,A.idx order by A.time) as n_state
	from pattyntestplcts.alarms as A 
	order by "time")
select C.* from pattyntestplcts.alarms C inner join n on C.uuid = n.n_uuid
where n.alarmstate <> n_state --and n.n_uuid  c.uuid
order by C.idx, C.time


-- alle blokken van 10-20-0, met tussenliggende 10-20 waarden !!
with n as(
	select a.uuid, A.idx, A.skey, A.alarmstate, A.time , A.recipeloadcounter,
	lead(A.UUID,1) over (partition by A.machineserial,A.idx order by A.time) as n_uuid,
	lead(A.alarmstate,1) over (partition by A.machineserial,A.idx order by A.time) as n_state
	from pattyntestplcts.alarms as A 
	order by "time"),
x as (
	select * from pattyntestplcts.alarms B
	where B.alarmstate <> 0
	
	)
select C.* from pattyntestplcts.alarms C inner join n on C.uuid = n.n_uuid
where n.alarmstate <> n_state 
union 
select * from x 
order by idx, time


CREATE OR REPLACE FUNCTION pattynnovation.pattyn_consolidate_alarms()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
	begin 
		

-- alle blokken van 10-20-0, met tussenliggende 10-20 waarden !! Kunnen we hier een 'edge' kolom aan toevoegen ? 
with n as(
	select a.uuid, A.idx, A.skey, A.alarmstate, A.time , A.recipeloadcounter,
	lead(A.UUID,1) over (partition by A.machineserial, A.idx order by A.time) as n_uuid,
	lead(A.alarmstate,1) over (partition by A.machineserial, A.idx order by A.time) as n_state
	from pattynnovation.alarms as A 
	order by "time"
	),
x as (
	select --B.* 
	B."time",B.line, B.machine, B.machineserial, B.recipe, B.recipeloadcounter, B.idx,B.loggeractivationuid, B.loggeridentifier, B.skey, B.alarmstate
	, B.uuid
	from pattynnovation.alarms B left join n on  B.uuid = n.n_uuid
	where B.alarmstate <> 0 and n.alarmstate = n.n_state
	),
consolidated as 
	(select --C.* , 
	c."time", 	c.line, c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.idx,c.loggeractivationuid, c.loggeridentifier, c.skey, c.alarmstate
	, c.uuid
	,true as edge 
	from pattynnovation.alarms C inner join n on C.uuid = n.n_uuid
	where n.alarmstate <> n_state 
	union 
	select x.* , false as edge 
	from x 
	order by idx, time)
--select * from consolidated A left join alarms_consolidated B on A.uuid = B.uuid where B.uuid is null
INSERT INTO pattynnovation.alarms_consolidated ("time", line,machine, machineserial, recipe, recipeloadcounter, idx, loggeractivationuid, loggeridentifier, skey, alarmstate, uuid, edge) 
select A.* from consolidated A left join pattynnovation.alarms_consolidated B on A.uuid = B.uuid where B.uuid is null;

	return;
	end;
$function$
;

select * from alarms_consolidated
order by line, machine, idx, time


CREATE TABLE if not exists  alarms_consolidated (
	"time" timestamptz NULL,
	line varchar(255) NULL,
	machine varchar(255) NULL,
	machineserial varchar(255) NULL,
	recipe varchar(255) NULL,
	recipeloadcounter varchar(255) NULL,
	idx varchar(255) NULL,
	loggeractivationuid varchar(255) NULL,
	loggeridentifier varchar(255) NULL,
	skey varchar(255) NULL,
	alarmstate float8 NULL,
	uuid uuid NULL,
	edge bool NULL
);


-- Permissions
ALTER TABLE alarms_consolidated OWNER TO "PattynAdmin";
GRANT ALL ON TABLE alarms_consolidated TO PattynAdmin;
GRANT SELECT ON TABLE alarms_consolidated TO Public;

perform (select create_hypertable('alarms_consolidated', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => true, migrate_data => TRUE));
--(427,pattyntestplcts,alarms_consolidated,t)

--https://docs.timescale.com/api/latest/actions/add_job/#optional-arguments

-- Add a custom action	
/*	
	create or replace procedure public.job_alarm_consolidate (job_id int, config jsonb)
	language plpgsql
	as $$
		begin 
			perform (select pattynnovation.pattyn_consolidate_alarms());
		end
	$$;
*/
		
-- Add a job
	--	SELECT add_job('job_alarm_consolidate', '5 min', config => '{}'); --1298
--2119

select delete_job (1001)


select * from public.job_alarm_consolidate()

select * from timescaledb_information.job_stats where job_id = 1001

select * from timescaledb_information.jobs J right join
		timescaledb_information.job_stats JS on J.job_id = JS.job_id --where JS.job_id = 2119
		order by J.application_name , J.job_id

show log_destination


-- run the function
--select pattyn_consolidate_alarms()
select * from pattyntestplcts.alarms_consolidated ac --763
select count (*) from pattyntestplcts.alarms_consolidated ac  --780
 	
select count(*) from pattynnovation.alarms a --#=65674
where skey is null -- #=58156
order by line, idx, time desc

