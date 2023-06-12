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
	from alarms as A 
where time > (now() - interval'12h')
	order by "time"
	),
x as (
	select --B.* 
	B."time",B.machine, B.machineserial, B.recipe, B.recipeloadcounter, B.idx,B.loggeractivationuid, B.loggeridentifier, B.skey, B.alarmstate
	, B.uuid
	from alarms B left join n on  B.uuid = n.n_uuid
	where B.alarmstate <> 0 and n.alarmstate = n.n_state
	),
consolidated as 
	(select --C.* , 
	c."time", 	c.machine, c.machineserial, c.recipe, c.recipeloadcounter, c.idx,c.loggeractivationuid, c.loggeridentifier, c.skey, c.alarmstate
	, c.uuid
	,true as edge 
	from alarms C inner join n on C.uuid = n.n_uuid
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

-- Permissions

ALTER FUNCTION pattynnovation.pattyn_consolidate_alarms() OWNER TO "PattynAdmin";
GRANT ALL ON FUNCTION pattynnovation.pattyn_consolidate_alarms() TO "PattynAdmin";
