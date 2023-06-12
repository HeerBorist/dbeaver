
create or replace function public.pattyn_general_emodestate_carry_forward()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
 

declare 
lastcarryforwardtime timestamptz;
startcarryforwardtime timestamptz;
aantal integer;

	begin

-- stmachine_emodestate	: carryforward per line/machine... this status is recipe independent...

-- add a float8 column for mode state : chedk for existance first !!
-- ALTER TABLE pattyntestplcts."general" ADD carry_fw_modestate float8 NULL DEFAULT NULL;
-- speed up searches ??
--CREATE INDEX general_carry_fw_modestate_idx ON pattyntestplcts."general" (carry_fw_modestate);


-- find the first null value in time : (over machineserial) for carry_fw_modestate, this is the carry_fw_modestate startrow for next update
-- this could have been a 'late sync inserted line' !! due to asynchronous syncing, values can be added in the past !!

lastcarryforwardtime := (select time from general where carry_fw_modestate is null order by time asc limit 1) as a ;

raise notice 'lastcarryforwardtime % : ', lastcarryforwardtime ;


-- now find the last record where the emodestate is not NULL, take into account the machineserial !, this will be our first carry-forward value.
-- start form above value and look backward in the table ...

startcarryforwardtime := (
with minim_emode as 
(
	-- find the last record where the emodestate is not NULL, take into account the machineserial !, this will be our first carry-forward value.
	select  machineserial --, recipeloadcounter 
	,max(time) as time 
	from general 
	where stmachine_emodestate is not null
	and time <= lastcarryforwardtime --'2021-06-03 10:44:34.136 +0200'  --$$lastcarryforwardtime  -- the first null emode carry_fw_record
	group by machineserial --, recipeloadcounter  
	order by time desc
	limit 10  -- max 10 machineserials ? 
) 
select min(time) from minim_emode); -- := $$startcarryforwardtime

raise notice 'startcarryforwardtime % : ', startcarryforwardtime ;

-- start forwarding from $$startcarryforwardtime for the emodestate...
-- stmachine_emodestate
create temp table temp_emodestatecarryforward as
select uuid, first_value(stmachine_emodestate) over w as carryforward_modestate --, *
from ( 
	select --* , 
		time, machineserial,uuid, stmachine_emodestate,
		count(stmachine_emodestate) over (partition by machineserial order by time) as value_partition -- count is faster in this case !!
		--		sum(case when stmachine_emachineprogramstate is null then 0 else 1 end) over (partition by recipeloadcounter order by time) as value_partition
		from general 
		where time >= startcarryforwardtime --'2021-06-03 10:44:26.740 +0200'--$$startcarryforwardtime
		) as q
	window w as (partition by machineserial, value_partition order by time)  --80sec for approx 8 months, approx 20 sec for 3 months of data
	order by time asc;

--now use this result to update table general, column carry_fw_modestate !!  (do not update if new carryforward_modestate == null)
--select * from temp_emodestatecarryforward;
raise notice 'ready to update the carryforward_modestate';
/*
aantal := (select count(*) from temp_emodestatecarryforward T inner join general g
on g.uuid = T.uuid 
and T.carryforward_modestate is not null);

raise notice 'preparing for update of % records',aantal;
*/

update general g
set carry_fw_modestate = T.carryforward_modestate
from temp_emodestatecarryforward T
where g.uuid = T.uuid 
and T.carryforward_modestate is not null;

raise notice 'all  carryforward_modestate updated... ';

-- clean up...
drop table if exists temp_emodestatecarryforward;

raise notice 'finished ! ';

return;
	end;
$function$
;


------------------------------------------------------- end emodestate function

--select * from pattyn_general_emodestate_carry_forward()