-- a bunch of trials to find the most performing way to do this...
-- find the final solution in Alarm_Helpers_2 script

select count(*) from alarms_consolidated ac 


with prep as (
select time, lead(time,1) over (partition by idx order by machineserial , idx, time) as nexttime,
alarmstate, edge, uuid, skey, idx,  recipeloadcounter  from alarms_consolidated ac
order by idx, time asc
),
prep2 as (
select pattyn_utc2epoch(p.nexttime::timestamp) as nexttime, pattyn_utc2epoch(p.time::timestamp) as ori_time,
pattyn_utc2epoch(p.nexttime::timestamp) - pattyn_utc2epoch(p.time::timestamp) as nanodistance
,p.* from prep p) 
select p2.idx::integer, mode() within group (order by p2.nanodistance) as modal_distance from prep2 p2
--where p2.alarmstate=0
group by p2.idx
order by 1
/*
|idx|modal_distance|
|---|--------------|
|1|710.0|
|2|710.0|
|4|710.0|
|5|710.0|
|12|710.0|
|16|710.0|
|19|710.0|
|24|120000.0|
*/

-- Logic : 
-- normal distance can be derived from the 0 state values
-- in a hybrid logging system, the on-change values will allways fall in between the on follow (hybrid) timeperiod
-- if a parameter is set to log a value every 2 minutes (2*60*1000 = 120.000 nanoseconds), then a lot of 0 values will have a 'distance' of 120.000
-- the on change values will have a distance with previous values of less than 120.000

-- nanodistance varies between 120.001 and 119.999 ... ??

with prep as (
select time, lead(time,1) over (partition by idx order by machineserial , idx, time) as nexttime,
alarmstate,  uuid, skey, idx,  recipeloadcounter  from alarms a
where alarmstate = 0 and skey is not null
order by idx, time asc
),
prep2 as (
select pattyn_utc2epoch(p.nexttime::timestamp) as nexttime, pattyn_utc2epoch(p.time::timestamp) as ori_time,
pattyn_utc2epoch(p.nexttime::timestamp) - pattyn_utc2epoch(p.time::timestamp) as nanodistance
,p.* from prep p) 
select * from prep2
order by idx, time

select * from alarms_consolidated ac 
where uuid='96d8c004-fac4-40ed-9a29-a7a61128e91f' -- 2021-09-20 23:50:45

select time, idx, alarmstate, uuid, edge from alarms_consolidated ac 
where time >='2021-09-20 23:50:45' and idx = '1' and edge = true
order by time asc

--mode() within group(order by alarmstate) AS "Alarm",

-- beetje verder zoeken naar mode : zoek de distance tussen twee 'on period' gelogde punten...
with cte as (
	select time, lead(time,1) over (partition by idx order by machineserial , idx, time) as nexttime,
	pattyn_utc2epoch(time::timestamp)  as rowtime, 
	pattyn_utc2epoch((lead(time,1) over (partition by idx order by machineserial , idx, time))::timestamp) as nextrowtime,
	pattyn_utc2epoch((lead(time,1) over (partition by idx order by machineserial , idx, time))::timestamp) -pattyn_utc2epoch(time::timestamp) as distance_to_next,
	machineserial, alarmstate  --,uuid, skey, idx,  recipeloadcounter  
	from alarms a
	where skey is not null and alarmstate = 0
	order by machineserial, idx, time asc)
select machineserial, mode() within group(order by distance_to_next) as on_period_value from cte
group by machineserial 
order by machineserial

-- gaps and islands problem : zoek de startijd van elke nieuwe blok ?? 
--https://stackoverflow.com/questions/55654156/group-consecutive-rows-based-on-one-column

select *, 
row_number() over (order by machineserial, idx, time) r1,
row_number() over (partition by machineserial, idx, alarmstate order by time) r2,
row_number() over (order by machineserial, idx, time) - row_number() over (partition by machineserial, idx, alarmstate order by time) grp
from alarms order by machineserial, idx, time


select machineserial,idx,min(time), alarmstate, grp
from (select machineserial, idx, time, alarmstate ,--*, 
row_number() over (order by machineserial, idx, time) - row_number() over (partition by machineserial, idx, alarmstate order by time) grp
from alarms) t1
group by machineserial, idx, grp, alarmstate
order by machineserial, idx, min(time)


--***********************

-- zoek blok van 3 logisch aansluitende rijen, en voorzie 1 gemeenschappelijke guid
with R as (select gen_random_uuid() as uniek)
select uniek, time, idx, alarmstate, uuid 
from alarms_consolidated ac join R on true
where time >='2021-09-20 23:50:45' and idx = '1'  and edge = true
order by time asc, idx offset 1 rows fetch first 3 rows only


-- selecteer het eindpunt van elke blok van 3 edges...de volgende blok van 3 (of 2) hoort samen als 1 alarm...
select time, idx, alarmstate, edge, uuid from alarms_consolidated ac
where time >='2021-09-20 23:50:45' and idx = '1'  and edge = true and alarmstate = 0
order by time asc limit 100--, idx offset 1 rows fetch first 10 rows only

--dan via lateral join een fetch 3 where not in guid ? kan dit lukken ? 

/*
|time               |idx|alarmstate|uuid                                |
|-------------------|---|----------|------------------------------------|
|2021-09-20 23:50:45|1  |0.0       |96d8c004-fac4-40ed-9a29-a7a61128e91f|
|2021-09-21 00:08:16|1  |0.0       |579c73a7-9dc7-4f55-bb41-e24772d6e018|
|2021-09-21 00:35:15|1  |0.0       |b90a1fc5-12cd-4e53-ab50-3834c23924e5|
|2021-09-21 02:37:07|1  |0.0       |873aaf1d-40d1-421d-93e0-4760b2a70c42|
|2021-09-21 05:08:25|1  |0.0       |0ae28bfc-3ed6-4ce9-821e-040ae5a36483|
|2021-09-21 05:55:35|1  |0.0       |2f57477d-f971-42df-af5e-95673c46ba11|
|2021-09-21 06:24:36|1  |0.0       |f9b80852-5397-4b4f-911f-9ffc8edc0970|
|2021-09-21 07:34:44|1  |0.0       |8fbd196b-45d2-45dc-8251-893a7beaafe2|
|2021-09-21 09:08:02|1  |0.0       |99cc86f7-00e7-401d-8f49-a355da78632e|
|2021-09-21 10:56:15|1  |0.0       |6c9bef68-45e1-4d7e-a1d0-cfc5506fd878|
*/

--https://www.cybertec-postgresql.com/en/understanding-lateral-joins-in-postgresql/

-- onderstaande werkt, maar heeft afhankelijk van de sequentie 10-20-0 of 10-0 een factor 3 of 2 nodig... dus niet volledig generiek
-- logica : selecteer het eindpunt van elke blok, neem daarna de 3 volgende records met grotere tijd...
explain analyse
select o2.time, o2.idx, o2.alarmstate, o2.uuid, o2.edge ,uniek from	
(	select ac.time, ac.idx, ac.alarmstate, ac.uuid , gen_random_uuid() as uniek
	from alarms_consolidated ac
	where ac.time >='2021-09-20 23:50:45' 
	and ac.idx = '1'  and ac.edge = true and ac.alarmstate = 0
	order by ac.idx, ac.time asc limit 10) o1 
left join lateral --Zoek vanaf het eindpunt van de vorige record, neem max 3 waarden...==> 0,10,20 of 0,10,0 !!
	(select time, idx, alarmstate, uuid , edge  from alarms_consolidated 
 		where uuid <> o1.uuid and time >= o1.time
		and idx = '1'  and edge = true 
 		order by idx, time
	    offset 0 rows fetch first 3 rows only) o2 on true
	    
--Nested Loop Left Join  (cost=167.90..935.11 rows=30 width=51) (actual time=1.279..5.579 rows=30 loops=1)

/*
-- beetje anders : andere offset !!
-- zoek op einde van vorige blok, neem volgende record + next xx records (dus volledige blok...)	 
explain analyse   
select o2.time, o2.idx, o2.alarmstate, o2.uuid, o2.edge ,uniek from	
(	select ac.time, ac.idx, ac.alarmstate, ac.uuid , gen_random_uuid() as uniek
	from alarms_consolidated ac
	where ac.time >='2021-09-20 23:50:45' 
	and ac.idx = '1'  and ac.edge = true and ac.alarmstate = 0
	order by ac.idx, ac.time asc limit 10) o1 
left join lateral --Zoek vanaf het eindpunt van de vorige record, neem max 3 waarden...==> 0,10,20 of 0,10,0 !!
	(select time, idx, alarmstate, uuid , edge  from alarms_consolidated 
 		where uuid = o1.uuid or time >= o1.time --==> resulteert in dubbel zo dure query !!
		and idx = '1'  and edge = true 
 		order by idx, time
	    offset 1 rows fetch first 3 rows only) o2 on true
	    	
--Nested Loop Left Join  (cost=405.00..3277.75 rows=30 width=51) (actual time=3.483..23.155 rows=30 loops=1)    

 */		
 		
/*
|time               |idx|alarmstate|uuid                                |edge|uniek                               |
|-------------------|---|----------|------------------------------------|----|------------------------------------|
|2021-09-21 00:04:34|1  |10.0      |d4e109e6-63b7-413b-b9d8-c1ee99464678|true|d6dbad3d-ec74-4233-a8b1-bad27002f4f0|
|2021-09-21 00:08:15|1  |20.0      |b1ce538f-5ae2-4059-a294-0180640acdd3|true|d6dbad3d-ec74-4233-a8b1-bad27002f4f0|
|2021-09-21 00:08:16|1  |0.0       |579c73a7-9dc7-4f55-bb41-e24772d6e018|true|d6dbad3d-ec74-4233-a8b1-bad27002f4f0|
|2021-09-21 00:32:52|1  |10.0      |5696288a-557e-44aa-bd71-f1b083cd51f1|true|fe38829d-32ff-43a7-a3fc-c658c02d58c0|
|2021-09-21 00:35:14|1  |20.0      |a342773d-d2c5-4797-85f2-8a8f8dcf6cf1|true|fe38829d-32ff-43a7-a3fc-c658c02d58c0|
|2021-09-21 00:35:15|1  |0.0       |b90a1fc5-12cd-4e53-ab50-3834c23924e5|true|fe38829d-32ff-43a7-a3fc-c658c02d58c0|
|2021-09-21 02:36:26|1  |10.0      |4d59ba5a-15ab-4b1f-b630-e955fee5ac60|true|a42ef948-96fa-4571-a25c-e282774ee148|
|2021-09-21 02:37:07|1  |20.0      |3192c0f4-4cf5-491e-a8ba-04e3190f4708|true|a42ef948-96fa-4571-a25c-e282774ee148|
|2021-09-21 02:37:07|1  |0.0       |873aaf1d-40d1-421d-93e0-4760b2a70c42|true|a42ef948-96fa-4571-a25c-e282774ee148|
...
*/

-- vertrekken vanaf originele alarm tabel ? : direct duration per lijn toevoegen, en unieke code per alarmblok ??
	    
-- alle blokken van 10-20-0, met tussenliggende 10-20 waarden !! Kunnen we hier een 'edge' kolom aan toevoegen ? 
-- en halverwege enkele bewerkingen uitvoeren op alarmen die nog niet in de consolidated tabel zitten ?
-- opletten .... een alarm kan nog actief zijn op moment van uitvoeren, en dus ergens in status 10- 20 (zonder afsluit 0) !!
-- dus, vertrekken van de alarms tabel kan onvolledige resultaten opleveren... en dus overal eigenlijk.
-- Daarom : het is belangrijk dat we de 'volledige' alarmblokken kunnen uitfilteren en daarop berekeningen loslaten !!
-- Wat is een volledige alarm blok ? start bij 10 , opeenvolgende waarden van 10 of 20, eindigt met de eerstvolgende 0
--( waarbij de 0 een universele waaarde is, maar de status 10, 20 ook kan worden vervangen door bvb. enkel 1 )
-- 

-- origineel om alle tussenliggende nul waarden te verwijderen.
-- kan ook zonder de edge kolom voor vereenvoudiging en snelheid van volgende stappen !!	    
	    
with n as(
	select a.uuid, A.idx, A.skey, A.alarmstate, A.time , A.recipeloadcounter,
	lead(A.UUID,1) over (partition by A.machineserial, A.idx order by A.time) as n_uuid,
	lead(A.alarmstate,1) over (partition by A.machineserial, A.idx order by A.time) as n_state
	from pattyntestplcts.alarms as A 
	order by "time"),
x as (
	select B.* from pattyntestplcts.alarms B left join n on  B.uuid = n.n_uuid
	where B.alarmstate <> 0 and n.alarmstate = n.n_state
	),
consolidated as 
	(select C.* , true as edge 
	from pattyntestplcts.alarms C inner join n on C.uuid = n.n_uuid
	where n.alarmstate <> n_state 
	union 
	select x.* , false as edge 
	from x 
	order by idx, time)
select * from consolidated order by machineserial , idx, time 
--select count(*) from consolidated -- 7402 
--select A.* from consolidated A left join pattyntestplcts.alarms_consolidated B on A.uuid = B.uuid where B.uuid is null;	    
	    


--https://www.cybertec-postgresql.com/en/understanding-lateral-joins-in-postgresql/
-- logica : selecteer het eindpunt van elke blok, neem daarna de 3 volgende records met grotere tijd...en voeg uuid per blok toe


explain analyse 
select o2.*, uniek
--o2.time, o2.machineserial, o2.machine, o2.recipe, o2.recipeloadcounter, o2.skey, o2.idx, o2.alarmstate, o2.uuid, o2.edge ,uniek, o2.r2
from	
(	select --ac.time, ac.idx, ac.alarmstate, ac.uuid , gen_random_uuid() as uniek
	*,  gen_random_uuid() as uniek
	from alarms_consolidated ac
	where 
--	ac.time >='2021-09-20 23:50:45' and
	--ac.idx = '4'  and 
	ac.edge = true and ac.alarmstate = 0
	order by ac.machine,ac.idx, ac.time asc --limit 1000
	) as o1 
 left join lateral --Zoek vanaf het eindpunt van de vorige record, neem max 3 waarden...==> 0,10,20 of 0,10,0 !!
	(select *, row_number() over (partition by machine,idx, alarmstate order by time) r2  from alarms_consolidated
	--time, idx, alarmstate, uuid , edge, row_number() over (partition by idx, alarmstate order by time) r2  from alarms_consolidated 
 		where uuid <> o1.uuid and time >= o1.time
		and machine = o1.machine
 		and idx = o1.idx --'4'  
		and edge = true 
 		order by machine, idx, time
	    offset 0 rows fetch first 3 rows only) as o2 on o2.idx = o1.idx--true
	where o2.r2 = 1
	order by time, machine, idx --idx , time--, idx--, alarmstate



-- of het gehele probleem met 1 functie uitwerken : zou wel eens overzichtelijker en sneller kunnen zijn !!
--https://stackoverflow.com/questions/14010348/group-by-repeating-attribute/14016575#14016575

/* -- originele functie 
CREATE OR REPLACE FUNCTION f_alarm_groups()
  RETURNS TABLE (ids int[])
  LANGUAGE plpgsql AS
$func$
DECLARE
   _id    int;
   _uid   int;
   _id0   int;                         -- id of last row
   _uid0  int;                         -- user_id of last row
BEGIN
   FOR _id, _uid IN
       SELECT id, user_id FROM messages ORDER BY id
   LOOP
       IF _uid <> _uid0 THEN
          RETURN QUERY VALUES (ids);   -- output row (never happens after 1 row)
          ids := ARRAY[_id];           -- start new array
       ELSE
          ids := ids || _id;           -- add to array
       END IF;

       _id0  := _id;
       _uid0 := _uid;                  -- remember last row
   END LOOP;

   RETURN QUERY VALUES (ids);          -- output last iteration
END
$func$;

SELECT * FROM f_alarm_groups();
*/

/* test functie : lukt niet 
drop function public.ssav_f_alarm_groups();

CREATE OR REPLACE FUNCTION public.ssav_f_alarm_groups()

RETURNS TABLE(
		uuid uuid,
		unique_uuid uuid	) 

--	  returns setof alarms 
--	  returns setof uuid
 
  LANGUAGE plpgsql AS
$func$
declare
	_f record;
--	_f alarms%rowtype;
  --   _id    int;
--   _uid   int;
--   _id0   int;                         -- id of last row
--   _uid0  int;                         -- user_id of last row
BEGIN
   /*
	FOR _f IN
       SELECT a.time, a.machine, a.machineserial, a.recipe, a.recipeloadcounter, a.idx, a.loggeractivationuid, 
       a.loggeridentifier, a.skey, a.alarmstate, a.uuid 
       --,true as edge 
       FROM alarms a ORDER BY a.machine, a.idx, a.time
      */
       
   FOR _f IN
       (SELECT a.time, a.machineserial, a.idx, a.alarmstate , a.uuid
       FROM alarms_consolidated a  
       --where time > '2021-09-27 12:00:00'
       ORDER BY a.machineserial, a.idx, a.time 
       )
       
   loop
   
       IF _f.alarmstate = 0 then
          raise notice 'entering the then loop for : %' ,_f.alarmstate;
--          RETURN QUERY VALUES (_f.uuid);   -- output row (never happens after 1 row)
--          return next;
--          return next _f.uuid;
--          ids := ARRAY[_id];           -- start new array
       else
           	raise notice 'entering the else loop for : %',_f.alarmstate;
--          ids := ids || _id;           -- add to array
			-- 	do nothing       
       END IF;

--       _id0  := _id;
--       _uid0 := _uid;                  -- remember last row
   END LOOP;
      raise notice 'outside loop now !';

   --RETURN QUERY VALUES (_f.uuid);          -- output last iteration
   
--     return;
   
END
$func$;

SELECT * FROM ssav_f_alarm_groups();
*/


