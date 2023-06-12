select time_bucket('1 hour', time) as hour, count(*) from pattyntestplcts.argocheck_criterions_pbd
group by hour
order by hour desc limit 1000



select * from pattyntestplcts.argocheck_criterions_pbd order by time  desc limit 100
--2021-06-28 15:38:56
--2021-06-28 18:15:11
--2021-06-28 19:29:10
--2021-06-29 05:20:19
-- 2021-06-30 07:37:25


select * from pattyntestplcts.argocheck_criterions_pbd order by time  asc limit 100
--2021-06-24 14:55:27

select first(time, time) from pattyntestplcts.argocheck_criterions_pbd
select last(time, time) from pattyntestplcts.argocheck_criterions_pbd

select 
first(time, time) as start,
last (time, time) as end,
first(criterionname, time)
from pattyntestplcts.argocheck_criterions_pbd

(EXTRACT(EPOCH FROM last(time, time)) - EXTRACT(EPOCH FROM first(time, time)))/3600 as hours ,
--count(*) as rows,
approximate_row_count('pattyntestplcts.argocheck_criterions_pbd') as rows 
from pattyntestplcts.argocheck_criterions_pbd

select * from public.dba_database_details dtd 
where relscheme = 'pattyntestplcts'
and relname = 'argocheck_criterions_pbd'

select * from public.dba_compression_summary dcs  
where relscheme = 'pattyntestplcts'
and relname = 'argocheck_criterions_pbd'

select * from public.dba_chunk_details dcd   
where relscheme = 'pattyntestplcts'
--and relname = 'argocheck_criterions_pbd'
and relname like '%criteri%'
order by chunk_rngstart desc

select * from public.dba_chunk_details dcd   
where relscheme = 'pattyntestplcts'
--and relname = 'argocheck_criterions_pbd'
and relname like '%actor%'
order by chunk_rngstart desc



select (EXTRACT(EPOCH FROM '2021-06-30 08:12:21') - EXTRACT(EPOCH FROM '2021-06-24 14:55:27'))/3600


analyze pattyntestplcts.argocheck_criterions_pbd


--75.765.440
--80.617.936

select count(*) from pattyntestplcts.argocheck_criterions_pbd -- takes about 33 seconds for result : 80.618.371
--
select * from approximate_row_count('pattyntestplcts.argocheck_criterions_pbd') -- takes about 0.2 seconds for result : 80.617.936
--92.086.240

select * from pattyntestplcts.argocheck_criterions_pbd
where time > '2021-06-26 03:00:00' and time < '2021-06-26 03:01:00'
order by criterionname , productid
--limit 1000

select * from timescaledb_information.jobs where hypertable_schema like '%testplcts%'




-- local retention policy for criterions : set to 7 days !!
-- cloud compression policy for criterions: set to 8 days !!
-- cloud retention policy for criterions : set to 15 days !!

-- retention policy for materialized views ? (no compression possible)
-- 

/*
SELECT remove_retention_policy('pattyntestplcts.argocheck_criterions_pbd');
SELECT add_retention_policy('pattyntestplcts.argocheck_criterions_pbd', INTERVAL '15 days'); --120

alter table pattyntestplcts.argocheck_criterions_pbd SET(timescaledb.compress);
select add_compression_policy ('pattyntestplcts.argocheck_criterions_pbd', interval '8 days');

SELECT remove_compression_policy('pattyntestplcts.sensorobjects');
SELECT add_compression_policy('pattyntestplcts.sensorobjects', INTERVAL '15d');

SELECT add_compression_policy('pattyntestplcts.actorobjects', INTERVAL '15d'); 7439 MB, 41 chunks

*/

select * from pattyntestplcts.argocheck_general_datas_pbd order by time desc

select * from timescaledb_information.continuous_aggregates ca 
where hypertable_name like '%count%' 
--_materialized_hypertable_305

select * from timescaledb_information.jobs where job_id=1353 
where hypertable_schema like '%testplcts%'
--{"end_offset": "01:00:00", "start_offset": "6 days", "mat_hypertable_id": 305}

select * from pattyntestplcts.pbd_argocount_1h
order by time desc
limit 100

select BUCKET, * from pattyntestplcts.pbd_argocount_1 
where firstbatch is not null
order by bucket desc , time desc 



limit 1000---474


select * from pattyntestplcts.argocount_datas_pbd order by time desc limit 1000
--124247

select * from pattyntestplcts.argocheck_criterions_pbd order by time desc limit 1000

;

/*
--with data  -- with no data

SELECT add_continuous_aggregate_policy('weightdata_hourly',
    start_offset => INTERVAL '2 day', 
    end_offset => INTERVAL '1 h',
    schedule_interval => INTERVAL '1 h'); 

*/

select * from pg_roles limit 100
select * from pg_catalog.pg_hba_file_rules 

select * from public.test_general 

create rule "_RETURN" as on select to public.test_general
do instead


pattyntestplcts.actorobjects

select * from pattyntestplcts.actorobjects a 
--where machine not like '%NoRead%'
order by time desc limit 10

select last(time, time), machine from alarms
--where time > '2021-06-10'
group  by machine

select * from alarms 
where machine = '1002' and time > '2021-06-01' 
order by time desc limit 12

select last(time, time), machine from pattyntestplcts.actorobjects
where time > '2021-06-01'
group  by machine

show search_path

Users : try out :

select * from pg_catalog.pg_user
select * from pg_catalog.pg_stat_activity psa 

--table permissions:

select 
 * 
from information_schema.role_table_grants 
where grantee='YOUR_USER'
;

--ownership:

select 
   * 
from pg_tables 
where tableowner = 'YOUR_USER'
;

--schema permissions:

select  
  r.usename as grantor, e.usename as grantee, nspname, privilege_type, is_grantable
from pg_namespace
join lateral (
  SELECT
    *
  from
    aclexplode(nspacl) as x
) a on true
join pg_user e on a.grantee = e.usesysid
join pg_user r on a.grantor = r.usesysid 
 where e.usename = 'YOUR_USER'
;


ALTER TABLE pattyntestplcts.weightdata_hourly OWNER TO testcompany;
ALTER TABLE pattyntestplcts.weightdata_hourly OWNER TO "PattynAdmin";
GRANT ALL ON TABLE pattyntestplcts.weightdata_hourly TO testcompany;
revoke ALL ON TABLE pattyntestplcts.weightdata_hourly from testcompany;

GRANT select ON TABLE pattyntestplcts.weightdata_hourly TO testcompany;

select * from pattyntestplcts.weightdata_hourly limit 10


select * from public.pattyn_weight_summary()

select * from public.pattyn_weight_summary('%cal%')

select  
 (select * from pattyn_getbatchstart('20')) as fuctiontest, -- hier worrdt in 'subquery' de functie opgeroepen !
 G.recipe,
 G.machine,
 G.recipeloadcounter
from general G
limit 100





SELECT
  RecipeLoadCounter as "Id"
  , mode() WITHIN GROUP (ORDER BY Recipe) as "Recipe"
  , extract(epoch from first(time, time))*1000 as "Start"
  , extract(epoch from last(time, time))*1000 as "End"
  , extract(epoch from last(time, time)) - extract(epoch from first(time, time)) as "Duration"
  , sum(boxmade) as "Total boxes"
  , sum(boxmade)*60/(extract(epoch from last(time, time)) - extract(epoch from first(time, time))) as "Speed"
  , pattyn_dtou_statecnt_aggr(time, stmachine_emachineprogramstate::int, 600 order by time asc) filter (where stmachine_emachineprogramstate is not null) as "Alarm count"
  , pattyn_dtou_stateduration_aggr(time, stmachine_emachineprogramstate::int, 600 order by time asc) filter (where stmachine_emachineprogramstate is not null) as "Alarm duration"
FROM general
WHERE 
RecipeLoadCounter = '55'
--  recipe in ($Recipe) and 
--  $__timeFilter(time) 
GROUP BY RecipeLoadCounter
ORDER BY RecipeLoadCounter desc
limit 50

select * from general order by time desc limit 100


select * from pg_proc where proname like '%dtou%'

select proname, prosrc,proargnames, proargdefaults  from pg_proc where proname like '%dtou%'



select n.nspname as function_schema,
       p.proname as function_name,
       l.lanname as function_language,
       case when l.lanname = 'internal' then p.prosrc
            else pg_get_functiondef(p.oid)
            end as definition,
       pg_get_function_arguments(p.oid) as function_arguments,
       t.typname as return_type
from pg_proc p
left join pg_namespace n on p.pronamespace = n.oid
left join pg_language l on p.prolang = l.oid
left join pg_type t on t.oid = p.prorettype 
where n.nspname not in ('pg_catalog', 'information_schema')
and p.proname like '%dtou%'
order by function_schema,
         function_name;
        
        
        
select n.nspname as schema_name,
       p.proname as specific_name,
       case p.prokind 
            when 'f' then 'FUNCTION'
            when 'p' then 'PROCEDURE'
            when 'a' then 'AGGREGATE'
            when 'w' then 'WINDOW'
            end as kind,
       l.lanname as language,
       case when l.lanname = 'internal' then p.prosrc
            else pg_get_functiondef(p.oid)
            end as definition,
       pg_get_function_arguments(p.oid) as arguments,
       t.typname as return_type
from pg_proc p
left join pg_namespace n on p.pronamespace = n.oid
left join pg_language l on p.prolang = l.oid
left join pg_type t on t.oid = p.prorettype 
where n.nspname not in ('pg_catalog', 'information_schema')
and p.proname like '%dtou%'
order by schema_name,
         specific_name;

        
        
SELECT time_bucket('1 hour',time) AS hourly,machine, machineserial, recipe,recipeloadcounter, sum(boxmade) AS boxmade
FROM general
WHERE time > NOW() - INTERVAL '25 hours' AND machineserial is not null group by machine, machineserial, hourly, recipe, recipeloadcounter
ORDER BY hourly desc, machine, recipe, recipeloadcounter desc

select * from general order by time desc limit 1000



