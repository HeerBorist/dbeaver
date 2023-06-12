
--------------------------------------------------------------------------------------
-------------- 					Overview of jobs 					  ----------------
--------------------------------------------------------------------------------------

select * from timescaledb_information.jobs
select * from timescaledb_information.job_stats 
where job_id = 2120 or where job_id = 2119
--select delete_job(1325);

-- job_alarm_consolidate
select * from timescaledb_information.jobs J right join
		timescaledb_information.job_stats JS on J.job_id = JS.job_id where JS.job_id = 2119
		order by J.application_name , J.job_id

dba_update_alarm_lines_for_all_schemes		
select * from timescaledb_information.jobs J right join
		timescaledb_information.job_stats JS on J.job_id = JS.job_id where JS.job_id = 2120
		order by J.application_name , J.job_id
		
		
-- small overview
Select distinct rtrim(left(application_name, length(application_name) - 6)) as shortname, 
	proc_schema ,proc_name, count(job_id) 
	from timescaledb_information.jobs 
	group by shortname, proc_schema , proc_name 
	order by proc_schema
/*
|shortname                          |proc_schema          |proc_name                          |count|
|-----------------------------------|---------------------|-----------------------------------|-----|
|Compression Policy                 |_timescaledb_internal|policy_compression                 |2    |
|Refresh Continuous Aggregate Policy|_timescaledb_internal|policy_refresh_continuous_aggregate|34   |
|Retention Policy                   |_timescaledb_internal|policy_retention                   |46   |
|Telemetry Report                   |_timescaledb_internal|policy_telemetry                   |1    |
|User-Defined Action                |public               |dba_tablesize_job                  |1    |
|User-Defined Action                |public               |size_collector                     |1    |
*/

select * from 	timescaledb_information.jobs J where proc_name like '%retent%'
and hypertable_name like '%gener%'

	
	
-- view definition with lots of details
SELECT *    FROM _timescaledb_config.bgw_job j
     LEFT JOIN _timescaledb_catalog.hypertable ht ON ht.id = j.hypertable_id
     LEFT JOIN _timescaledb_internal.bgw_job_stat js ON js.job_id = j.id
    where application_name like '%User-Defined%'
   order by j.id ;
   
    
-- get the crashing jobs in list of scheduled jobs: 
 select * from timescaledb_information.jobs J right join
		timescaledb_information.job_stats JS on J.job_id = JS.job_id 
where JS.total_failures > 0
and JS.last_run_status <> 'Success'

/*
|job_id|application_name          |schedule_interval|max_runtime|max_retries|retry_period|proc_schema|proc_name     |owner      |scheduled|config|next_start         |hypertable_schema|hypertable_name|hypertable_schema|hypertable_name|job_id|last_run_started_at|last_successful_finish|last_run_status|job_status|last_run_duration|next_start         |total_runs|total_successes|total_failures|
|------|--------------------------|-----------------|-----------|-----------|------------|-----------|--------------|-----------|---------|------|-------------------|-----------------|---------------|-----------------|---------------|------|-------------------|----------------------|---------------|----------|-----------------|-------------------|----------|---------------|--------------|
|1092  |User-Defined Action [1092]|00:05:00         |00:00:00   |-1         |00:05:00    |public     |size_collector|PattynAdmin|true     |{}    |2021-09-07 10:07:04|                 |               |                 |               |1092  |2021-09-07 09:42:28|2021-07-27 16:23:35   |Failed         |Scheduled |00:00:00.011424  |2021-09-07 10:07:04|29345     |27700          |1645          |
*/
 
-- find information for the 'size_collector' job ? 

-- list all functions and function definitions? 
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
and p.proname like '%pattyn%'
or p.proname like '%pbd%'
or p.proname like 'test%'

order by function_schema,
         function_name;
/*        
        


--SELECT add_job('pattynnovation.job_alarm_consolidate_pn', '7 min', config => '{}'); --1353

call run_job(1339)
call run_job(1349)
call run_job(1350)

--https://docs.timescale.com/api/latest/actions/alter_job/
select alter_job (1353, schedule_interval => interval '1 min', retry_period => interval '1 min', max_runtime => interval '6 min', max_retries => 2, next_start => '2022-05-05 14:00:00.0+02', config => '{}')

--(1353,00:04:00,00:06:00,2,00:01:00,t,{},"2022-05-05 14:00:00+02")



select alter_job   (1339, scheduled => false)
select alter_job   (1353, scheduled => true)
select delete_job (1349)
select delete_job (1339)
select delete_job (1353)

select * from pattynnovation.alarms_consolidated order by time desc limit 10


--function : 
select pattynnovation.pattyn_consolidate_alarms()

--drop procedure pattynnovation.job_alarm_consolidate(integer, jsonb)

CREATE OR REPLACE PROCEDURE pattynnovation.job_alarm_consolidate(job_id integer, config jsonb)
 LANGUAGE plpgsql
AS $procedure$
		begin 
			perform (select pattynnovation.pattyn_consolidate_alarms());
			--raise notice 'dit is een test voor uitvoering  : '; 
			--perform pattynnovation.pattyn_consolidate_alarms();
			--raise notice 'dit is een test na uitvoering  : ';
			
		end
	$procedure$
;

--call pattynnovation.job_alarm_consolidate(job_id integer, config jsonb)
call pattynnovation.job_alarm_consolidate(1353, '{}')
*/
show search_path 



        
--  select delete_job(1092)
         select delete_job(1338);
  