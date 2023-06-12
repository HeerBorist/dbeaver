
--https://docs.timescale.com/api/latest/actions/add_job/#add-job
--https://docs.timescale.com/timescaledb/latest/how-to-guides/user-defined-actions/
--https://docs.timescale.com/timescaledb/latest/overview/core-concepts/user-defined-actions/#examples
--https://github.com/timescale/timescaledb/issues/2966




-- the full package, as simple as possible : 
-- 1/ create a table
-- 2/ create a function
-- 3/ create a procedure (with job_id and jsonconfig) !! to perform the function 
-- 4/ add job to run the procedure every minute 
-- 5/ relax and enjoy...


-- DROP TABLE pattynnovation.pfr_test_job_result_table;
CREATE TABLE pattynnovation.pfr_test_job_result_table (
	value int4 NULL,
	"time" timestamp NULL
);

--test : 
--select * from pattynnovation.pfr_test_job_result_table
--select count(*) from pattynnovation.pfr_test_job_result_table


--DROP FUNCTION pattynnovation.pfr_function_job_basic_insert()
CREATE OR REPLACE FUNCTION pattynnovation.pfr_function_job_basic_insert()
returns void 
--returns boolean -- if you need some kind of output confirmation
 LANGUAGE plpgsql
AS $function$
	begin
		insert into pattynnovation.pfr_test_job_result_table (value, time)
		values ( 1, now());

		--return true;
	END;

$function$
;
-- test : 
--select * from pattynnovation.pfr_function_job_basic_insert()
-- drop procedure pattynnovation.pfr_test_job_basic_insert(integer, jsonb)
CREATE OR REPLACE PROCEDURE pattynnovation.pfr_test_job_basic_insert(job_id integer, config jsonb)
LANGUAGE plpgsql
AS $procedure$
begin
	--perform (select pattynnovation.pfr_function_job_basic_insert());
	perform pattynnovation.pfr_function_job_basic_insert();
	raise notice 'executed job % with config % on %', job_id, config, now();
END
$procedure$;



--NEED TO ADD THE ID and CONFIG JSON !!
--call pattynnovation.pfr_test_job_basic_insert(1,'{}')

select add_job('pattynnovation.pfr_test_job_basic_insert', '1 minute', config => '{}'); --1351
--select delete_job(1337);

set client_min_messages to DEBUG1;

call run_job(1351)

select * from timescaledb_information.jobs
select * from timescaledb_information.job_stats where job_id = 1337


select * from timescaledb_information.jobs J right join
		timescaledb_information.job_stats JS on J.job_id = JS.job_id where JS.job_id = 1351
		order by J.application_name , J.job_id
		

select * from pattynnovation.pfr_test_job_result_table 


select * from timescaledb_information.job_stats where job_id = 1358

--select delete_job(1353)
select alter_job   (1358, scheduled => false)
select alter_job   (1358, scheduled => true)


--https://docs.timescale.com/api/latest/actions/alter_job/
select alter_job (1353, schedule_interval => interval '1 min', retry_period => interval '1 min', max_runtime => interval '6 min', max_retries => 2, next_start => '2022-05-05 14:00:00.0+02', config => '{}')

--(1353,00:04:00,00:06:00,2,00:01:00,t,{},"2022-05-05 14:00:00+02")



SELECT *    FROM _timescaledb_config.bgw_job j
     LEFT JOIN _timescaledb_catalog.hypertable ht ON ht.id = j.hypertable_id
     LEFT JOIN _timescaledb_internal.bgw_job_stat js ON js.job_id = j.id
    where application_name like '%User-Defined%' 
--    and j.id = 1358
   order by j.id ;



