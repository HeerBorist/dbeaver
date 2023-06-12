
-- ssav 20220330
-- goal : create a setup to fill a (hyper)tabel per schema for the general machine status aggregation 
-- we have to create two (2) aggregations !


-------------------STEP 1 : loop through all schema's in a database ------------------

select * from pattyn_find_customer_schema()

--end-----------------STEP 1 : find and loop through all schema's in a database ------------------

-------------------STEP 2 : find in all the Pattyn database schemas a specific table (e.g. "alarms")  ------------------

--use public.pattyn_table_exists
-- adapt function to take schema as argument !!

-- drop FUNCTION public.pattyn_table_exists(varchar, varchar)
-- select * from public.pattyn_table_exists('general')

/*
show search_path

show search_path --"$user", testschema1, public
select * from public.pattyn_table_exists('alarms') 						--alarms
select * from public.pattyn_table_exists('alarms','testschema1') 		--alarms
select * from public.pattyn_table_exists('alarms','pattyntestplcts') 	--pattyntestplcts.alarms
--or 
select public.pattyn_table_exists('alarms') 					--alarms
select public.pattyn_table_exists('alarms','testschema1') 		--alarms
select public.pattyn_table_exists('alarms','pattyntestplcts') 	--pattyntestplcts.alarms
select public.pattyn_table_exists('alarms','public') 			--NULL
*/

select * from information_schema.tables where table_catalog = 'Pattyn' 
and table_schema like '%'
and table_name = 'alarms'
--end-----------------STEP 2 : find in all the Pattyn database schemas a specific table (e.g. "alarms")  ------------------


-- STEP 3 update the general machine aggregation for all schema's !!  (loop through all companies)
/*
 * loop through all the schemas in the current database
 * 		perform a function <<public.pattyn_general_boxproduction_aggregation (schemaname ) >> on each schema
 * 		perform a function <<public.pattyn_general_xxx_aggregation (schemaname ) >> on each schema
 * 
 */

--drop function public.pattyn_update_alarm_lines_for_all_schemes( )
CREATE OR REPLACE FUNCTION public.pattyn_update_general_machine_lines_for_all_schemes( )
	RETURNS void
	
	LANGUAGE plpgsql
	AS $function$
	declare 
		rec record;
	begin 
		raise notice 'start function <<public.pattyn_update_alarm_lines_for_all_schemes>>';
		for rec in 
			select pattyn_find_customer_schema from pattyn_find_customer_schema()
		loop
			raise notice 'schema found : %', rec.pattyn_find_customer_schema;
		
		-- perform 2 functions here for the general machine update : 
			--aggregation for boxproduction
			perform public.pattyn_general_boxproduction_aggregation (rec.pattyn_find_customer_schema);
			raise notice 'executed function <<public.pattyn_general_boxproduction_aggregation>> for scheme : %', rec.pattyn_find_customer_schema;
		
			--- aggregation for general machine status

		--	perform public.pattyn_alarm_lines_aggregation(rec.pattyn_find_customer_schema);
		
		
		
--			raise notice 'executed function <<public.pattyn_alarm_lines_aggregation>> for scheme : %', rec.pattyn_find_customer_schema;
			raise notice '--------------------------------------------------------';
		end loop;
	end;
$function$
;



-- call the function !
--select pattyn_update_alarm_lines_for_all_schemes();
--select count(*) from pattyntestplcts.alarm_lines al; --6333
--select * from pattyntestplcts.alarm_lines al order by time desc limit 10;

-- STEP 4 
-- create a procedure to loop through every scheme and perform the alarm_lines update

create or replace procedure public.dba_general_update_job (job_id int, config jsonb)
	language plpgsql
	as $$
		begin 
			perform (select public.pattyn_update_general_machine_lines_for_all_schemes());
		end
	$$;

--create a continuous automation for step 3 !! (a scheduled job)
SELECT add_job('public.dba_alarm_lines_update_job', '1 hour', config => '{}'); --2120

-- end STEP 4



--SELECT alter_job(2120, scheduled => false);
--SELECT alter_job(2120, scheduled => true);
--SELECT delete_job(2120);
--call run_job(2120)

SELECT * FROM timescaledb_information.job_stats where job_id = 1299;
-- show the jobs
SELECT * FROM timescaledb_information.jobs where job_id = 2120;


