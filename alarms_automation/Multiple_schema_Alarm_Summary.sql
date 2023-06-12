
-- ssav 2021/09/17
-- goal : create a setup to fill a (hyper)tabel per schema for the alarm summary


-------------------STEP 1 : loop through all schema's in a database ------------------

	-- Find all schemas  in the current database :
		SELECT schema_name 
	    FROM information_schema.schemata
	    WHERE schema_name NOT LIKE 'pg_%' 
	    AND schema_name != 'information_schema'
	    and schema_name not like '%information%'
	    and schema_name not like '_timescale%'
   	    and schema_name not like 'public';

	-- Find all tables and materialized views in the current database : 
		SELECT 'SELECT * FROM ' || table_schema || '.' || table_name || ';' AS query,
		table_schema || '.' || table_name as fullname
		FROM information_schema.tables 
		WHERE table_schema IN
		(
		    SELECT schema_name 
		    FROM information_schema.schemata
		    WHERE schema_name NOT LIKE 'pg_%' 
		    AND schema_name != 'information_schema'
		    and schema_name not like '%information%'
		    and schema_name not like '_timescale%'
       	    and schema_name not like 'public'
		);
	
--drop function public.ssav_loop_schema() 

create or replace function public.pattyn_find_customer_schema() returns setof text LANGUAGE plpgsql as $$
declare 
	_loopschema RECORD;
begin 

	-- SSAV 20210917
	-- function gives a simple recordset with all usefull (company) schema names in the current database...
	-- select * from pattyn_find_customer_schema()
	/*
	|pattyn_find_customer_schema|
	|---------------------------|
	|pattyntestplcts            |
	|testschema1                |
	
	*/

	--raise notice 'looping through all schemas';
	for _loopschema in 
		SELECT schema_name as fullschema
	    FROM information_schema.schemata
	    WHERE schema_name NOT LIKE 'pg_%' 
	    AND schema_name != 'information_schema'
	    and schema_name not like '%information%'
	    and schema_name not like '_timescale%'
   	    and schema_name not like 'public'
	    order by 1
	 loop 
		--out_record := loopschema.fullschema;
	 	--raise notice 'found schema with name : %' , _loopschema.fullschema; --quote_ident(schema_name);
	 	return next _loopschema.fullschema;
	end loop;
   return;
end;
$$;

select * from pattyn_find_customer_schema()

--end-----------------STEP 1 : find and loop through all schema's in a database ------------------

-------------------STEP 2 : find in all the Pattyn database schemas a specific table (e.g. "alarms")  ------------------

--use public.pattyn_table_exists
-- adapt function to take schema as argument !!

-- drop FUNCTION public.pattyn_table_exists(varchar, varchar)
CREATE OR REPLACE FUNCTION public.pattyn_table_exists( 
tablename character varying DEFAULT NULL::character varying,
schemaname character varying DEFAULT NULL::character varying,
OUT table_out regclass)
 RETURNS regclass
 LANGUAGE plpgsql
AS $function$

-- SSAV : 2021/09/15
-- SSAV : 2021/10/12 --updates
-- 
-- check if a table exists within (one of) the schemas in the searchpath of the user calling this function !
-- by omitting the schemaname in the function, this function can be used for different schema's, and thus for different users...
--	to check if they have (in their searchpath) access to the table 
--  
-- results are limited to database 'Pattyn' !!

-- usage : 
-- select pattyn_table_exists('weightdata')

-- if the searchpath = current shema, then output = 'alarms'
-- if the searchpath = other shema, then output = 'schemaname'.'alarms'

/*
show search_path --"$user", testschema1, public
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
*/

<<startblock>>
declare 
	_T ALIAS for tablename;
	_S ALIAS for schemaname;
	_safename boolean; 
	_safeschema boolean;
begin 	

	table_out := Null;
	-- check for special characters or keywords to prevent SQL injection !!

	_safename := false;
	_safeschema := false;
	select into strict _safename (select pattyn_check_string(_T));
	select into strict _safeschema (select pattyn_check_string(_S));

	if _S is null then 
		_safeschema = true ;
		_S := '%';
	end if;

	--raise notice 'status for safename : % ; status for safeschame : % .', _safename, _safeschema;
 
	if (_safename and _safeschema) = false then 
		raise notice 'exit startblock';
		table_out := null;
		exit startblock;
	else 
		--raise notice 'enter else with parameters _S : % and _T : %.' , _S, _T;
	 case when 
	 	(select exists ( 
			SELECT 1 -- table_schema ||'.'|| table_name 
			FROM information_schema.tables 
			where
			table_catalog= 'Pattyn'
			and table_schema like _S --'pattyntestplcts'
			AND table_name = _T
			))
		then 
		   	--raise notice '_T = %', _T;
		   
			case when _S = '%'
			    then table_out := to_regclass(_T);
			else table_out := to_regclass(_S ||'.'||_T);
			end case;
		else 
				--raise notice 'nothing found...';
				table_out := null;
		end case;
	
	 end if;
end;
$function$
;

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


-- STEP 3 update the alarm aggregation for all schema's !!  (loop through all companies)
/*
 * loop through all the schemas in the current database
 * 		perform a function <<public.pattyn_alarm_lines_aggregation (schemaname ) >> on each schema
 * 
 */

--drop function public.pattyn_update_alarm_lines_for_all_schemes( )
CREATE OR REPLACE FUNCTION public.pattyn_update_alarm_lines_for_all_schemes( )
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
			perform public.pattyn_alarm_lines_aggregation(rec.pattyn_find_customer_schema);
			raise notice 'executed function <<public.pattyn_alarm_lines_aggregation>> for scheme : %', rec.pattyn_find_customer_schema;
			raise notice '--------------------------------------------------------';
		end loop;
	end;
$function$
;



-- call the function !
select pattyn_update_alarm_lines_for_all_schemes();
select count(*) from pattyntestplcts.alarm_lines al; --6333
select * from pattyntestplcts.alarm_lines al order by time desc limit 10;

-- STEP 4 
-- create a procedure to loop through every scheme and perform the alarm_lines update

create or replace procedure public.dba_alarm_lines_update_job (job_id int, config jsonb)
	language plpgsql
	as $$
		begin 
			perform (select public.pattyn_update_alarm_lines_for_all_schemes());
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


