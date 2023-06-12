

--https://www.dbrnd.com/2017/07/postgresql-different-options-to-check-if-table-exists-in-database-to_regclass/

-- procedure to check if a table exists. does this work for materialized views, views ??
-- is this SQL Injection safe ? 

-- can we read the actual schema for the user ??  via searchpath ???


SELECT to_regclass('schema_name.table_name');

SELECT EXISTS 
(
	SELECT 1
	FROM information_schema.tables 
	
	WHERE table_schema = 'schema_name'
---	AND table_catalog=‘your_database_name’
	AND table_name = 'table_name'
);

SELECT EXISTS 
(
	SELECT 1 
	FROM pg_catalog.pg_class c
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = 'schema_name'
	AND c.relname = 'table_name'
	AND c.relkind = 'r' -- r = tables 
);

SELECT EXISTS 
(
	SELECT 1 
	FROM pg_tables
	WHERE schemaname = 'schema_name'
	AND tablename = 'table_name'
);


--1- test with table  
pattyntestplcts.weightdata
--2- test with materialized view
pattyntestplcts.weightdata_hourly
--3- test with normal view : 
public.vw_dashboardlogs

---------------------------- 1 ---------------------------------------

SELECT to_regclass('pattyntestplcts.weightdata'); --pattyntestplcts.weightdata  (voor user PattynAdmin)
SELECT to_regclass('pattyntestplcts.weightdata'); --weightdata  (voor user testcompany)
SELECT to_regclass('pattyntestplcts.weightdata1'); -- null
SELECT to_regclass('weightdata'); -- weightdata  (enkel als in het goeie schema !!)

-- Hiermee kan een 'variabele' worden getest op bestaan als regclass ??
with test as (select current_schema ||'.weightdata' as testname)
SELECT to_regclass(test.testname), test.testname from test; 

SELECT to_regclass('weightdata_hourly'); -- weightdata  (enkel als in het goeie schema !!)
SELECT to_regclass('public.vw_dashboardlogs'); -- weightdata  (enkel als in het goeie schema !!)

SELECT to_regclass('vw_dashboardlogs'); -- weightdata  (enkel als in het goeie schema !!)


show search_path; --"$user", public


select current_schema; --pattyntestplcts
select current_schema ||'.weightdata'; --pattyntestplcts
select current_user;

SELECT schema_name, * FROM information_schema.schemata
where schema_name ; 
pg_get_viewdef('weightdata_hourly')



SELECT EXISTS 
(
	SELECT 1
	FROM information_schema.tables 
---	WHERE table_schema = 'pattyntestplcts'
	where  table_catalog= 'Pattyn'
	AND table_name = 'vw_dashboardlogs'
);

SELECT to_regclass('vw_dashboardlogs'); -- weightdata  (enkel als in het goeie schema !!)


---------------------------- CREATE A FUNCTION FOR THIS  ---------------------------------------

create or replace function public.pattyn_table_exists(tablename character varying DEFAULT NULL::character varying, out table_out regclass)
returns regclass
language plpgsql

as $function$

-- SSAV : 2021/09/15
-- 
-- check if a table exists within (one of) the schemas in the searchpath of the user calling this function !
-- by omitting the schemaname in the function, this function can be used for different schema's, and thus for different users...
-- results are limited to database 'Pattyn'

-- usage : 
-- select pattyn_table_exists('weightdata')

<<startblock>>
declare 
	_T ALIAS for tablename;
	_safename boolean; 
begin 	

	table_out := Null;
	-- check for special characters or keywords to prevent SQL injection !!
	select into strict _safename (select pattyn_check_string(_T));

	if not _safename then 
		exit startblock;
	else 

	 case when 
	 	(select EXISTS( 
			SELECT 1 FROM information_schema.tables 
			--	WHERE table_schema = 'pattyntestplcts'
			where  table_catalog= 'Pattyn' 	
			AND table_name = _T
			))
		then 
				table_out := to_regclass(_T);
		else 
				table_out := null;
		end case;
	 end if;
end;
$function$;

---------------------------- end of CREATE A FUNCTION FOR THIS  ---------------------------------------


select to_regclass('weightdata')
--select pattyn_check_string('weightdata')
select current_schema || '.' || pattyn_table_exists('weightdata') as 
select pattyn_table_exists('weightdata')


-- alarms test
select time, machineserial, alarmstate, alarmskeyfollow, recipe  from alarms4 
where alarmskeyfollow = '_29_1_39_12'
and recipe = 'Barry Callebaut'
and   time < '2021-09-01 11:34:48' and time > '2021-08-31 15:06:26'
order by alarmskeyfollow, time asc limit 10000