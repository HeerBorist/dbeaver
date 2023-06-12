-- 2021/04/22
-- todo : rename table and functions
-- point to pattyn....
-- remove the pretty fields
-- put materialized view on result
-- put retention on hypertable
-- 

-- Script sequence to find the size for all database objects and put in dashboard

-- create a job for this 'action'
--https://docs.timescale.com/v2.0/using-timescaledb/actions#create

SELECT * FROM timescaledb_information.jobs where job_id > 1233;
SELECT * FROM timescaledb_information.job_stats where job_id =1237;
select *  from timescaledb_information.continuous_aggregates
select * from timescaledb_information.chunks c where hypertable_name like '%231%'



-- Create a user defined action to schedule : 

create or replace procedure size_collector (job_id int, config jsonb)
language plpgsql
as $$
	begin 
		perform (select public.testinsert());
	end
$$;


	create or replace procedure dba_tablesize_job (job_id int, config jsonb)
	language plpgsql
	as $$
		begin 
			perform (select public.pattyn_dba_tablesizes());
		end
	$$;

SELECT add_job('dba_tablesize_job', '5 min', config => '{}'); --1097



 -- Twee testen : Add a job 
SELECT add_job('size_collector', '5 min', config => '{}'); --1092

--select add_job('public.testinsert', '15 min', config => '{}'); --1091 Lijkt te mislukken !
-- call run_job(1091)
-- SQL Error [42883]: ERROR: function public.testinsert(integer, jsonb) does not exist

--SELECT alter_job(1000, config => '{"hypertable":"metrics"}');

SELECT alter_job(1000, scheduled => false);
SELECT alter_job(1000, scheduled => true);

SELECT delete_job(1090);
call run_job(1091)


-- the function : 
select public.testinsert()



-- drop table public.mytable
-- drop function public.testinsert()

-- Create a function for this:

---------------------------START OF THE FUNCTION ---------------------------------
create or replace function public.testinsert ()
returns void
language plpgsql

as $$

	begin 
		-- COMMENT GOES HERE
	
	CREATE TABLE if not exists public.mytable (
	"time" timestamptz NOT NULL,
	"typeNr" int4 NULL,
	"type" varchar NULL,
	relscheme varchar NULL,
	relname varchar NULL,
	chunkname varchar NULL,
	viewname varchar NULL,
	"viewOwner" varchar NULL,
	rowcount float4 NULL,
	"RngStart" timestamptz NULL,
	"RngEnd" timestamptz NULL,
	"size" int8 NULL,
	"sizePretty" text NULL,
	"indexSize" int8 NULL,
	"indexSizePretty" text NULL,
	totalsize int8 NULL,
	"totalsizePretty" text NULL
);
	--CREATE INDEX mytable_time_idx ON public.mytable USING btree ("time" DESC);

	
	--Convert table to hypertable. Do not raise a warning if the table is already a hypertable:
	perform (select create_hypertable('public.mytable', 'time', chunk_time_interval => INTERVAL '10 day', if_not_exists => true, migrate_data => TRUE));

	
		with alldata as (
			select 
			1 as "typeNr",
			'TABLE' as "type",
			nspname as "relscheme",
			relname as "relname",
			'' as "chunkname",
			'' as "viewname",
			'' as "viewOwner",
			reltuples as "rowcount",
			null as "RngStart",
			null as "RngEnd",
			  pg_relation_size(C.oid) "size"
			,  pg_size_pretty(pg_relation_size(C.oid)) "sizePretty"
			, pg_indexes_size(C.oid) "indexSize"
			, pg_size_pretty(pg_indexes_size(C.oid)) "indexSizePretty" 
			, pg_total_relation_size(C.oid) "totalsize"
			, pg_size_pretty(pg_total_relation_size(C.oid)) "totalsizePretty"
			from pg_class C
			left join pg_namespace N on C.relnamespace = N.oid
			where C.relkind = 'r'
			and C.relnamespace in (select oid from pg_namespace where nspname NOT IN (
			        'pg_catalog',
			        'information_schema',
			        '_timescaledb_catalog',
			        '_timescaledb_config',
			        'timescaledb_information',
			        '_timescaledb_cache',
			        '_timescaledb_internal')
			    )
			union all
			--Step 2 : chunks
			select 
			2 as "typeNr",
			'CHUNK' as "type",
			ch.hypertable_schema as "relscheme",
			CH.hypertable_name as "relname",
			C.relname as "chunkname",
			'' as "viewname",
			'' as "viewOwner",
			C.reltuples as "rowcount",
			ch.range_start as "RngStart",
			ch.range_end as "RngEnd",
			  pg_relation_size(C.oid) "size"
			,  pg_size_pretty(pg_relation_size(C.oid)) "sizePretty"
			, pg_indexes_size(C.oid) "indexSize"
			, pg_size_pretty(pg_indexes_size(C.oid)) "indexSizePretty" 
			, pg_total_relation_size(C.oid) "totalsize"
			, pg_size_pretty(pg_total_relation_size(C.oid)) "totalsizePretty"
			--,* 
			from pg_class C
			left join timescaledb_information.chunks CH on C.relname = CH.chunk_name
			where C.relkind = 'r'
			and C.relnamespace in (select oid from pg_namespace where nspname NOT IN (
			        'pg_catalog',
			        'information_schema',
			        '_timescaledb_catalog',
			        '_timescaledb_config',
			        'timescaledb_information',
			        '_timescaledb_cache') 
			    )
			and ch.hypertable_schema not in ('_timescaledb_internal')
			and ch.chunk_name is not null 
			 --Step 3 : materialized views
			union all
			select 
			3 as "typeNr",
			'MATV' as "type",
			mv.view_schema as "relscheme",
			MV.hypertable_name as "relname",
			C.relname as "chunkname",
			mv.view_name as "viewname",
			mv.view_owner as "viewOwner",
			C.reltuples as "rowcount",
			ch.range_start as "RngStart",
			ch.range_end as "RngEnd",
			  pg_relation_size(C.oid) "size"
			,  pg_size_pretty(pg_relation_size(C.oid)) "sizePretty"
			, pg_indexes_size(C.oid) "indexSize"
			, pg_size_pretty(pg_indexes_size(C.oid)) "indexSizePretty" 
			, pg_total_relation_size(C.oid) "totalsize"
			, pg_size_pretty(pg_total_relation_size(C.oid)) "totalsizePretty"
			,* 
			from pg_class C
			left join timescaledb_information.chunks CH on C.relname = CH.chunk_name
			left join timescaledb_information.continuous_aggregates MV on (CH.hypertable_name = MV.materialization_hypertable_name and CH.hypertable_schema = MV.materialization_hypertable_schema )
			where C.relkind = 'r'
			and C.relnamespace in (select oid from pg_namespace where nspname NOT IN (
			        'pg_catalog',
			        'information_schema',
			        '_timescaledb_catalog',
			        '_timescaledb_config',
			        'timescaledb_information',
			        '_timescaledb_cache')
			    )
			 and MV.hypertable_name is not null 
			order by relscheme, relname, "typeNr" , chunkname
			 ) 
		-- select now() as time, * from alldata
		 insert into public.mytable select now() as time, * from alldata;
		--	select * into public.mytable from (select now() as time, * from alldata) as a

		return;
	end;
$$ ;

-------------------------------- end of the function !! -----------------------------



-- get the latest resultset table : 
select * from public.mytable M where time in (select last(time,time) from public.mytable )
order by  relscheme, relname,"typeNr", size desc

select public.pattyn_dba_tablesizes()

select * from public.dbatablesizes M where time in (select last(time,time) from public.dbatablesizes )
order by  relscheme, relname,"typeNr", size desc


---******************************************* INDEX ********************

-- testen met index...
select time, relscheme, relname, 
pg_size_pretty(sum(size)) as size, 
pg_size_pretty(sum("indexSize" )) as indexsize, 
pg_size_pretty(sum(totalsize )) as totalsize
,round(sum("indexSize" )/sum(size),2) as indexratio
from public.dbatablesizes M where time in (select last(time,time) from public.dbatablesizes )
and M.relname like '%general%'
group by time,relscheme, relname
order by  relscheme, relname
-- get the latest resultset table : add retention policy and mat view information 

DROP INDEX ppltesttslite3.general_loggeractivationuid;
DROP INDEX ppltesttslite3.general_loggeridentifier;
DROP INDEX ppltesttslite3.general_machine;
DROP INDEX ppltesttslite3.general_machineserial;
--DROP INDEX ppltesttslite3.general_recipebatch;


---*******************************************END INDEX********************

-- summarize a boolean :
select
	recipe,
	time_bucket( '1 hour',	time) as bucket,
	sum(boxmade::integer) as boxmade
from
	ppltesttslite3."general"
group by
	recipe ,
	bucket
order by
	recipe, bucket desc
limit 1000

--voor : 
--2021-04-26 14:35:20	ppltesttslite2	general	51 MB	29 MB	81 MB	0.58
--2021-04-26 14:35:20	ppltesttslite3	general	1264 kB	888 kB	2184 kB	0.70


---*******************************************COMPRESSION********************
-- test compression of objects table : 
alter table ppltesttslite2.objects SET(
timescaledb.compress,
timescaledb.compress_segmentby = 'recipe');

select add_compression_policy ('ppltesttslite2.objects', interval '3 days');





--- compression for argocheck

select * from ppltesttslite2.argocheck_datas_pbd adp order by time desc limit 100

select pg_total_relation_size('ppltesttslite2.argocheck_datas_pbd') --49152

alter table ppltesttslite2.argocheck_datas_pbd SET(
timescaledb.compress,
timescaledb.compress_segmentby = 'recipenamestr');

select add_compression_policy ('ppltesttslite2.argocheck_datas_pbd', interval '1 days'); --1126

-- at start : 4.831 Gb
-- after compression : 






	--compression on materialized view ? ==> dit lukt niet
alter table testplctimescale.weightdata_hourly SET(
timescaledb.compress,
timescaledb.compress_segmentby = 'recipe');


--select add_compression_policy ('testplctimescale.weightdata_hourly', interval '3 days');


-- COMPRESSION
select * from timescaledb_information.jobs
select * from timescaledb_information.compression_settings

select show_chunks('ppltesttslite2.objects')
select * from chunk_compression_stats('ppltesttslite2.argocheck_datas_pbd')

--SELECT compress_chunk(i) from show_chunks('conditions', newer_than, older_than) i;
SELECT decompress_chunk('_timescaledb_internal._hyper_2_2_chunk');

select drop_chunk ('_timescaledb_internal._hyper_80_334_chunk')

select * from ppltesttslite2.objects o order by time desc limit 10000
----DECOMPRESSION************

SELECT s.job_id
FROM timescaledb_information.jobs j
  INNER JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
WHERE j.proc_name = 'policy_compression' AND s.hypertable_name = 'objects'; --1110 ?

-- pause the compression job
SELECT alter_job(1110, scheduled => false);
-- first decompres all chunks, then the table...

--SELECT decompress_chunk(i) from show_chunks('ppltesttslite2.objects', newer_than, older_than) i;
SELECT decompress_chunk(i) from show_chunks('ppltesttslite2.objects') i;

SELECT decompress_chunk('_timescaledb_internal._hyper_80_334_chunk')

ALTER TABLE ppltesttslite2.objects SET (timescaledb.compress = false );


---*******************************************END COMPRESSION********************



---******************************************* POLICIES VIEW CREATION ********************


select  
config ->> 'drop_after' as Retention,
config ->> 'drop_after' as dropafterTEXT ,
* 
from public.mytable M 
left join timescaledb_information.jobs J on( M.relscheme = J.hypertable_schema and M.relname = J.hypertable_name )
where M.time in (select last(time,time) from public.mytable ) and M.relname  like '%objects%'
order by  M.relscheme, M.relname,M."typeNr", M.size desc



-- Opzoeken welke retention policies er actief zijn : HIERVOOR hypertable id gebruiken ? of schema + name ?
-- join of lateral met bovenstaande functie ? 
-- view aanmaken voor laatste run van de functie ??

select * from timescaledb_information.jobs
select * from timescaledb_information.job_stats -- show jobs/policies
select * from timescaledb_information.jobs where job_id = 1079
-- config : JSON {"drop_after": "04:00:00", "hypertable_id": 31}
--{"end_offset": "01:00:00", "start_offset": "10 days", "mat_hypertable_id": 48}
select * from timescaledb_information.job_stats where job_id = 1079
select * from timescaledb_information.job_stats where job_id = 1110
select * from alter_job(1079) -- geeft alle nodige informatie ?



-- How to query the JSON column : basic, no subnodes
--PostgreSQL provides two native operators -> and ->> to help you query JSON data.

	--The operator -> returns JSON object field by key.
	--The operator ->> returns JSON object field by text.

select config -> 'drop_after' as droptime, * from timescaledb_information.jobs


/*select distinct proc_name from timescaledb_information.jobs
-- proc_name : 
policy_compression
dba_tablesize_job
policy_refresh_continuous_aggregate
policy_retention
policy_telemetry
size_collector*/

select * from ppltesttslite3.weightdata_v2 wv order by time desc

--select config from alter_job(1079)
select job_id, proc_name, owner, scheduled, config ,
config -> 'drop_after' as dropafterJSON ,
config ->> 'drop_after' as dropafterTEXT ,
config ->> 'hypertable_id' as id,
--cast(config ->> 'drop_after' as interval) as dropinterval
*
from timescaledb_information.jobs
where config -> 'drop_after' is not null -- find e.g. retention policies contain field drop_after 

select * from public.mytable m order by time desc limit 200



-- op zoek naar de id's ??
select * from _timescaledb_catalog.hypertable where id=27
select * from _timescaledb_catalog.chunk where hypertable_id = 27 --dropped !!
select * from _timescaledb_catalog.continuous_agg where raw_hypertable_id = 27 --mat_hypertable_id = 48


select config -> 'drop_after' as droptime, * from timescaledb_information.jobs


select * from timescaledb_information.continuous_aggregates

select * from timescaledb_information.jobs J where j.proc_name = 'policy_refresh_continuous_aggregate'


			 --


