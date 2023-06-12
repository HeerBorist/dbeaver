CREATE OR REPLACE FUNCTION public.pattyn_dba_tablesizes()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

	begin 
		-- COMMENT GOES HERE

		-- how to use : 
		-- select public.pattyn_dba_tablesizes()
		--
		-- select last resultset : 
		--
/*		select * from public.dbatablesizes M where time in (select last(time,time) from public.dbatablesizes )
order by  relscheme, relname,"typeNr", size desc
*/

		
		
		
-- Add a custom action	
/*	
	create or replace procedure dba_tablesize_job (job_id int, config jsonb)
	language plpgsql
	as $$
		begin 
			perform (select public.pattyn_dba_tablesizes());
		end
	$$;
*/
		
-- Add a job
	--	SELECT add_job('dba_tablesize_job', '5 min', config => '{}'); --
		

-- Then function : 		
	
	CREATE TABLE if not exists public.dbatablesizes (
	"time" timestamptz NOT NULL,
	"typeNr" int4 NULL,
	"type" varchar NULL,
	relscheme varchar NULL,
	relname varchar NULL,
	chunkname varchar NULL,
	viewname varchar NULL,
--	"viewOwner" varchar NULL,
	rowcount float4 NULL,
--	"RngStart" timestamptz NULL,
--	"RngEnd" timestamptz NULL,
	"size" int8 NULL,
--	"sizePretty" text NULL,
	"indexSize" int8 NULL,
--	"indexSizePretty" text NULL,
	totalsize int8 NULL
--	"totalsizePretty" text NULL
);
	--CREATE INDEX dbatablesizes_time_idx ON public.dbatablesizes USING btree ("time" DESC);

	
	--Convert table to hypertable. Do not raise a warning if the table is already a hypertable:
	perform (select create_hypertable('public.dbatablesizes', 'time', chunk_time_interval => INTERVAL '10 day', if_not_exists => true, migrate_data => TRUE));

	
		with alldata as (
			select 
			1 as "typeNr",
			'TABLE' as "type",
			nspname as "relscheme",
			relname as "relname",
			'' as "chunkname",
			'' as "viewname",
--			'' as "viewOwner",
			reltuples as "rowcount",
--			null as "RngStart",
--			null as "RngEnd",
			  pg_relation_size(C.oid) "size"
--			,  pg_size_pretty(pg_relation_size(C.oid)) "sizePretty"
			, pg_indexes_size(C.oid) "indexSize"
--			, pg_size_pretty(pg_indexes_size(C.oid)) "indexSizePretty" 
			, pg_total_relation_size(C.oid) "totalsize"
--			, pg_size_pretty(pg_total_relation_size(C.oid)) "totalsizePretty"
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
--			'' as "viewOwner",
			C.reltuples as "rowcount",
--			ch.range_start as "RngStart",
--			ch.range_end as "RngEnd",
			  pg_relation_size(C.oid) "size"
--			,  pg_size_pretty(pg_relation_size(C.oid)) "sizePretty"
			, pg_indexes_size(C.oid) "indexSize"
--			, pg_size_pretty(pg_indexes_size(C.oid)) "indexSizePretty" 
			, pg_total_relation_size(C.oid) "totalsize"
--			, pg_size_pretty(pg_total_relation_size(C.oid)) "totalsizePretty"
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
--			mv.view_owner as "viewOwner",
			C.reltuples as "rowcount",
--			ch.range_start as "RngStart",
--			ch.range_end as "RngEnd",
			  pg_relation_size(C.oid) "size"
--			,  pg_size_pretty(pg_relation_size(C.oid)) "sizePretty"
			, pg_indexes_size(C.oid) "indexSize"
--			, pg_size_pretty(pg_indexes_size(C.oid)) "indexSizePretty" 
			, pg_total_relation_size(C.oid) "totalsize"
--			, pg_size_pretty(pg_total_relation_size(C.oid)) "totalsizePretty"
			--,* 
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
		 insert into public.dbatablesizes select now() as time, * from alldata;
		--	select * into public.mytable from (select now() as time, * from alldata) as a

		return;
	end;
$function$
;

-- Permissions

ALTER FUNCTION public.pattyn_dba_tablesizes() OWNER TO "PattynAdmin";
GRANT ALL ON FUNCTION public.pattyn_dba_tablesizes() TO "PattynAdmin";

select * from public.pattyn_dba_tablesizes()

select * from public.dbatablesizes M where time in (select last(time,time) from public.dbatablesizes )
order by  relscheme, relname,"typeNr", size desc 

select count(*) from public.dbatablesizes
delete from public.dbatablesizes
call run_job(1000)

select delete_job(1001)
SELECT * FROM timescaledb_information.jobs where job_id =1000;
SELECT * FROM timescaledb_information.job_stats --where job_id = 1000;
