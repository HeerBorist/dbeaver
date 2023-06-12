/*
CREATE OR REPLACE FUNCTION fn_grant_all_views(schema_name TEXT, role_name TEXT)
RETURNS VOID AS $func$

DECLARE view_name TEXT;

BEGIN

  FOR view_name IN
    SELECT viewname FROM pg_views WHERE schemaname = schema_name
  LOOP
    EXECUTE 'GRANT ALL PRIVILEGES ON ' || schema_name || '.' || view_name || ' TO ' || role_name || ';';
  END LOOP;

END; $func$ LANGUAGE PLPGSQL

SELECT fn_grant_all_views('public','PattynAdmin');
grant all PRIVILEGES on public.dba_table_details to "PattynAdmin"
*/


-- SHOW DATABASE SHEMA's whithout using distinc
  create or replace view public.dba_show_shemes as   
select s.nspname as table_schema,
       s.oid as schema_id,
       u.usename as owner
from pg_catalog.pg_namespace s
join pg_catalog.pg_user u on u.usesysid = s.nspowner
where (nspname not in ('information_schema', 'pg_catalog', 'timescaledb_information' ) and nspname not like '%_timescale_%')
      and nspname not like 'pg_toast%'
      and nspname not like 'pg_temp_%'
order by table_schema;

select table_schema  from dba_show_shemes 

ALTER TABLE public.dba_show_shemes OWNER TO root;
GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE public.dba_show_shemes TO root;
GRANT SELECT ON TABLE public.dba_show_shemes TO aaksweden;
GRANT SELECT ON TABLE public.dba_show_shemes TO aaksweden_grafana;
GRANT SELECT ON TABLE public.dba_show_shemes TO bungeloders;
GRANT SELECT ON TABLE public.dba_show_shemes TO bungeloders_grafana;



	-- tables
--	drop view public.dba_table_details
	create or replace view public.dba_table_details as
			select 
			nspname as "relscheme",
			relname as "relname",
			reltuples as "rowcount",
			  pg_relation_size(C.oid) "size"
			, pg_indexes_size(C.oid) "indexsize"
			, pg_total_relation_size(C.oid) "totalsize"
			--,*
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
			    order by 1,2
			    
--grant all PRIVILEGES on public.dba_table_details to "PattynAdmin"
--select * from public.dba_table_details 

--GRANT ALL ON SCHEMA bungelodersnlts TO "PattynAdmin";
--GRANT ALL ON SCHEMA aakswetimescale TO "PattynAdmin";

-- retention policies
-- drop view public.dba_retention_details
	create or replace view public.dba_retention_summary as
			    select 
			DT.relscheme as "relscheme"
			,DT.relname as "relname"
			, J.config 
			, J.proc_name 
			, J.retry_period 
			, J.next_start 
			, J.schedule_interval 
			, J.scheduled 
--			from pg_class C
--			left join pg_namespace N on C.relnamespace = N.oid
			from public.dba_table_details DT
			left join timescaledb_information.jobs J on (DT.relscheme = J.hypertable_schema and DT.relname = J.hypertable_name )
			where J.proc_name like '%retention%'
			union 
			    select 
			MV.relscheme as "relscheme"
			,MV.mv_hypertable_name as "relname"
			, J.config
			, J.proc_name 
			, J.retry_period 
			, J.next_start 
			, J.schedule_interval 
			, J.scheduled 
--			from pg_class C
--			left join pg_namespace N on C.relnamespace = N.oid
			from public.dba_materialized_view_summary MV
			left join timescaledb_information.jobs J on (MV.mv_hypertable_name = J.hypertable_name )
--			where C.relkind = 'r'
			where J.proc_name like '%retention%'
			
--			and C.relnamespace in (select oid from pg_namespace where nspname NOT IN (
--			        'pg_catalog',
--			        'information_schema',
--			        '_timescaledb_catalog',
--			        '_timescaledb_config',
--			        'timescaledb_information',
--			        '_timescaledb_cache',
--			        '_timescaledb_internal')
--			    )

-- select * from public.dba_retention_summary			    

			    select * from timescaledb_information.jobs J -- add materialized views !! hypertable_name
			    select * from public.dba_table_details
				select * from public.dba_materialized_view_summary --mv_hypertable_name  _materialized_hypertable_107
			    
			    
-- compression -- wat als compression op materialized view ?? is dit zichtbaar ? ==> compression op MatView is niet mogelijk ?
-- drop view public.dba_compression_summar
   	create or replace view public.dba_compression_summary as
			    select 
			nspname as "relscheme"
			,relname as "relname"
			, J.config
			, J.proc_name 
			, J.retry_period 
			, J.next_start 
			, J.schedule_interval 
			, J.scheduled 
			from pg_class C
			left join pg_namespace N on C.relnamespace = N.oid
			left join timescaledb_information.jobs J on (nspname = J.hypertable_schema and relname = J.hypertable_name )
			where C.relkind = 'r'
			and J.proc_name like '%compression%'
			and C.relnamespace in (select oid from pg_namespace where nspname NOT IN (
			        'pg_catalog',
			        'information_schema',
			        '_timescaledb_catalog',
			        '_timescaledb_config',
			        'timescaledb_information',
			        '_timescaledb_cache',
			        '_timescaledb_internal')
			    )
			    
-- select * from public.dba_compression_summary
			    
-- chunks (add commpressed chunks ??)
			    
--Step 2 : chunks details (add commpressed chunks ??) -- zonder materialized view chunks !!
-- drop view public.dba_chunk_details
	create or replace view public.dba_chunk_details as
			select 
			ch.hypertable_schema as "relscheme", --1
			CH.hypertable_name as "relname", --2
			C.relname as "chunks",
			(C.reltuples) as "rowcount",
			(ch.range_start) as "chunk_rngstart",
			(ch.range_end) as "chunk_rngend",
			(age( ch.range_end, ch.range_start)) as "chunkinterval",
			(  pg_relation_size(C.oid)) "size"
			,(pg_indexes_size(C.oid)) "indexsize"
			,(pg_total_relation_size(C.oid)) "totalsize"
			--,* 
			from pg_class C
			left join timescaledb_information.chunks CH on C.relname = CH.chunk_name
--			join lateral (select * from chunk_compression_stats(ch.hypertable_schema ||'.'||ch.hypertable_name ) as Chcomp on chunkschema ??-- 'ppltesttslite2.objects')
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
			order by 1,2,3

-- select * from public.dba_chunk_details
select * from public.dba_chunk_details where relscheme like '%4%' 

			    
--Step 2 : chunks summarized -- add commpressed chunks ?
-- drop view public.dba_chunk_summary
	create or replace view public.dba_chunk_summary as			
			select 
			ch.hypertable_schema as "relscheme", --1
			CH.hypertable_name as "relname", --2
			count(C.relname) as "chunks",
			sum(C.reltuples) as "rowcount",
			min(ch.range_start) as "chunk_rngstart",
			max(ch.range_end) as "chunk_rngend",
			age(max(ch.range_end),min(ch.range_start)) as "datatimespan",
			max(age( ch.range_end, ch.range_start)) as "chunkinterval",
			sum(  pg_relation_size(C.oid)) "size"
			, sum(pg_indexes_size(C.oid)) "indexsize"
			, sum(pg_total_relation_size(C.oid)) "totalsize"
--			,* 
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
			group by 1,2
			
			-- select * from public.dba_chunk_summary

--Step 3 : materialized views : can be more than 1 per hypertable
-- drop view public.dba_materialized_view_details
	create or replace view public.dba_materialized_view_details as

	select relscheme, relname,chunks ,mv_hypertable_name,viewname,viewowner,rowcount, rngstart,rngend,size,indexsize,totalsize,
	config, proc_name,retry_period,next_start,schedule_interval,scheduled  
			from -- FIRST GET THE INFORMATION ON MATERIALIZED VIEWS and underlying chunks (if any)
			(select 
			MV.view_schema as "relscheme",
			MV.hypertable_name as "relname",
			ch.chunk_name as "chunks",
			ch.hypertable_name as "mv_hypertable_name",
			MV.view_name as "viewname",
			MV.view_owner as "viewowner",
			ch.range_start as "rngstart",
			ch.range_end as "rngend"
			, J.config
			, J.proc_name 
			, J.retry_period 
			, J.next_start 
			, J.schedule_interval 
			, J.scheduled 
			from timescaledb_information.continuous_aggregates MV
			left join pg_class C on C.relname = MV.view_name 
			left join timescaledb_information.chunks CH on MV.materialization_hypertable_name = CH.hypertable_name
			left join timescaledb_information.jobs J on MV.materialization_hypertable_name = J.hypertable_name
			where c.relkind = 'v'
--			and j.proc_name like '%continuous_aggregate%'
--			and C.relnamespace in (select oid from pg_namespace where nspname NOT IN (
--			        'pg_catalog',
--			        'information_schema',
--			        '_timescaledb_catalog',
--			        '_timescaledb_config',
--			        'timescaledb_information',
--			        '_timescaledb_cache')
--			    )
			) t1
			left join lateral -- GET THE UNDERLYING CHUNK-relation oid to calculate tuples and size from previous selected chunks
			(select-- c.oid "newoid", 
				C.reltuples as "rowcount"
				, pg_relation_size(C.oid) "size"
				, pg_indexes_size(C.oid) "indexsize"
				, pg_total_relation_size(C.oid) "totalsize"
				from pg_class C 
				where C.relname = t1.chunks ) LJ on true --C.relname = CH.chunk_name
			order by 1,2,4,3

			
--select * from public.dba_materialized_view_details
			
--Step 3 : materialized views summary
-- drop view public.dba_materialized_view_summary
	create or replace view public.dba_materialized_view_summary as
			select 
			relscheme 
			,relname 
			,mv_hypertable_name
			,count(chunks) as "chunks" 
			,min(viewname) as "viewname"
			,min(viewowner) as "viewowner"
			,sum(rowcount) as "rowcount" 
			,min(rngstart) as "chunk_rngstart" 
			,max(rngend) as "chunk_rngend" 
			,age(max(rngend),min(rngstart)) as "datatimespan"
			,max(age(rngend,rngstart)) as "chunkinterval"
			,sum(size) as "size"
			,sum(indexsize) as "indexsize"
			,sum(totalsize) as "totalsize"
			, min(config::text) as "config"
--			, min(J.proc_name )
--			, min(J.retry_period) 
--			, J.next_start 
			,max(schedule_interval) as "schedule_interval" 
			, scheduled 
--			,* 
			from public.dba_materialized_view_details
			 group by 1,2, viewname,scheduled,mv_hypertable_name
			order by 1,2,3
	
			-- select * from public.dba_materialized_view_summary
			
			

--select * from timescaledb_information.continuous_aggregates -- 
--select * from timescaledb_information.jobs J where j.proc_name = 'policy_refresh_continuous_aggregate'
-- 

-- All Tables Overview (Summary) -- treat MatView as separate table !!

-- drop view public.dba_database_details
create or replace view public.dba_database_details as

select -- TABLE SUMMARY + chunks + retention
1::int as "tbltypenr"
,'Table' as "tbltype"
,relscheme
,relname
,coalesce (dtd.totalsize::int8, 0) + coalesce(dcs.totalsize::int8, 0)   as totalsize
,coalesce (dtd.indexsize::int8, 0) + coalesce(dcs.indexsize::int8, 0)  as indexsize
,coalesce (dtd.size::int8, 0) + coalesce(dcs.size::int8, 0)   as tablesize
,coalesce (dtd.rowcount::int8, 0) + coalesce(dcs.rowcount::int8, 0)   as records
,dcs.chunks as chunks_amount
,dcs.chunkinterval as chunkinterval
,dcs.chunk_rngstart as chunkstart
,dcs.chunk_rngend as chunkend
,dcs.datatimespan as dataperiod
,'false' as "MVscheduled" --,coalesce(dmvs.scheduled::bool, false ) as matvw_active
,'' as "viewname" --,dmvs.viewname
,'' as "viewowner" --,dmvs.viewowner
--,drs.config as "retentionconfig"
,drs.config ->> 'drop_after' as "retention" 
,drs.scheduled as "RETscheduled"
,drs.schedule_interval as "ret_interval"
,dcos.config ->> 'compress_after' as "compress_after"
,dcos.scheduled as "comp_scheduled"
from dba_table_details dtd 
left join dba_chunk_summary dcs using (relscheme,relname)
--left join dba_materialized_view_summary dmvs using (relscheme,relname)
left join  dba_retention_summary drs  using (relscheme,relname)
left join  public.dba_compression_summary dcos  using (relscheme,relname)
--order by relscheme,relname,1
union 
 select -- MATERIALIZED VIEW SUMMARY + chunks + retention
'2' as "tbltypenr"
,'Materialized View' as "tbltype"
,dmvs.relscheme
,dmvs.relname
, coalesce(dmvs.totalsize::int8,0)  as totalsize
, coalesce(dmvs.indexsize::int8,0)  as indexsize
, coalesce(dmvs.size::int8,0)  as tablesize
, coalesce(dmvs.rowcount::int8,0)  as records
,dmvs.chunks as chunks_amount
,dmvs.chunkinterval as chunkinterval
,dmvs.chunk_rngstart as chunkstart
,dmvs.chunk_rngend as chunkend
,dmvs.datatimespan as dataperiod
,coalesce(dmvs.scheduled::bool, false ) as "MVscheduled"
--,dmvs.chunks as "chunks_amount"
,dmvs.viewname
,dmvs.viewowner
--,drs.config as "retentionconfig"
,drs.config ->> 'drop_after' as "retention" --kijkt nog naar de tabel, en niet naar de materialized view !!
,drs.scheduled as "RETscheduled"
,drs.schedule_interval as "ret_interval"
,'' as "compress_after"
,false as "comp_scheduled"
from dba_materialized_view_summary dmvs --using (relscheme,relname)
left join  dba_retention_summary drs  on (drs.relscheme = dmvs.relscheme and drs.relname=dmvs.mv_hypertable_name)
--left join  public.dba_compression_summary dcos  using (relscheme,relname) -- no compression possible on 
order by 3,4,1

-- select * from public.dba_database_details where relscheme like '%tslite3%'
-- select * from public.dba_database_details where relscheme like '%testplctime%'


select * from dba_table_details dtd 
analyze  
select * from public.dba_database_details where relscheme like '%tslite3%'




--******************** VERGELIJKING ******************************
-- test of er verschil is met oorspronkelijke job creatie : 

select * from pg_namespace

select
  pg_database_size ('Pattyn') "database",
    pg_size_pretty (
        pg_database_size ('Pattyn') 
    ) as total_Pattyn_database_size;
--17222930991
   
select * from public.dba_database_details where relscheme like '%tslite3%'

-- get the latest resultset table : 
select * from public.dbatablesizes M where time in (select last(time,time) from public.dbatablesizes )
order by  relscheme, relname,"typeNr", size desc

-- per schema
select  relscheme, type, sum(totalsize) "total", pg_size_pretty(sum(totalsize)) "prettytotal" from public.dbatablesizes M where time in (select last(time,time) from public.dbatablesizes )
group by  relscheme ,type
order by  1,2

-- totale database : 
select sum(totalsize) "total", pg_size_pretty(sum(totalsize)) "prettytotal" from public.dbatablesizes M where time in (select last(time,time) from public.dbatablesizes )

--17199628288 ==> toont beetje minder ??

--- via views : 
select * from public.dba_database_details

select  sum(totalsize) "total", pg_size_pretty(sum(totalsize)) "prettytotal"  from public.dba_database_details

--17209450496

--******************** EINDE VERGELIJKING ******************************


