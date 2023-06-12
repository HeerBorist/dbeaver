
--https://www.postgresqltutorial.com/postgresql-database-indexes-table-size/

-- https://docs.timescale.com/latest/api : how to set chunk and chunk sizes
-- https://docs.timescale.com/latest/api#chunks_detailed_size
-- select the size of column


select * from timescaledb_information.chunks --alle hypertable_schema details voor deze server ?
select * from timescaledb_information.hypertables -- toont geen materialized views !


select * from timescaledb_information.chunks order by hypertable_schema , hypertable_name , chunk_name , range_start 

select * from timescaledb_information.chunks 
where chunk_name like '%52_305%'
order by hypertable_schema , hypertable_name , chunk_name , range_start

--test

-- https://github.com/timescale/timescaledb/issues/1676
select oid,  relname "tableName", reltuples "rowsCount" 
, pg_size_pretty(pg_relation_size(oid)) "size"
, pg_indexes_size(oid) "indexSize"
, pg_size_pretty(pg_indexes_size(oid)) "indexSizePretty" 
from pg_class where relkind = 'r' 
and relnamespace = (select oid from pg_namespace where nspname='_timescaledb_internal') 
order by pg_relation_size(oid) desc;


select * from pg_class 

select * from timescaledb_information.hypertables
select * from timescaledb_information.chunks
select * from pg_class where relname in (select hypertable_name from timescaledb_information.hypertables)



--_hyper_52_305_chunk
--
select * from chunks_detailed_size('monitoring')
select * from chunks_detailed_size('testplctimescale.monitoring')

select * from chunks_detailed_size('_timescaledb_internal._hyper_45_114_chunk')

select * from hypertable_size('testplctimescale.weightdata_v1')
select * from hypertable_detailed_size('testplctimescale.weightdata_v1')

select * from pg_relation_size('monitoring')

table bytes : 
select * from pg_relation_size('monitoring')

--total size of a hypertable : including indexes and toast data
select * from hypertable_size('monitoring')
select * from hypertable_detailed_size('monitoring')
--select * from hypertable_index_size('monitoring')

select * from show_chunks ('testplctimescale.xmlobject') 


-- get size information for all hypertables
SELECT hypertable_schema , hypertable_name, owner, num_chunks 
,pg_size_pretty (hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass)) as humansize
,hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass) as totalsize
  FROM timescaledb_information.hypertables
  order by totalsize desc
  
-- select hypertable_index_size () 

select pg_column_size(a)
from (select * from ppltesttslite.objects)
s(a) 

-- table size
select pg_relation_size('objects');

--------------------------------------------------------
select nspname,  N.oid as "NSPoid", C.oid as "ClassOID",
    relname AS "relation",
    pg_size_pretty (
        pg_total_relation_size (C .oid)
    ) AS "total_size"
FROM
    pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace)
WHERE
    nspname NOT IN (
        'pg_catalog',
        'information_schema'
    )
AND C .relkind <> 'i'
AND nspname !~ '^pg_toast'
ORDER BY
    pg_total_relation_size (C .oid) DESC
--LIMIT 20;

--------------------------------------------------------

-- Zoek alle schema's 
select distinct nspname from pg_namespace 
WHERE
    nspname NOT IN (
        'pg_catalog',
        'information_schema'
    )
order by 1


select * from pg_namespace
select * from pg_class

select nspname,* from pg_class C left join pg_namespace N on (N."oid" = C.relnamespace)
order by  N.nspname 


SELECT
    pg_size_pretty (
        pg_database_size ('Pattyn')
    );

SELECT
    pg_size_pretty (
        pg_tablespace_size ('pg_default')
    );
 
-- https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-DBOBJECT
-- https://www.postgresqltutorial.com/postgresql-database-indexes-table-size/

select pg_column_size() from public.example where line like 'real_%' order by id asc
select pg_total_relation_size() from public.example where line like 'real_%' order by id asc-- including indexes !!, in bytes
select pg_size_pretty (pg_column_size('public.example.rreal')) -- 
select pg_size_pretty (pg_relation_size('public.example')) --table only
select pg_size_pretty (pg_total_relation_size('public.example')) -- table including indexes or additional objects

-- column size ? hoe werkt deze functie ??
select pg_column_size(a)
from (select iint8 from public.example)
s(a) 

--******************************************************************************************************

--- database evaluation
SELECT
    pg_database.datname,
    pg_database_size(pg_database.datname) AS bytes,
    pg_size_pretty(pg_database_size(pg_database.datname)) AS hsize
    FROM pg_database
   order by bytes desc;

--select pg_size_pretty (pg_database_size ('Pattyn'));

  
  -- get size information for all hypertables
SELECT hypertable_schema , hypertable_name, owner, num_chunks 
,pg_size_pretty (hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass)) as humansize
,hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass) as totalsize
  FROM timescaledb_information.hypertables
  order by hypertable_schema , hypertable_name ,totalsize desc

 
  select * from pg_namespace --oid 19937 : testplctimescale
select * from pg_class where relkind = 'r' 
select * from pg_class where relname like '%hour%' -- relkind = v (view)
--_materialized_hypertable_48


-- op zoek naar de id's ??
select * from _timescaledb_catalog.hypertable where id=27
select * from _timescaledb_catalog.chunk where hypertable_id = 27 --dropped !!
select * from _timescaledb_catalog.continuous_agg where raw_hypertable_id = 27 --mat_hypertable_id = 48


-- Overzicht van tabels, chunks en materialized views...
select H.ID, H.schema_name , H.table_name , h.associated_schema_name , h.associated_table_prefix ,
C.id "ChunkId", C.hypertable_id "ChunkHyperTableId", c.table_name "ChunkTableName", c.dropped "ChunkDropped", 
M.user_view_name "MVuserviewname", M.user_view_schema "MVuserviewschema" , M.bucket_width --,* 
from _timescaledb_catalog.hypertable H 
left outer join _timescaledb_catalog.chunk C on H.id = C.hypertable_id 
left outer join _timescaledb_catalog.continuous_agg M on H.id = M.raw_hypertable_id 
where H.id=71

select * from pg_class 
select * from _timescaledb_catalog.hypertable H 



select * FROM timescaledb_information.continuous_aggregates;
select * FROM _timescaledb_catalog.continuous_agg M 

--*************Dit wordt hem....
-- https://github.com/timescale/timescaledb/issues/1676
select C.oid,  C.relname "tableName", c.reltuples "rowsCount" 
, pg_size_pretty(pg_relation_size(C.oid)) "size"
, pg_indexes_size(C.oid) "indexSize"
, pg_size_pretty(pg_indexes_size(C.oid)) "indexSizePretty" 
, pg_total_relation_size(C.oid) "totalsize"
, pg_size_pretty(pg_total_relation_size(C.oid)) "totalsizePretty"
from pg_class C 
where C.relkind = 'r'
and C.relnamespace = (select oid from pg_namespace where nspname='_timescaledb_internal')
order by pg_relation_size(C.oid) desc;

--*************

/*

--************* alles samen, nu nog zien hoe we dit in een narrow table kunnen verwerken ? 
select 
  ch.hypertable_schema as "ChHyperSchema" ,CH.hypertable_name as "ChHyperTblName" ,N.nspname as "NameSpace"  , C.relname "tableName", 
MV.hypertable_name, mv.view_schema,mv.view_name,mv.view_owner,--mv.materialization_hypertable_name,

N."oid" as "NspOID", C.oid as "ClassOID",   c.reltuples "rowsCount" ,  ch.range_start, ch.range_end ,
--CH.*
  pg_relation_size(C.oid) "size"
,  pg_size_pretty(pg_relation_size(C.oid)) "sizePretty"
, pg_indexes_size(C.oid) "indexSize"
, pg_size_pretty(pg_indexes_size(C.oid)) "indexSizePretty" 
, pg_total_relation_size(C.oid) "totalsize"
, pg_size_pretty(pg_total_relation_size(C.oid)) "totalsizePretty"
from pg_class C 
left join timescaledb_information.chunks CH on C.relname = CH.chunk_name 
left join timescaledb_information.continuous_aggregates MV on (CH.hypertable_name = MV.materialization_hypertable_name and CH.hypertable_schema = MV.materialization_hypertable_schema )
left join pg_namespace N on C.relnamespace = N.oid 
where C.relkind = 'r'
and C.relnamespace in (select oid from pg_namespace where nspname NOT IN (
        'pg_catalog',
        'information_schema',
        '_timescaledb_catalog',
        '_timescaledb_config',
        'timescaledb_information',
        '_timescaledb_cache')
    )
order by  ch.hypertable_schema,CH.hypertable_name, N.nspname ,C.relname, pg_relation_size(C.oid) desc;  --86

*/
 
 
 
-- hypertable schema, view name, view owner, hypertable_name, view_schema
select MV.* FROM timescaledb_information.continuous_aggregates MV
left join timescaledb_information.chunks CH on (CH.hypertable_name = MV.materialization_hypertable_name and CH.hypertable_schema = MV.materialization_hypertable_schema )
;


select * from pg_namespace -- oid ==> nspname
select * from pg_class C where relname like '%weight%' and relkind = 'r'
select * from timescaledb_information.chunks --alle hypertable_schema details voor deze server ?
select * from timescaledb_information.hypertables -- toont geen materialized views !
select * FROM timescaledb_information.continuous_aggregates;
select * FROM _timescaledb_catalog.continuous_agg M 

select * from timescaledb_information.chunks where hypertable_name like '%material%' --hypertable_name



select * from _timescaledb_catalog.chunk where hypertable_id = 27 --dropped !!
select * from show_chunks ('ppltesttslite.objects')

select nspname,
    relname AS "relation",
    pg_size_pretty (
        pg_total_relation_size (C .oid)
    ) AS "total_size"
FROM
    pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace)
WHERE
    nspname NOT IN (
        'pg_catalog',
        'information_schema',
        '_timescaledb_catalog',
        '_timescaledb_config',
        'timescaledb_information',
        '_timescaledb_cache'
    )
AND C .relkind <> 'i'
AND nspname !~ '^pg_toast'
ORDER BY
    pg_total_relation_size (C .oid) DESC
LIMIT 20;

--*************


