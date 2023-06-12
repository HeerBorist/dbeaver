--- compression for argocheck

select pg_size_pretty(totalsize),* from public.dba_database_details where relname like '%argo%'

select * from ppltesttslite3.argocheck_datas_pbd adp order by time desc limit 100
select pg_total_relation_size('ppltesttslite3.argocheck_datas_pbd') --49152

alter table ppltesttslite3.argocheck_datas_pbd SET(
timescaledb.compress,
timescaledb.compress_segmentby = 'recipenamestr');

select add_compression_policy ('ppltesttslite3.argocheck_datas_pbd', interval '1 days'); --1129
select * from chunk_compression_stats('ppltesttslite3.argocheck_datas_pbd')


-- at start :3.688 gb
-- after compression : 

select * from timescaledb_information.jobs
select * from timescaledb_information.compression_settings
select * from chunk_compression_stats('ppltesttslite3.argocheck_datas_pbd')

--- compression for monitoring

select pg_size_pretty(totalsize),* from public.dba_database_details where relname like '%monit%'

select * from testplctimescale.monitoring order by time desc limit 100

alter table testplctimescale.monitoring SET(
timescaledb.compress),
timescaledb.compress_segmentby = 'loggeridentifier');


select add_compression_policy ('testplctimescale.monitoring', interval '1 days'); --1126

select * from chunk_compression_stats('ppltesttslite2.monitoring')

-- at start : 4.831 Gb
-- after compression : 

--- compression for objects

select pg_size_pretty(totalsize),* from public.dba_database_details where relname like '%objects%'

select * from ppltesttslite3.objects order by time desc limit 100

alter table ppltesttslite4.argocheck_datas_pbd SET(timescaledb.compress)
select add_compression_policy ('ppltesttslite4.argocheck_datas_pbd', interval '1 days'); --1182
select * from chunk_compression_stats('ppltesttslite4.argocheck_datas_pbd')

--alter table ppltesttslite2.monitoring SET(
--timescaledb.compress,
--timescaledb.compress_segmentby = 'loggeridentifier');

select add_compression_policy ('ppltesttslite4.objects', interval '1 days'); --1128

select * from chunk_compression_stats('ppltesttslite4.objects')

-- at start : 5.177 gb
-- after compression : 

--- compression for robot_datas

select pg_size_pretty(totalsize),* from public.dba_database_details where relname like '%robot%'

select * from ppltesttslite3.robot_datas_pbd order by time desc limit 100
select pg_total_relation_size('ppltesttslite3.robot_datas_pbd') --49152

alter table ppltesttslite3.robot_datas_pbd SET(
timescaledb.compress,
timescaledb.compress_segmentby = 'recipenamestr');

select add_compression_policy ('ppltesttslite3.robot_datas_pbd', interval '2 days'); --1129

-- at start :3.688 gb
-- after compression : 

select * from timescaledb_information.jobs
select * from timescaledb_information.compression_settings
select * from chunk_compression_stats('ppltesttslite3.robot_datas_pbd')


--- compression for robot_datas_general


select pg_size_pretty(totalsize),* from public.dba_database_details where relname like '%robot%'

select * from ppltesttslite3.robot_datas_pbd order by time desc limit 100
select pg_total_relation_size('ppltesttslite3.robot_general_datas_pbd') --49152

alter table ppltesttslite3.robot_general_datas_pbd SET(
timescaledb.compress,
timescaledb.compress_segmentby = 'recipenamestr');

select add_compression_policy ('ppltesttslite3.robot_general_datas_pbd', interval '2 days'); --1129

-- at start :3.688 gb
-- after compression : 


--- compression for robot_datas_general


---losse testen...

select pg_size_pretty(totalsize),* from public.dba_database_details where relname like '%check%'

select * from ppltesttslite2.checkweigher_datas_pbd cdp  order by time desc limit 100
select pg_total_relation_size('ppltesttslite3.robot_general_datas_pbd') --49152

alter table ppltesttslite2.checkweigher_datas_pbd SET(
timescaledb.compress),
timescaledb.compress_segmentby = 'recipenamestr');

select add_compression_policy ('ppltesttslite2.checkweigher_datas_pbd', interval '1 days'); --1129



select * from timescaledb_information.jobs
select * from timescaledb_information.compression_settings
select * from chunk_compression_stats('ppltesttslite2.checkweigher_datas_pbd')


select * from timescaledb_information.jobs
select * from timescaledb_information.compression_settings
select * from chunk_compression_stats('ppltesttslite2.monitoring')

select pg_size_pretty(totalsize),* from public.dba_database_details where relname like '%monit%'

select * from timescaledb_information.compression_settings
select * from chunk_compression_stats('ppltesttslite3.argocheck_datas_pbd') ==> job 1129
ALTER TABLE ppltesttslite3.argocheck_datas_pbd ADD testcolumn varchar(50) NULL;
--SQL Error [0A000]: ERROR: cannot add column with constraints or defaults to a hypertable that has compression enabled

--find the job
SELECT s.job_id, *
FROM timescaledb_information.jobs j
  INNER JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
WHERE j.proc_name = 'policy_compression' --AND s.hypertable_name = 'objects'; --1110 ?

SELECT alter_job(1129, scheduled => false);
--(1129,12:00:00,00:00:00,-1,01:00:00,f,"{""hypertable_id"": 100, ""compress_after"": ""2 days""}","2021-05-05 18:07:49.183832+02")
ALTER TABLE ppltesttslite3.argocheck_datas_pbd ADD testcolumn varchar(50) NULL;

ALTER TABLE ppltesttslite3.argocheck_datas_pbd set (timescaledb.compress = false);
	--SQL Error [0A000]: ERROR: cannot change configuration on already compressed chunks
	--  Detail: There are compressed chunks that prevent changing the existing compression configuration.


--- try to put compression on a materialized view : 
select * from timescaledb_information.continuous_aggregates ca 
--ppltesttslite2._materialized_hypertable_107  weightdata_v2_hourly


select * from ppltesttslite2._materialized_hypertable_107 order by time desc limit 100
select pg_total_relation_size('ppltesttslite3.robot_general_datas_pbd') --49152

alter table _timescaledb_internal._materialized_hypertable_107 SET(
timescaledb.compress)

select add_compression_policy ('ppltesttslite3.robot_general_datas_pbd', interval '2 days'); --1129



/*
 CHECK COMPRESSION
select * from timescaledb_information.jobs
select * from timescaledb_information.compression_settings

select show_chunks('ppltesttslite3.objects')
select * from chunk_compression_stats('ppltesttslite3.objects')

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

*/
select  count(*), first(time,time), last(time,time) from ppltesttslite4.software_pbd  order by time desc
select * from public.dba_database_details ddd 
select * from ppltesttslite4.weightdata w order by time desc
select * from ppltesttslite4.argocheck_datas_pbd adp  order by time desc
select extract(epoch from time)*1000 as milliseconds, * from ppltesttslite4.software_pbd_bug  order by time desc limit 1000--milliseconds


-- Dieter dashboard check

SELECT
  RecipeLoadCounter as "Id", 
  mode() WITHIN GROUP (ORDER BY Recipe) as "Recipe",
  extract(epoch from min(bucketfirsttime))*1000 as "Start",
  extract(epoch from max(bucketlasttime))*1000 as "End",
  (extract(epoch from max(bucketlasttime)) - extract(epoch from min(bucketfirsttime))) as "Duration", 
  -- array_agg(distinct(setpoint)) as setpoint -- in case want to visualize if setpoint changed 
  -- mode() WITHIN GROUP (ORDER BY setpoint) as "Target weight", -- show target weight? 
  sum(totalweight)/1000 as "Total weight",
  (sum(totalweight)*3.6)/(extract(epoch from max(bucketlasttime))-extract(epoch from min(bucketfirsttime))) as "Speed", 
  -- could use fillingfirsttime and fillinglasttime instead, than performance will be higher
  sum(nrofboxes) as "Total boxes",
  avg(stdevboxweight)*1000 as "Standard deviation"
FROM ppltesttslite4.weightdata_hourly --$WeightTable
--WHERE 
--  recipe in ($Recipe) and 
--  RecipeLoadCounter in ($RecipeIdx)
GROUP BY RecipeLoadCounter
ORDER BY RecipeLoadCounter desc
limit 50

select * from ppltesttslite4.weightdata_hourly  where recipeloadcounter = '54'

select * from ppltesttslite4.weightdata  where recipeloadcounter = '61' order by time asc --2021-05-04 07:34:00 tot 2021-05-04 08:57:38

select * from ppltesttslite4.weightdata where time > ('2021-05-04 23:30:58'::timestamp - interval '1 min') and time < ('2021-05-04 23:30:58'::timestamp + interval '1 min') order by time
select * from ppltesttslite4.weightdata where time > ('2021-05-05 04:07:39'::timestamp - interval '1 min') and time < ('2021-05-05 04:07:39'::timestamp + interval '1 min') order by time

select * from ppltesttslite4.weightdata where time >= ('2021-05-04 23:30:58') and time <= ('2021-05-05 04:07:39') order by time
select * from ppltesttslite4.weightdata where time >= ('2021-05-04 23:30:58') and time <= ('2021-05-05 04:07:39') order by time desc

select * from ppltesttslite4.weightdata_hourly  where recipeloadcounter = '61' order by bucketfirsttime asc --2021-05-04 23:30:58 ==> te vroeg ?? 
select * from ppltesttslite4.weightdata_hourly  where recipeloadcounter = '61' order by bucketlasttime desc --2021-05-05 04:07:39 ==> exact

select * from ppltesttslite4.weightdata where time >= ('2021-05-04 23:33:35') and time <= ('2021-05-05 04:07:33') order by time
select * from ppltesttslite4.weightdata where time >= ('2021-05-04 23:33:35') and time <= ('2021-05-05 04:07:39') order by time desc

select * from ppltesttslite4.weightdata_hourly  where recipeloadcounter = '61' order by fillingfirsttime asc --2021-05-04 23:33:35 => beetje later
select * from ppltesttslite4.weightdata_hourly  where recipeloadcounter = '61' order by fillinglasttime desc --2021-05-05 04:07:33 ==> beetje te vroeg ?? -- werkt op fnetweight ? 




where recipeloadcounter = '54'

select * from timescaledb_information.jobs j where hypertable_name like '%weigh%' and hypertable_schema like '%4%'
select view_definition, * from timescaledb_information.continuous_aggregates MV


 SELECT weightdata.loggeractivationuid,
    weightdata.machine,
    weightdata.machineserial,
    weightdata.recipe,
    weightdata.recipeloadcounter,
    weightdata.recipientweight_imoduleindex,
    weightdata.recipientweight_eweightresult,
    (weightdata.recipientweight_istructversion)::integer AS recipientweight_istructversion,
    time_bucket('01:00:00'::interval, weightdata."time") AS bucket,
    count(weightdata.recipientweight_eweightresult) AS nrofweightresult,
    max(weightdata.recipientweight_fsetpoint) AS setpoint,
    max(weightdata.recipientweight_fmaxoverweight) AS maxoverweight,
    max(weightdata.recipientweight_fmaxunderweight) AS maxunderweight,
    count(weightdata.recipientweight_fnetweight) AS nrofboxes,
    sum(weightdata.recipientweight_fnetweight) AS totalweight,
    min(weightdata.recipientweight_fnetweight) AS minboxweight,
    max(weightdata.recipientweight_fnetweight) AS maxboxweight,
    avg(weightdata.recipientweight_fnetweight) AS avgboxweight,
    stddev(weightdata.recipientweight_fnetweight) AS stdevboxweight,
    first(weightdata."time", weightdata.recipientweight_fnetweight) AS fillingfirsttime,
    last(weightdata."time", weightdata.recipientweight_fnetweight) AS fillinglasttime,
    first(weightdata."time", weightdata."time") AS bucketfirsttime,
    last(weightdata."time", weightdata."time") AS bucketlasttime,
    avg(weightdata.recipientweight_ftareweight) AS avgtareweight,
    max((weightdata.recipientweight_smodulekey)::text) AS skey
   FROM ppltesttslite4.weightdata
  WHERE (weightdata.recipientweight_fnetweight IS NOT NULL)
  GROUP BY weightdata.recipientweight_istructversion, weightdata.machine, 
 weightdata.machineserial, weightdata.loggeractivationuid, 
weightdata.recipe, weightdata.recipeloadcounter, 
weightdata.recipientweight_imoduleindex, 
weightdata.recipientweight_eweightresult, 
(time_bucket('01:00:00'::interval, weightdata."time"));
