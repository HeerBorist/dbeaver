select * from generate_series(1,20)
select * from generate_series(1,20, .1)

select generate_series * random() from (select * from generate_series(1,20)) myseries
select (generate_series * random()*10^15)::varchar(25) from (select * from generate_series(1,20)) myseries

with numbers as (select * from generate_series(1,20))
select generate_series * random() from numbers


SELECT date_trunc('day', dd):: date
FROM generate_series
        ( '2021-01-01'::timestamp 
        , '2021-05-01'::timestamp
        , '1 day'::interval) dd
        ;
        
SELECT *,date_trunc('day', dd):: date
FROM generate_series
        ( '2021-01-01'::timestamp 
        , now()
        , '1 hour'::interval) dd
        ;
  
       
SELECT t.day::date 
FROM   generate_series(timestamp '2004-03-07'
                     , timestamp '2004-08-16'
                     , interval  '1 day') AS t(day);      
                    

SELECT generate_series(timestamp '2021-03-07', now(), '1 day')::date AS day;
SELECT generate_series(timestamp '2021-03-07', now(), '1 hour')::time AS hour;
SELECT generate_series(timestamp '2021-03-07', now(), '1 hour')::timestamp AS hour;
SELECT generate_series(timestamp 'now()' - interval '6 hour', now(), '1 hour')::timestamp AS hour;
SELECT generate_series(timestamp 'now()' - interval '1 hour', now(), '1 second')::timestamp AS seconds;


--weekdays ? 
select date_trunc('week', )



select timestamp '2021-03-07' + interval '3 hour'
select timestamp 'now()' + interval '3 hour'

select current_date::timestamp + interval '3 day'
select current_date::timestamp

-- stappen van 6 uur (24*.25) voor de volgende 
select (current_date::timestamp + ((a-1)||' days')::interval)::timestamptz
from generate_series(1, 3, .25) a

--https://regilero.github.io/postgresql/english/2017/06/26/postgresql_advanced_generate_series/

-- nested ....   
select (random()* 10)::int as number FROM generate_series(1,100) as generator
-- in subquery : doe generator * 0 !!
select (random()* 10)::int + (generator*0) as number FROM generate_series(1,100) as generator

select a from generate_series(1,100) a

-- create table with random data ?
CREATE TABLE t_random AS SELECT s, md5(random()::text) FROM generate_Series(1,5) s;
INSERT INTO t_random VALUES (generate_series(1,1000000000), md5(random()::text));

SELECT pg_size_pretty(pg_relation_size('t_random'));
-- create table with random data ?


select a from generate_series(1,100) a
select a from generate_series(1,) a
select (random()*5)::int

with numbers as (select a from generate_series(1,100) a)

drop table mytable
with myrand as (select md5(RANDOM()::TEXT) as "first", md5(RANDOM()::TEXT) as "second", CASE WHEN RANDOM() < 0.5 THEN 'male' ELSE 'female' END FROM generate_series(1, 10))
select * into mytable from myrand

select * from mytable 

insert into mytable (select md5(RANDOM()::TEXT) as "first", md5(RANDOM()::TEXT) as "second", CASE WHEN RANDOM() < 0.5 THEN 'male' ELSE 'female' END FROM generate_series(1, 10))

insert into mytable (select md5(RANDOM()::TEXT) as "first", md5(RANDOM()::TEXT) as "second", CASE WHEN RANDOM() < 0.5 THEN 'male' ELSE 'female' END FROM generate_series(1, (random()*10)::int))

delete from mytable

select count(*) from mytable 


select (random()*10)::int + 20 
select (20+random()+random()*.5-random()*1)::float4

select count(*) from mytable 

select a, * from generate_series ((select count(*) from mytable),(select count(*) from mytable)+(random()*20)::int  ) a
with batch as (select b from generate_series(1,5) b)

------ ********** CREATE A SIMPLE DUMMY TABLE **************

CREATE TABLE public.myweightdata (
	boxid int NOT NULL,
	batchid int NULL,
	weight float4 NULL
);



delete from myweightdata

-- create some random numbered batches
insert into myweightdata 
	(select generator, 1, 
	20+random()+random()*.5-random()*1::float4 	FROM generate_series((select count(*) from myweightdata),(select count(*) from myweightdata)+(random()*20)::int) generator);
insert into myweightdata 
	(select generator, 2, 
	20+random()+random()*.5-random()*1::float4 	FROM generate_series((select count(*) from myweightdata),(select count(*) from myweightdata)+(random()*20)::int) generator);
insert into myweightdata 
	(select generator, 3, 
	20+random()+random()*.5-random()*1::float4 	FROM generate_series((select count(*) from myweightdata),(select count(*) from myweightdata)+(random()*20)::int) generator);
insert into myweightdata 
	(select generator, 4, 
	20+random()+random()*.5-random()*1::float4 	FROM generate_series((select count(*) from myweightdata),(select count(*) from myweightdata)+(random()*20)::int) generator);
insert into myweightdata 
	(select generator, 5, 
	20+random()+random()*.5-random()*1::float4 	FROM generate_series((select count(*) from myweightdata),(select count(*) from myweightdata)+(random()*20)::int) generator);
	
select count(*), stddev(weight) from myweightdata ;
select batchid, count(*), stddev(weight) from myweightdata group by batchid
select avg(z.s) from (select batchid, count(*), stddev(weight) as s from myweightdata group by batchid) z


with z as (select batchid, count(*) "c", stddev(weight) as s from myweightdata group by batchid) 
select z.c,z.s  from z


----**************Create a table ******************shortstring in varchar5

drop table public.tbl_shortstring;

CREATE TABLE public.tbl_shortstring (
time timestamp,
	shortstring varchar(5) NULL
);

insert into tbl_shortstring (
select t.seconds, 
--md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT) as shortstring  
left(md5(RANDOM()::TEXT),5)::text as shortstring
from (SELECT generate_series(timestamp 'now()' - interval '24 hour', now(), '1 second')::timestamp) AS t(seconds) --3600 records per uur...
);

select * from tbl_shortstring limit 5;  --3768 kb / 86401 rows

select 
pg_size_pretty (pg_total_relation_size('public.tbl_shortstring')) "prettysize",
pg_total_relation_size('public.tbl_shortstring') "totalsize",
count(*) from tbl_shortstring;

----**************Create a table ******************short string in varchar(255)

drop table public.tbl_longstring;

CREATE TABLE public.tbl_longstring (
time timestamp,
	shortstring varchar(255) NULL
);

insert into tbl_longstring (
select t.seconds,
left(md5(RANDOM()::TEXT),5)::text as longstring
from (SELECT generate_series(timestamp 'now()' - interval '24 hour', now(), '1 second')::timestamp) AS t(seconds) --3600 records per uur...
);

select * from tbl_longstring limit 5; --3768 kb / 86401 rows

select 
pg_size_pretty (pg_total_relation_size('public.tbl_longstring')) "prettysize",
pg_total_relation_size('public.tbl_longstring') "totalsize",
count(*) from tbl_longstring;

--select length('string van lengte 254 : ' || md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT))



----**************Create a table ******************longstring in varchar(255)

drop table public.tbl_longstring1;

CREATE TABLE public.tbl_longstring1 (
time timestamp, 
longstring varchar(255) NULL
);

insert into tbl_longstring1 (
select t.seconds, 
('string van lengte 254 : ' || md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)) as longstring
from (SELECT generate_series(timestamp 'now()' - interval '24 hour', now(), '1 second')::timestamp) AS t(seconds) --3600 records per uur...
);

select * from tbl_longstring1 limit 5; --25 MB / 86401 rows 

select 
pg_size_pretty (pg_total_relation_size('public.tbl_longstring1')) "prettysize",
pg_total_relation_size('public.tbl_longstring1') "totalsize",
count(*) from tbl_longstring1;


--select length('string van lengte 254 : ' || md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT)||'-'||md5(RANDOM()::TEXT))


----**************Create a table ******************timestamp, boolean in boolean

drop table public.tbl_boolean;

CREATE TABLE public.tbl_boolean (
time timestamp,
	mybool boolean NULL
);

insert into tbl_boolean (
select t.seconds, 
(round(random())::int)::boolean "mybool"
from (SELECT generate_series(timestamp 'now()' - interval '24 hour', now(), '1 second')::timestamp) AS t(seconds) --3600 records per uur...
);

select * from tbl_boolean limit 5; --3768 kb / 86401 rows 


select 
pg_size_pretty (pg_total_relation_size('public.tbl_boolean')) "prettysize",
pg_total_relation_size('public.tbl_boolean') "totalsize",
count(*) from tbl_boolean;


----**************Create a table ******************timestamp, boolean in float8

drop table public.tbl_boolean8;

CREATE TABLE public.tbl_boolean8 (
time timestamp,
	mybool float8 NULL
);

insert into tbl_boolean8 (
select t.seconds, 
(round(random())::int)::float8 "mybool"
from (SELECT generate_series(timestamp 'now()' - interval '24 hour', now(), '1 second')::timestamp) AS t(seconds) --3600 records per uur...
);

select * from tbl_boolean8 limit 5; --3768 kb / 86401 rows 


select 
pg_size_pretty (pg_total_relation_size('public.tbl_boolean8')) "prettysize",
pg_total_relation_size('public.tbl_boolean8') "totalsize",
count(*) from tbl_boolean8;


----**************Create a table ******************timestamp, float8 in float8

drop table public.tbl_float8;

CREATE TABLE public.tbl_float8 (
time timestamp,
	myfloat float8 NULL
);

insert into tbl_float8 (
select t.seconds, 
(random())*10^15::float8 "myfloat"
from (SELECT generate_series(timestamp 'now()' - interval '24 hour', now(), '1 second')::timestamp) AS t(seconds) --3600 records per uur...
);

select * from tbl_float8 limit 5; --3768 kb / 86401 rows 


select 
pg_size_pretty (pg_total_relation_size('public.tbl_float8')) "prettysize",
pg_total_relation_size('public.tbl_float8') "totalsize",
count(*) from tbl_float8;

----**************Create a hypertable ******************timestamp, float8 in float8
-- create hypertable !!
select create_hypertable('public.tbl_float8', 'time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => true, migrate_data => TRUE);

select * from tbl_float8 limit 5; --> 8192 bytes of 8 kb / 86401 rows (want alles zit nu in de chunks van de hypertable )

SELECT hypertable_schema , hypertable_name, owner, num_chunks 
,pg_size_pretty (hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass)) as humansize
,hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass) as totalsize
  FROM timescaledb_information.hypertables
  order by totalsize desc
  
-- zoek grootte van hypertables en tabel zelf :  
select * from timescaledb_information.hypertableS  -- tbl_float8 - 25 CHUNKS - 4640 kB  //+  total table size : 8192 bytes of 8kb


--SELECT default_version, installed_version FROM pg_available_extensions where name = 'timescaledb';



