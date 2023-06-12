
select current_database(), current_schema , current_user
 
--|current_database|current_schema  |current_user|
--|----------------|----------------|------------|
--|Pattyn          |testplctimescale|testcompany |

show search_path;

--|search_path             |
--|------------------------|
--|testplctimescale, public|


-- list schemas : testcompany user zou moeten eigenaar zijn van schema testplctimescale ??
select s.nspname as table_schema,
       s.oid as schema_id,  
       u.usename as owner
from pg_catalog.pg_namespace s
join pg_catalog.pg_user u on u.usesysid = s.nspowner
order by table_schema;

/*
|table_schema           |schema_id|owner|
|-----------------------|---------|-----|
|_timescaledb_cache     |16931    |root |
|_timescaledb_catalog   |16929    |root |
|_timescaledb_config    |16932    |root |
|_timescaledb_internal  |16930    |root |
|information_schema     |13158    |root |
|pg_catalog             |11       |root |
|pg_toast               |99       |root |
|pplsoftwarets          |19941    |root |
|public                 |2200     |root |
|testplctimescale       |19937    |root |
|timescaledb_information|17392    |root |
*/


select * from public.capturetranslations c  
--SQL Error [42501]: ERROR: permission denied for table capturetranslations


select * from pplsoftwarets.monitoring m 
-- SQL Error [42501]: ERROR: permission denied for schema pplsoftwarets

select * from testplctimescale.alarms a  

--|time               |machine|machineserial|recipe         |recipebatch|skey        |loggeractivationuid                 |alarm_backnowledged
--|-------------------|-------|-------------|---------------|-----------|------------|------------------------------------|-------------------
--|2021-03-16 15:41:09|1002   |2019P092     |J01            |39         |_32_1_69_1  |b3a8e4aa-2c69-2f1e-c321-c10034d1781f|1.0                
--|2021-03-16 15:41:50|1002   |2019P092     |J01            |39         |_29_1_39_1  |b3a8e4aa-2c69-2f1e-c321-c10034d1781f|0.0                

select  * from information_schema.role_table_grants where table_schema not like '_time%'
/*
|grantor    |grantee    |table_catalog|table_schema    |table_name   |privilege_type|is_grantable|with_hierarchy|
|-----------|-----------|-------------|----------------|-------------|--------------|------------|--------------|
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1|INSERT        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1|SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1|UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1|DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1|TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1|REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1|TRIGGER       |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|general      |INSERT        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|general      |SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|general      |UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|general      |DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|general      |TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|general      |REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|general      |TRIGGER       |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|alarms       |INSERT        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|alarms       |SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|alarms       |UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|alarms       |DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|alarms       |TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|alarms       |REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|alarms       |TRIGGER       |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|objects      |INSERT        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|objects      |SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|objects      |UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|objects      |DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|objects      |TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|objects      |REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|objects      |TRIGGER       |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|monitoring   |INSERT        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|monitoring   |SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|monitoring   |UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|monitoring   |DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|monitoring   |TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|monitoring   |REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|monitoring   |TRIGGER       |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|parameters   |INSERT        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|parameters   |SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|parameters   |UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|parameters   |DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|parameters   |TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|parameters   |REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|parameters   |TRIGGER       |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|options      |INSERT        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|options      |SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|options      |UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|options      |DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|options      |TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|options      |REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|options      |TRIGGER       |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|module       |INSERT        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|module       |SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|module       |UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|module       |DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|module       |TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|module       |REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|module       |TRIGGER       |YES         |NO            |
*/

-- user laten gebruik maken van schema public : lijkt in orde .. alle functies kunnen worden aangeroepen
-- 3. Then GRANT USAGE on schema:
-- GRANT USAGE ON SCHEMA schema_name TO username;

-- user laten selecteren uit public.table lukt niet , dus : 
-- 4. GRANT SELECT

--Grant SELECT for a specific table:
--	GRANT SELECT ON table_name TO username;

--Grant SELECT for multiple tables:
--	GRANT SELECT ON ALL TABLES IN SCHEMA schema_name TO username;

--If you want to grant access to the new table in the future automatically, you have to alter default:

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO testcompany;

select * from capturetranslations2
drop table capturetranslations2

-- (uitvoeren als root ! )
GRANT SELECT ON public.capturetranslations TO testcompany;

select  * from information_schema.role_table_grants where table_schema not like '_time%' order by table_schema
select  * from information_schema.role_table_grants where table_schema like 'public' order by table_schema

/*
|grantor    |grantee    |table_catalog|table_schema    |table_name         |privilege_type|is_grantable|with_hierarchy|
|-----------|-----------|-------------|----------------|-------------------|--------------|------------|--------------|
|root       |testcompany|Pattyn       |public          |capturetranslations|SELECT        |NO          |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1      |SELECT        |YES         |YES           |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1      |UPDATE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1      |DELETE        |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1      |TRUNCATE      |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1      |REFERENCES    |YES         |NO            |
|testcompany|testcompany|Pattyn       |testplctimescale|weightdata_v1      |TRIGGER       |YES         |NO            |
	etc
*/

-- bij aanmaken van nieuwe public.tabel heeft user geen rechten ? 

-- user kan tabellen aanmaken in public ?? : ja 

CREATE TABLE public.capturetranslations2 (
	"type" varchar(9) NOT NULL,
	typeid int4 NOT NULL,
	parentmoduleid int4 NOT NULL,
	parentobjectid int4 NOT NULL,
	consecutivenumber int4 NOT NULL,
	id int4 NOT NULL,
	"name" varchar(255) NOT NULL,
	"language" varchar(5) NOT NULL,
	"translation" varchar(1000) NULL,
	skey varchar(14) NOT NULL,
	uniquekey varchar(25) NOT NULL,
	CONSTRAINT pk_translations2 PRIMARY KEY (uniquekey)
);

-- user kan tabellen verwijderen in public ?? : ja 
drop table public.capturetranslations1

-- user kan lokaal nog tabellen aanmaken/verwijderen
CREATE TABLE localcapturetranslations (
	"type" varchar(9) NOT null);
drop table localcapturetranslations


-- user has the 'INHERIT' privileges ?? ==> maakt momenteel geen verschil ??
ALTER ROLE testcompany NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN;

-- maar, testcompany is geen owner van het schema testplctimescale... (owner = root)
-- dus misschien moeten we dit eerst eens aanpassen ? 

alter schema testplctimescale owner to testcompany

-- revoke moet op special user 'public' gebeuren... !? 

-- voorkomen dat iemand met een public role (iedereen dus), iets kan aanmaken : lijkt te werken !
revoke create on schema public from public;

-- revoce delete (on existing tables ? ) 
revoke delete on all tables in schema public from public;
--
revoke insert on all tables in schema public from public;
-- 
revoke update on all tables in schema public from public;
revoke truncate on all tables in schema public from public;

----------------------------------------------
--- Kan ik het vanuit Grafana kapot maken ?
----------------------------------------------


CREATE TABLE testplctimescale.newtable (
	column1 varchar NULL,
	column2 varchar NULL
);


INSERT INTO testplctimescale.newtable
(column1, column2)
VALUES('test', 'test');

select * from testplctimescale.newtable
select random() * 9 + 1;

delete from testplctimescale.newtable
where column1 in (select column1 from testplctimescale.newtable limit 1)

--DROP TABLE testplctimescale.newtable;






 


