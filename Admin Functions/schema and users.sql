


--CREATE SCHEMA [IF NOT EXISTS] schema_name;
--CREATE SCHEMA [IF NOT EXISTS] AUTHORIZATION username;


-- select all existing schema's from database
SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname;



	-- create a new role called Company1 : role is valid in the database cluster, and so are valid in all databses in the cluster
CREATE ROLE company1 WITH LOGIN PASSWORD 'company1';
	-- create a new USER called Company1 : CREATE USER is now an alias for CREATE ROLE. 
	-- The only difference is that when the command is spelled CREATE USER, LOGIN is assumed by default, whereas NOLOGIN is assumed when the command is spelled CREATE ROLE
--  CREATE USER company1 with LOGIN PASSWORD 'company1';
	-- Second, create a schema for company1:
CREATE SCHEMA AUTHORIZATION company1;
	--Third, create a new schema called company1 that will be owned by company1:
CREATE SCHEMA IF NOT EXISTS company1 AUTHORIZATION company1;



CREATE SCHEMA Company1 AUTHORIZATION company1;


CREATE ROLE User1 WITH 
	NOSUPERUSER
	CREATEDB
	NOCREATEROLE
	INHERIT
	LOGIN
	NOREPLICATION
	NOBYPASSRLS;
	
-- select all users
SELECT usename AS role_name,
  CASE 
     WHEN usesuper AND usecreatedb THEN 
	   CAST('superuser, create database' AS pg_catalog.text)
     WHEN usesuper THEN 
	    CAST('superuser' AS pg_catalog.text)
     WHEN usecreatedb THEN 
	    CAST('create database' AS pg_catalog.text)
     ELSE 
	    CAST('' AS pg_catalog.text)
  END role_attributes
FROM pg_catalog.pg_user
ORDER BY role_name desc;

-- check the priviliges of a user
select * from pg_user;
select * from pg_roles;

select search_path;


-- table names en dergelijke : 
SELECT table_name FROM $tablesRef where table_schema like '%public%' --regex : /weightdata_v1/
SELECT table_name FROM information_schema.tables where table_schema like '%public%' --regex : /weightdata_v1/
information_schema.tables




select current_database(), current_schema , current_user
show search_path;


SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname;

--SELECT schema_name, * FROM information_schema.schemata --where schema_owner like 'company1'

/*
select s.nspname as table_schema,
       s.oid as schema_id,  
       u.usename as owner
from pg_catalog.pg_namespace s
join pg_catalog.pg_user u on u.usesysid = s.nspowner
order by table_schema;
*/



WITH "names"("name") AS (
  SELECT n.nspname AS "name"
    FROM pg_catalog.pg_namespace n
      WHERE n.nspname !~ '^pg_'
        AND n.nspname <> 'information_schema'
) SELECT "name",
  pg_catalog.has_schema_privilege(current_user, "name", 'CREATE') AS "create",
  pg_catalog.has_schema_privilege(current_user, "name", 'USAGE') AS "usage"
    FROM "names";
   
   
    
   
   


