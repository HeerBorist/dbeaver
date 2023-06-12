select * from pg_user;

select usename, usesysid, useconfig from pg_user;

select * from pg_roles;

select current_database(), current_schema , current_user
show search_path;

--ALTER ROLE root SET search_path = "$user", public;
--ALTER ROLE root SET search_path =  public;


--table PERMISSIONS
select  * from information_schema.role_table_grants 
--where grantee='testcompany'
where grantee='root'
and table_schema like 'testplc%'
;

--OWNERSHIP
select 
   * 
from pg_tables 
where tableowner = 'testcompany'
;

--schema PERMISSIONS
select  
  r.usename as grantor, e.usename as grantee, nspname, privilege_type, is_grantable
from pg_namespace
join lateral (
  SELECT
    *
  from
    aclexplode(nspacl) as x
) a on true
join pg_user e on a.grantee = e.usesysid
join pg_user r on a.grantor = r.usesysid 
 where e.usename = 'testcompany'
;


-- list schemas 
select s.nspname as table_schema,
       s.oid as schema_id,  
       u.usename as owner
from pg_catalog.pg_namespace s
join pg_catalog.pg_user u on u.usesysid = s.nspowner
order by table_schema;

-- way to create a schema : 
--CREATE SCHEMA schema_name AUTHORIZATION user_name;
-- You can even omit the schema name, in which case the schema name will be the same as the user name. 

-- Change schema owner
-- ALTER SCHEMA other_schema OWNER TO user;


-- Change table owner
-- ALTER TABLE other_schema.table1 OWNER TO user;

-- set the search path for a user :
-- ALTER ROLE username SET search_path = schema1,schema2,schema3,etc;

--  set the search path for a user + database :
-- alter role username IN DATABASE DATABASENAME set search_path = whatever

-- ALTER ROLE ALL SET search_path = "$user".

GRANT SELECT ON public.capturetranslations TO testcompany;


-- aanmaak van voorzichtiger user ipv root : 

CREATE ROLE "PattynAdmin" NOSUPERUSER NOCREATEDB NOCREATEROLE noINHERIT LOGIN PASSWORD 'NimdaNyttap';
alter role "PattynAdmin" SUPERUSER NOINHERIT
grant all privileges on database "Pattyn" to "PattynAdmin"
grant all privileges on schema testplctimescale to "PattynAdmin"
grant all privileges on schema public to "PattynAdmin"
ALTER DEFAULT privileges FOR USER "PattynAdmin" GRANT all privileges ON schemas TO "PattynAdmin";



ALTER DEFAULT PRIVILEGES
FOR USER "PattynAdmin"
--IN SCHEMA testplctimescale
--GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "PattynAdmin";
--GRANT all privileges ON TABLES TO "PattynAdmin";
GRANT all privileges ON schemas TO "PattynAdmin";


ALTER USER "PattynAdmin" WITH PASSWORD 'z5GKS=rnk5#@vUs+gUGc';


