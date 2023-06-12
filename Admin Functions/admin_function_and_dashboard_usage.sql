-- drop function public.pattyn_get_curr_fx_name()
/*
CREATE OR REPLACE FUNCTION public.pattyn_get_curr_fx_name()
RETURNS text AS  $$
DECLARE
  stack text; fcesig text;
BEGIN
  GET DIAGNOSTICS stack = PG_CONTEXT;
 -- fcesig := substring(stack from 'function (.*?) line');
  fcesig := substring(substring(stack from 'unction (.*)') from 'function (.*?) line');
  RETURN fcesig::regprocedure::text;
END;
$$ LANGUAGE plpgsql;
*/



--alter user PattynAdmin with password 'new_password';


CREATE OR REPLACE FUNCTION public.pattyn_dba_functionlogger(
	v_logcategory varchar(255) DEFAULT NULL, 
	v_logcomment varchar(255) DEFAULT null,
	v_organizationid integer default null,
	v_organization varchar(255) default null,
	v_userid integer default null,
	v_username varchar default null,
	v_useremail varchar default null,
	v_dashboard varchar default null,
	v_dashboardrange bigint default null
	)
 RETURNS void
 LANGUAGE plpgsql
AS $function$

	DECLARE
	 stack text; 
	 fcesig text;
	begin 
		
		
/* FUNCTION COMMENT : 
 * 
 * Created 	 : 20210728
 * CreatedBy : SSAV
 * 
 * ----------------------------------------------------------------------------------
 * Change History : 
 *
 * 
 * ----------------------------------------------------------------------------------
 *   
 * Purpose : 
 * 	Logging of use of dashboard or function.
 *  This can provide insights in the most used functions, customer dashboard use, ...
 *   
 * 
 * Inputs  : 
 * - lots of grafana built in variables (dashboard name, user, etc;...) and grafana user login, company, ...
 * 
 * dependencies and security : 
 * - check all variables for "sql injection like" commands : makes use of function "pattyn_check_string()" !!
 *
 * Output  : 
 * - log a record in the public.dba_log table.   
 *
 *	-- it is possible to call this function from every function you want to log : it will record the name of the called function.
 *	-- it is possible to call this function from a dashboard (as dashboard variable) to record opening of a dashboard / refresing of a dashboard	
 *	-- the logging will consist of : 
 *	-- - logtime, called function name, sessionuser and searchpath, clientaddress and port, dashboard detailed information
 *	-- optional : provide logcategory and or logcomment as string when calling the function.  
 *  
 * ----------------------------------------------------------------------------------
 * 
 * Example of how to Use the function : 
 *
 * --usage template in e.g. other function : 
 *	select pattyn_dba_functionlogger (v_logcategory:='',v_logcomment:='',v_organizationid:=,v_organization:='',v_userid:=,v_username:='',v_useremail:='',v_dashboard:='',v_dashboardrange:=)
 *
 *	-- grafana template : create dashboard variable as query against timescale datasource, refresh on dashboard load, or refresh on time range change...
 *  -- or call this function in a panel as "query B", without showing the results 
 * 
 * select pattyn_dba_functionlogger (v_logcategory:='Dashboard',
 * v_logcomment:='comment goes here',
 * v_organizationid:='$__org',
 * v_organization:='${__org.name}',
 * v_userid:='${__user.id}',
 * v_username:='${__user.login}',
 * v_useremail:='${__user.email}',
 * v_dashboard:='$__dashboard',
 * v_dashboardrange:= ((${__to}-${__from})/(1000*60))::bigint)
 * ----------------------------------------------------------------------------------
 * END OF FUNCTION COMMENT
 * 
 *		
 * -- Then function : 		
 *-- This is what the function needs to work properly.
 * -- Because a normal user will get an error message on the create table, it needs to be run by sysadmin once !!		
 *	
	CREATE TABLE if not exists public.dba_log (
	logtime timestamptz NOT NULL,
	logcategory varchar NULL,
	logcomment varchar NULL,
	graf_organizationid integer NULL,
	graf_organization varchar NULL,
	graf_userid integer NULL,
	graf_username varchar NULL,
	graf_useremail varchar NULL,
	graf_dashboard varchar NULL,
	graf_range bigint NULL,
	functionname varchar NULL,
	sessionuser varchar NULL,
	searchpath varchar null,
	clientaddr inet null,
	clientport integer null
);

-- Grant permissions to public, to make sure that all existing and future users can use the function !!
GRANT EXECUTE ON FUNCTION public.pattyn_dba_functionlogger(varchar, varchar) TO public;
GRANT INSERT ON TABLE public.dba_log TO public;
GRANT SELECT ON TABLE public.dba_log TO public;

*/
		
		-- check all inputs for dangerous commands...
		if 	(pattyn_check_string(v_logcategory) = 0) 
		or 	(pattyn_check_string(v_logcomment) = 0)
		--or 	(pattyn_check_string(v_organizationid) = 0)
		or 	(pattyn_check_string(v_organization) = 0)
		--or 	(pattyn_check_string(v_userid) = 0)
		or 	(pattyn_check_string(v_username) = 0)
		or 	(pattyn_check_string(v_useremail) = 0)
		or 	(pattyn_check_string(v_dashboard) = 0)
		then raise exception 'input did not pass the SQL injection security test !'
				using hint = 'check the input variables for words like -end- -begin- -drop- ...';
		else
			  GET DIAGNOSTICS stack = PG_CONTEXT;
			  fcesig := substring(substring(stack from 'unction (.*)') from 'function (.*?) line');
	
			 insert into public.dba_log 
			 (logtime,
			 logcategory,
			 logcomment,
			 graf_organizationid ,
			 graf_organization, 
			 graf_userid, 
			 graf_username, 
			 graf_useremail, 
			 graf_dashboard,
			 graf_range,
			 functionname,sessionuser,searchpath,clientaddr,clientport)	
			 select 
			 	now() as logtime, 
			    --format('%L',v_logcategory) as logcategory, --prevent sql injection in input field ?
			 	--format('%L',v_logcomment) as logcomment, --prevent sql injection in input field ?
			 	v_logcategory,
			 	v_logcomment,
			 	v_organizationid ,
				v_organization ,
				v_userid,
				v_username,
				v_useremail,
				v_dashboard,
				v_dashboardrange,
			    fcesig::regprocedure::text as functionname,
				session_user as sessionuser,
			    (SELECT setting FROM pg_settings WHERE name = 'search_path') as searchpath,
			    inet_client_addr(),
			    inet_client_port()
			   ;
		end if;

		-- make sure that, if there is any error in the logging function, it does not block any user or other functionality !!  
		exception
			when others then 
				raise info 'function pattyn_dba_functionlogger : %', sqlerrm;

			return;
		
	end;
$function$
;


--drop table public.dba_log

--show search_path

select count(*) from dba_log where logcategory like 'Dash%' 

select * from dba_log
--where logcategory like 'Dash%'
order by logtime desc

--delete from dba_log


select * from pattyn_weight_details('1 hour')  -- weight_details contains reference to functionlogger !
select pattyn_dba_functionlogger('function','data test')
select pattyn_dba_functionlogger('comment','this delete example will not create a logging');

/*
--usage template in e.g. other function : 
select pattyn_dba_functionlogger (v_logcategory:='',v_logcomment:='',v_organizationid:=,v_organization:='',v_userid:=,v_username:='',v_useremail:='',v_dashboard:='',v_dashboardrange:=)
*/


/*
-- grafana template : create dashboard variable as query against timescale datasource, refresh on dashboard load, or refresh on time range change...
select pattyn_dba_functionlogger (v_logcategory:='Dashboard',
v_logcomment:='comment goes here',
v_organizationid:='$__org',
v_organization:='${__org.name}',
v_userid:='${__user.id}',
v_username:='${__user.login}',
v_useremail:='${__user.email}',
v_dashboard:='$__dashboard',
v_dashboardrange:= ((${__to}-${__from})/(1000*60))::bigint)

*/


show timezone

select pattyn_dba_functionlogger (v_logcategory:='Dashboard',
v_logcomment:='Weightdata with LOGGER testing in the background !',
v_organizationid:='76',
v_organization:='Test-Company',
v_userid:='122',
v_username:='Pattyn_Stefan',
v_useremail:='122',
v_dashboard:='UserConnectionTest',
v_dashboardrange:= ((1627394167377-1627390567377)/(1000*60))::bigint)




-- DASHBOARD QUERIES
select count(*) from dba_log where logcategory like 'Dash%' 

select * from dba_log
--where logcategory like 'Dash%'
order by logtime desc
limit 1000


select * from dba_log
where graf_organization is null
order by logtime desc
limit 1000


select coalesce (graf_username,'NULL') as grafanausername from dba_log --where logcategory in ($category)
group by graf_username

select distinct graf_organization  from dba_log
select distinct logcategory  from dba_log

select graf_organization, count(*) from dba_log group by graf_organization

select coalesce (graf_organization,'NULL') as organization from dba_log group by graf_organization
select coalesce (graf_dashboard,'NULL') as dashboard from dba_log group by graf_dashboard
select coalesce (graf_username,'NULL') as username from dba_log group by graf_username
select coalesce (logcategory ,'NULL') as category from dba_log group by category

select coalesce (graf_dashboard,'NULL') as mydashboard from 
dba_log where graf_organization in ($organization) 
and logcategory in ($category) 
group by "graf_dashboard"



SELECT
  time_bucket('3600s',"logtime") as time ,
  logcategory, 
  graf_organization,
  graf_dashboard,
--  "logtime" AS time,
  count(logtime) as refreshes,
  1 as perhour
FROM dba_log
WHERE
  "logtime" BETWEEN '2021-07-26T09:29:18.286Z' AND '2021-08-02T09:29:18.286Z'
    --$__timeFilter("time")
  and logcategory in ('Dashboard')
group by time, logcategory ,graf_organization ,graf_dashboard 
order by time asc



SELECT
time_bucket('3600s',"logtime") as perhour ,  
--"logtime" as time,
graf_organization as metric,
count(graf_organization) as refreshes

--count("logtime")  as refreshes
  --1 as usage
FROM dba_log
WHERE
  logtime BETWEEN '2021-07-03T07:14:33.555Z' AND '2021-08-03T07:29:33.555Z'
  and logcategory in ('Dashboard')
group by perhour, graf_organization ---,graf_dashboard 
order by perhour asc


select * from dba_log
where logcategory in ('Dashboard')
order by logtime desc

/*
update  dba_log set graf_organization = 'Test-Company1'
where logcategory in ('Dashboard') and logcomment = 'Refreshing the dashboard'
--order by logtime desc
*/
-- old 

SELECT
    time_bucket('3600s',"logtime") as time ,
  graf_organization || ' / '|| graf_dashboard as dashboard,
    count(logcategory) as refreshes
FROM dba_log
WHERE
  "logtime" BETWEEN '2021-07-27T10:47:12.019Z' AND '2021-08-03T10:47:12.019Z'
  and logcategory in ('Dashboard','Dashboard INFLUX')
  and graf_organization in ('AAK-Sweden','Bunge-Loders','Pattyn')
  and "graf_dashboard" in ('DatabaseAdministration','Weight Data Batch overview','Live','Weigher data','Weight Data TS','Corner seals','Weight Data Batch TS','Machine detail','DashboardUsage','Box production','Weight Data Active Batch')
group by dashboard , time
order by refreshes desc