drop function IF EXISTS public.pbd_function_test_2();
CREATE OR REPLACE FUNCTION public.pbd_function_test_2(recipeLoadCounterIn integer)
 -- returns table(startTime timestamp, endtime timestamp, recipeLoadCounterVar integer, lastState integer)
 -- returns table(timeOut timestamp with time zone, machineTypeOut varchar, machineIndexOut varchar, stateOut float, duration interval)
 -- returns table(timeOut timestamp with time zone, machineTypeOut varchar, machineIndexOut varchar, stateOut float, duration interval)
 returns table(recipeLoadCounterOut integer, stateName varchar(32), stateOut float, duration interval, startTimeOut timestamp, endTimeOut timestamp, totalTimeRecipe interval)
 --returns table(recipeLoadCounterOut integer, startTimeOut timestamp, endTimeOut timestamp, timeOut timestamp with time zone, stateOut float, duration interval)
 -- STRICT
AS $function$
DECLARE
  machineType            varchar := 'argocount' ;
  machineIndexIn         varchar := '01' ;
  startTime                 timestamp ;
  endTime                  timestamp ;
  recipeTotalTime        interval;
  lastStateMachineInProduction  integer;
  lastStateMachineInDefault      integer;
  lastStateMachineWaitingIn      integer;
  lastStateMachineWaitingOut    integer;
  lastStateMachineWaitingInOut    integer;
  -- recipeLoadCounterVar     integer;
BEGIN 
	-- select some static parameters for the complete set : 
		select
	        time as startTime,
	        coalesce(lead(time) over (order by time), now()) as endTime
        from pattyntestplcts.software_pbd
        where recipeloadcounter >= recipeloadcounterIn --2184 --recipeloadcounterIn
        into starttime, endtime
        order by time asc
        limit 1;
       
       	-- select some static parameters for the complete set :
       select endTime - startTime  into recipeTotalTime;

       
    -- Fill some variables that will not be used later ??
	/*
       select  machine_in_production::integer,
            machine_default::integer,
            machine_waiting_infeed::integer,
            machine_waiting_outfeed::integer,
            machine_waiting_inoutfeed::integer
        from pattyntestplcts.status_pbd
        where time < endtime --'2021-06-07 22:22:47' --endtime
        into lastStateMachineInProduction, lastStateMachineInDefault, lastStateMachineWaitingIn, lastStateMachineWaitingOut, lastStateMachineWaitingInOut 
        order by time desc 
        limit 1;
     */
       
    return QUERY

       -- result for 'Machine_in_production'
    select 
            recipeloadcounterIn as recipeLoadCounterOut,
            'Machine_in_production'::varchar(32) as stateName,  -- change this !!
            machine_state.state as stateOut,
            sum(machine_state.duration) as duration,
            starttime as startTimeOut,
            endtime as endTimeOut,
            recipeTotalTime as totalTimeRecipe
            from (
		        select
		        machine_in_production as state,  -- change this !!
		        (coalesce(lead(time) over (order by time), endTime )  - time) as duration 
                from pattyntestplcts.status_pbd
                where 
                    --machine_in_production = 1 and
                    machine = machineType 
                    and machineIndex = machineIndexIn 
                    and recipeloadcounter = cast(recipeloadcounterIn as varchar) 
                    order by time asc
                 ) as machine_state
            group by machine_state.state

         
    union
--    result for 'Machine_default'
    select 
            recipeloadcounterIn as recipeLoadCounterOut,
            'Machine_default'::varchar(32) as stateName,  -- change this !!
            machine_state.state as stateOut,
            sum(machine_state.duration) as duration,
            starttime as startTimeOut,
            endtime as endTimeOut,
            recipeTotalTime as totalTimeRecipe
            from (
		        select
		        Machine_default as state,  --change this !!
		        (coalesce(lead(time) over (order by time), endTime )  - time) as duration 
                from pattyntestplcts.status_pbd
                where 
                    --machine_in_production = 1 and
                    machine = machineType 
                    and machineIndex = machineIndexIn 
                    and recipeloadcounter = cast(recipeloadcounterIn as varchar) 
                    order by time asc
                 ) as machine_state
            group by machine_state.state
 union
--    result for 'machine_ready'
 select 
            recipeloadcounterIn as recipeLoadCounterOut,
            'machine_ready'::varchar(32) as stateName,  -- change this !!
            machine_state.state as stateOut,
            sum(machine_state.duration) as duration,
            starttime as startTimeOut,
            endtime as endTimeOut,
            recipeTotalTime as totalTimeRecipe
            from (
		        select
		        machine_ready as state,  --change this !!
		        (coalesce(lead(time) over (order by time), endTime )  - time) as duration 
                from pattyntestplcts.status_pbd
                where 
                    --machine_in_production = 1 and
                    machine = machineType 
                    and machineIndex = machineIndexIn 
                    and recipeloadcounter = cast(recipeloadcounterIn as varchar) 
                    order by time asc
                 ) as machine_state
            group by machine_state.state
  
 union
--    result for 'machine_running'
 
  union
--    result for 'machine_waiting_infeed'

 union
--    result for 'machine_waiting_inoutfeed'
 
 union
--    result for 'machine_waiting_outfeed'
 
 
            
-- this is the last line :             
            ;

    
END
$function$ LANGUAGE plpgsql


select *  from public.pbd_function_test_2(2184) 
where stateout = 1 --or stateout = 0


select
				recipeloadcounter ,
		        machine_in_production as state,  -- change this !!
		        time
---		        (coalesce(lead(time) over (order by time), endTime )  - time) as duration 
                from pattyntestplcts.status_pbd
                where 
                    --machine_in_production = 1 and
                    machine = 'argocount' --achineType' 
                    and machineIndex = '01' 
                    and recipeloadcounter = cast(2184 as varchar) 
                    order by time asc

                    
