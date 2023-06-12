

/*
 * Created 	 : 20210129
 * CreatedBy : SSAV
 * ----------------------------------------------------------------------------------
 * Purpose : return weightdata per batch
 * 
 * ----------------------------------------------------------------------------------
 *
 * Inputs  : 
 * - none or one specific recipe...
 * 
 * Output  : 
 * - table with last xx batches 
 * 
 * Used By : 
 * - testing the reports... and report parameters (recipe in this case???)
 *   
 * ----------------------------------------------------------------------------------
 * Changes:
 * 20210531 SSAV - first creation
 * 
 * ---------------------------------------------------------------------------------- 
 * How to Use : 
 * 	select * from public.pattyn_weight_summary()
 * select * from public.pattyn_weight_summary('%cal%')
 * select * from public.pattyn_weight_summary('J10') -- mooie kolommen
 * select public.pattyn_weight_summary() -- als 1 veld !!

 * ----------------------------------------------------------------------------------
 */ 
		

-- Then function : 		


select sum(duration), count(duration) from (
select (time - prev_time) as duration
from (
    SELECT time, progstate,
        lag(time) over (order by time) as prev_time
      FROM (
                SELECT time
                    , stmachine_emachineprogramstate as progstate
                       , LAG(stmachine_emachineprogramstate) OVER (ORDER BY time) AS prev_progstate
           FROM general
           where
               stmachine_emachineprogramstate is not null
               order by time desc
               limit 1000) x
      WHERE
          progstate <> COALESCE(prev_progstate, progstate)
          and (progstate = 600 or prev_progstate = 600)
      ORDER BY time asc
      ) x
where prev_time is not null
    and progstate <> 600
order by time desc
) x

-- zoek een basis set data
select stmachine_emachineprogramstate,* from general 
where stmachine_emachineprogramstate is not null
and recipeloadcounter = '306'
order by time desc
limit 1000


select count(*) from general


select count(duration) as "Alarm count", sum(duration) as "Alarm time" from (
select (time - prev_time) as duration
from (
	SELECT time, progstate,prev_progstate,
		lag(time) over (order by time) as prev_time
	  FROM (
				SELECT time
					, stmachine_emachineprogramstate as progstate
	           		, LAG(stmachine_emachineprogramstate) OVER (ORDER BY time ) AS prev_progstate
	    --       		,COALESCE(LAG(stmachine_emachineprogramstate) OVER (ORDER BY time ), stmachine_emachineprogramstate)
	       FROM general
	       where 
	       	stmachine_emachineprogramstate is not null 
	       	 and recipeloadcounter = '306'
	       	 --and  $__timeFilter(time)	       	
	       	order by time  
	       	) x
	  WHERE 
	  	progstate <> COALESCE(prev_progstate, progstate) --coalesce only necessary for the very first row, to avoid a 'null' previous value by replacing it with the actual value
	  	and 
	  	(progstate = 600 or prev_progstate = 600)
	  ORDER BY time asc
	  ) x
where prev_time is not null 
	and progstate <> 600
order by time desc
) x

