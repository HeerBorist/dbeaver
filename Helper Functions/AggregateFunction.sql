
-- Create aggregate function 
-- https://hashrocket.com/blog/posts/custom-aggregates-in-postgresql
-- https://stackoverflow.com/questions/30667245/writing-my-own-aggregate-function-in-postgresql
-- https://www.cybertec-postgresql.com/en/writing-your-own-aggregation-functions/

--RAISE NOTICE 'i want to print % and %', var1,var2;


CREATE FUNCTION public.pattyn_dtou_statecnt_sfunc(agg_state integer[], val integer)
-- agg_state contains counter & prev val
RETURNS integer[]
immutable 
language plpgsql
as $$
declare 
    cnt integer; 
    prv integer; 
    new_agg_state integer[];
begin    
    
    if agg_state[0] is null
    then 
        cnt := 0; 
    else
        cnt := agg_state[0]; 
    end if; 

 

    if agg_state[1] is null
    then 
        prv := 0; 
    else
        prv := agg_state[1]; 
    end if; 
    
    if val = 300 and 
        val <> prv -- val matches and is different from previous val
    then 
        cnt := cnt + 1; 
    end if; 

 

    agg_state[0] := cnt; 
    agg_state[1] := val; 

 

    return agg_state; 
end; 
$$;    

 

drop function  public.pattyn_dtou_statecnt_ffunc(integer[])

 

create function public.pattyn_dtou_statecnt_ffunc(agg_state integer[])
returns integer
immutable
strict
language plpgsql
as $$
begin
  return agg_state[0];
end;
$$;

 

drop aggregate pattyn_dtou_statecnt_aggr(integer)

 

create aggregate public.pattyn_dtou_statecnt_aggr (val integer)
(
    sfunc = public.pattyn_dtou_statecnt_sfunc,
    stype = integer[],
    finalfunc = public.pattyn_dtou_statecnt_ffunc,
    initcond = '{0,0}'
);

-- original Dieter : 
select
recipeloadcounter,
pattyn_dtou_statecnt_aggr(stmachine_emachineprogramstate::int ) as changecnt
from general 
where 
    stmachine_emachineprogramstate is not null  and 
    recipeloadcounter = '43' or recipeloadcounter = '42' 
group by recipeloadcounter

/*
|recipeloadcounter|changecnt|
|-----------------|---------|
|43|213|
|42|26|
*/
 
-- proper way to use the function ! with an order !!
select
recipeloadcounter,
pattyn_dtou_statecnt_aggr(stmachine_emachineprogramstate::int order by time asc) as changecnt
from general 
where 
    stmachine_emachineprogramstate is not null  and 
    recipeloadcounter IN ('42','43') 
group by recipeloadcounter

/*
|recipeloadcounter|changecnt|
|-----------------|---------|
|42|25|
|43|213|
*/



