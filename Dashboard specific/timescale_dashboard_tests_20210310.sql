SELECT
  (sum(recipientweight_fnetweight) )/1000 as ton,
  (EXTRACT(EPOCH FROM ('2021-03-02T13:17:38.901Z'::timestamp - '2021-03-02T10:17:38.901Z'::timestamp))/3.6 ) as periodhours,
  ((sum(recipientweight_fnetweight) ) /  (EXTRACT(EPOCH FROM ('2021-03-02T13:17:38.901Z'::timestamp - '2021-03-02T10:17:38.901Z'::timestamp))/3.6 )) as tonperuur
FROM weightdata_v1
WHERE
 "time" BETWEEN '2021-03-02T10:19:40.451Z' AND '2021-03-02T13:19:40.451Z'
 
 
 
 select 
  recipe as metric,
  sum(recipientweight_fnetweight)/1000 as totalton

FROM weightdata_v1
where time
   BETWEEN '2021-03-02T10:39:19.405Z' AND '2021-03-02T13:39:19.406Z'
group by recipe
ORDER BY 1,2


select 
time, 
recipe,
--time_bucket(interval '600s' /10 , time) as Ten_Min,
sum(recipientweight_fnetweight)  AS "producedweight",
count(recipientweight_fnetweight) AS "producedBoxes"
--sum(recipientweight_fnetweight) * 6 AS "H_producedweight",
--count(recipientweight_fnetweight) * 6 AS "H_producedBoxes"
FROM weightdata_v1
WHERE
"time" BETWEEN '2021-03-04T07:00:00.0' AND '2021-03-04T09:00:0.0'
GROUP BY 1,2
ORDER BY 1 desc 

-- get all records in a 2 hour time frame
select 
time, 
recipe,
(recipientweight_fnetweight)  AS "producedweight",
(recipientweight_fnetweight) AS "producedBoxes"
FROM weightdata_v1
WHERE
"time" BETWEEN '2021-03-01T07:00:00.0Z' AND '2021-03-09T09:00:0.0Z'
ORDER BY 1 desc 


--Analytics per 15 minutes
select 
time_bucket_gapfill('900s', time)  as fifteen_Min,
sum(recipientweight_fnetweight)  AS "producedweight",
count(recipientweight_fnetweight) AS "producedBoxes",
sum(recipientweight_fnetweight) /count(recipientweight_fnetweight) AS "weightBoxes",
sum(recipientweight_fnetweight) * 4 AS "H_producedweight",
count(recipientweight_fnetweight) * 4 AS "H_producedBoxes"
FROM weightdata_v1
WHERE
  "time" BETWEEN '2021-03-09T07:00:00.0' AND '2021-03-09T09:00:00.0'
GROUP BY 1 ORDER BY 1  

--original
SELECT "Recipe" FROM (SELECT last("BoxMade"), "Recipe" FROM "autogen"."General" WHERE $timeFilter GROUP BY time($__interval) fill(null))

SELECT "Recipe" FROM (SELECT last("BoxMade"), "Recipe" FROM "autogen"."General" WHERE $__timeFilter("time") GROUP BY time($__interval) fill(null))


SELECT
A.time, A.recipe 
FROM (select  time_bucket('60s',"time") AS "time",boxmade, recipe from general where "time" BETWEEN '2021-03-09T08:42:53.535Z' AND '2021-03-09T14:42:53.535Z' GROUP BY 1,3,2 ) as A




SELECT
  --time,
  time_bucket('60s',"time") AS "time",
  last(boxmade, time) AS "boxmade",
  recipe 
  from general where recipe is not null 
  and time > now() - interval '1 hour'
  group by 1, 3
  limit 150
  
FROM (select  time_bucket('60s',"time") AS "time",boxmade, recipe from general where "time" BETWEEN '2021-03-09T08:42:53.535Z' AND '2021-03-09T14:42:53.535Z' GROUP BY 1,3,2 ) as A
--ORDER BY 1,2

select * from general limit 100

select stmachine_emachinepowerstate ,* from general  where time >  to_timestamp(1615450278590/1000) limit 100

select * from weightdata_v1 wv  where time between  now() - interval '15 min' and now()
select now()




select * from  pattyn_getrecipeperiods_epoch ('1002', (now()- interval '1 day')::timestamp, now()::timestamp) 
order by recipe_epoch_time_from desc --offset 2 limit 1


/*
|recipe         |recipe_epoch_time_from|recipe_epoch_time_to|
|---------------|----------------------|--------------------|
|Barry Callebaut|1615445180102         |1615450276992       |
*/

select  * from general  where time >= to_timestamp(1615445180102/1000) order by 1--2021-03-11 07:46:20
select time, recipientweight_fnetweight, first(recipientweight_fnetweight,time) from weightdata_v1 wv  where time >= to_timestamp(1615445180102/1000) order by 1 --2021-03-11 07:46:58


-- zoek start van een batch, en dan begin van de eerste doos in de weigher...
-- hiervoor moet wel begin en einde van de batch zijn geselecteerd om in dashboard te kunnen werken? 
select 'start batch' as moment, time, recipe from (select time, recipe from general where time >= to_timestamp(1615445180102/1000) order by 1 limit 1 ) as A
union all
select 'first fill' as moment , time, recipe from (select time, recipe from weightdata_v1 wv  where time >= to_timestamp(1615445180102/1000) order by 1 limit 1 ) as A
union all
select 'last fill' as moment, time, recipe from (select time, recipe from weightdata_v1 wv  where time <=  to_timestamp(1615450276992/1000) order by 1 desc limit 1 ) as A
union all
select 'end batch' as moment, time, recipe from (select time, recipe from general where time <= to_timestamp(1615450276992/1000) order by 1 desc limit 1 ) as A

select * from  pattyn_getrecipeperiods_epoch ('1002', (now()- interval '1 day')::timestamp, now()::timestamp) 
order by recipe_epoch_time_from desc --offset 2 limit 1

select * from  pattyn_getrecipeperiods ('1002', (now()- interval '1 day')::timestamp, now()::timestamp) 
order by time_from desc --offset 2 limit 1


select * from weightdata_v1 wv order by time desc limit 10
select * from general order by time desc limit 10

select recipientweight_fmaxoverweight , recip* from weightdata_v1 wv limit 10


		
		
		