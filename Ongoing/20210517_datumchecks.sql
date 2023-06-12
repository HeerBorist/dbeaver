SELECT
  RecipeLoadCounter as "Id", 
  mode() WITHIN GROUP (ORDER BY Recipe) as "Recipe",
  min(fillingfirsttime),
  max(fillinglasttime),
  
  extract(epoch from min(fillingfirsttime))*1000 as "Start",
  extract(epoch from max(fillinglasttime))*1000 as "End",
  (extract(epoch from max(fillinglasttime)) - extract(epoch from min(fillingfirsttime))) as "Duration", 
  -- array_agg(distinct(setpoint)) as setpoint -- in case want to visualize if setpoint changed 
  -- mode() WITHIN GROUP (ORDER BY setpoint) as "Target weight", -- show target weight? 
  sum(totalweight)/1000 as "Total weight",
  (sum(totalweight)*3.6)/(extract(epoch from max(fillinglasttime))-extract(epoch from min(fillingfirsttime))) as "Speed", 
  -- could use fillingfirsttime and fillinglasttime instead, than performance will be higher
  sum(nrofboxes) as "Total boxes",
  avg(stdevboxweight)*1000 as "Standard deviation"
FROM ppltesttslite4.weightdata_hourly wh 
WHERE 
  --recipe in ($Recipe) and 
  --$__timeFilter(bucket)

  RecipeLoadCounter ='152'
GROUP BY RecipeLoadCounter
ORDER BY RecipeLoadCounter desc
limit 50

--start :2021-05-17 02:33:47 --1621211627978 ==> 
--einde :2021-05-17 06:47:31 --1621226851092
http://10.10.8.129/grafana/dashboard/db/weight-data-batch-overview?var-MachineSerial=2019P092&from=1621211627978&to=1621226851092&var-Selected_RecipeIdx=152

--http://10.10.8.129/grafana/dashboard/db/weight-data-batch-overview?&var-MachineSerial=${MachineSerial}&from=${__data.fields.Start.numeric}&to=${__data.fields.End.numeric}&var-Selected_RecipeIdx=${__data.fields.Id.numeric}


select bucket, fillingfirsttime, fillinglasttime, bucketfirsttime, bucketlasttime,
* from ppltesttslite4.weightdata_hourly wh 
where RecipeLoadCounter = '152'
order by bucketlasttime desc
limit 100 
-- bucket :  2021-05-17 02:00:00  tot 2021-05-17 06:00:00
-- fillingtime : 2021-05-17 02:33:47 tot 2021-05-17 06:47:31
-- bucketfirsttime : 2021-05-17 02:22:29 tot 2021-05-17 06:58:52


--bucket BETWEEN '2021-05-17T00:33:47.978Z' AND '2021-05-17T04:47:31.092Z'

select extract(epoch from TIMESTAMP '2021-05-17 02:33:47') *1000 --1621218827000  (4u33!!)
select extract(epoch from TIMESTAMP '2021-05-17 06:47:31') *1000 --1621234051000

select date_part('epoch', TIMESTAMP '2021-05-17 06:47:31') *1000 AS epoch_time  
select to_timestamp(1621218827000/1000)



select * from ppltesttslite4.weightdata wh 
where RecipeLoadCounter = '152'
order by time asc
limit 100 

--2021-05-17 02:22:29  to : 2021-05-17 06:58:52
select extract(epoch from TIMESTAMP '2021-05-17 02:22:29') *1000 --1621218149000
select extract(epoch from TIMESTAMP '2021-05-17 06:58:52') *1000 --1621234732000

via tabel : 2021-05-17 03:23:51   tot   2021-05-17 06:47:31
via tabel : 2021-05-17 02:23:51   tot   2021-05-17 05:47:31


select * from weightdata w limit 10
select * from weightdata_hourly w limit 10

select round((sum(totalweight)/sum(nrofboxes))::numeric,2) as "Avg weight"
from weightdata_hourly w limit 10


select round(123.021321321321321321321,2)

show server_version
show timezone


reporting test : 

select M.relscheme, relname, sum(rowcount) as "Records", 
sum(size) /(1024*1024) as "tableMB",
sum(totalsize) /(1024*1024) as "totalMB",
sum(rowcount)/(sum(size)+1)*(1024*1024) as "table_recPerMB",
sum(size)/(sum(rowcount)+1) as "table_bytePerrecord",
sum(rowcount)/(sum(totalsize)+1)*(1024*1024) as "totalrecPerMB",
sum(totalsize)/(sum(rowcount)+1) as "totalbytePerrecord"
from public.dbatablesizes M where time in (select last(time,time) from public.dbatablesizes )
and relscheme in ('pattyntestplcts')
group by relscheme, relname
order by  relscheme, "tableMB" desc

select count(*) from "Pattyn".pattyntestplcts.weightdata


select * from public.dbatablesizes d where relscheme like 'pg%' limit 100
delete from public.dbatablesizes d where relscheme like 'pg%' limit 100

select distinct(relscheme ) from public.dbatablesizes d limit 100





