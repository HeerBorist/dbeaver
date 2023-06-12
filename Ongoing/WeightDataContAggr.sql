SELECT * FROM "WeightData_v1" wd LIMIT 100

SELECT 
"Recipe",
"RecipientWeight_iBatchNr" as RecipeBatchNr,
"RecipientWeight_sUnitId" ,
time_bucket(interval '15 m', time) as bucket15m,
first(time, time) as starttime,
last(time, time) as finishtime,
count("RecipientWeight_fNetWeight") as boxes,
avg("RecipientWeight_fNetWeight") as avgNetWeight,
sum("RecipientWeight_fNetWeight") as sumNetWeight,
-- batch distribution ?? Welke data is hiervoor nodig ?
-- accuracy - eweightresult : Welke data is hiervoor minimaal nodig ? underw 
--coalesce((select recipientweight_eweightresult from weightdata_v1 wv where recipientweight_eweightresult = '10'), null),
avg("RecipientWeight_fTareWeight")as avgTareWeight,
last("RecipientWeight_fSetPoint", time) as lastSetPoint
FROM "WeightData_v1"
where "RecipientWeight_sUnitId" = 'W1'
--and time > '2021-03-14 04:00:00' and time < '2021-03-15 12:00:00'
group by "Recipe", "RecipientWeight_iBatchNr", "RecipientWeight_sUnitId", bucket15m
order by bucket15m asc
LIMIT 40



SELECT *,
select recipientweight_eweightresult as ten from weightdata_v1 wv where recipientweight_eweightresult = '10' 
FROM weightdata_v1 LIMIT 100