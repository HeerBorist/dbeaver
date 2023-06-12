select ; select * from capturetranslations c

"; delete from "

select * from r r 


SELECT recipex as recipe, $__time(starttime), endtime as timeend, EXTRACT(epoch FROM (starttime - endtime))::int, unnest(qarr), unnest(resarr)
 from pattyn_dtou_batch_dynqArr('{"weightdata_v1|recipientweight_fnetweight|avg","weightdata_v1|recipientweight_fnetweight|sum"}', $__timeFrom(), $__timeTo())
 
 explain analyse SELECT recipex as recipe, starttime, endtime ,EXTRACT(epoch FROM (starttime - endtime))::int, unnest(qarr), unnest(resarr)
 from pattyn_dtou_batch_dynqArr('{"weightdata_v1|recipientweight_fnetweight|avg","weightdata_v1|recipientweight_fnetweight|sum"}', '2021-01-20T04:49:49.815Z', '2021-01-20T07:49:49.815Z')

 select * from  pattyn_getrecipeperiods_epoch ('1002', '2021-01-20T04:49:49.815Z', '2021-01-20T07:49:49.815Z');
 

SELECT
-- time,
-- MachineSerial,
-- Recipe,
--  recipientweight_eweightresult,recipientweight_fgrossweight,recipientweight_fmaxoverweight,recipientweight_fmaxunderweight,recipientweight_fnetweight,recipientweight_fsetpoint,recipientweight_ftareweight,recipientweight_ibatchnr,recipientweight_itimeofweightms,recipientweight_srecipientid,recipientweight_sunitid
*
FROM weightdata_v1
WHERE
  "time" BETWEEN '2021-01-20T23:00:00Z' AND '2021-01-22T22:59:59.999Z'
  and recipientweight_sunitid in ('W1','W2','W3','W4')
  and recipientweight_eweightresult in ('20','10')
limit (1000)


select time, alarm_bactive ,skey, recipe, * from alarms a where skey = '_32_1_69_1' and alarm_bactive is not null and recipe like '%Barry%' order by time limit (100)

{"Alarm_sKey": "_32_1_69_1", "Alarm_sTodConditionCleared": "2021-01-12-13:26:44.032"}
{"Alarm_sKey": "_32_1_69_1", "Alarm_sTodConditionRaised": "2021-01-12-13:26:44.332"}
{"Alarm_sKey": "_32_1_69_1", "Alarm_sTodConditionCleared": "2021-01-12-13:26:44.732"}


