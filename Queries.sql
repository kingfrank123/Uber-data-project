CREATE OR REPLACE TABLE `test_queries.tbl_analytics` AS (
SELECT 
m.trip_id
m.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.RatecodeID
r.Ratecode_name,
pay.Payment_type,
pay.payment_type_name,
m.fare_amount,
m.extra,
m.mta_tax,
m.tip_amount,
m.tolls_amount,
m.improvement_surcharge,
m.total_amount
m.congestion_surcharge
m.Airport_fee
FROM 

`test_queries.tbl_analytics.Main_Table` m
JOIN `test_queries.tbl_analytics.Datetime` d  ON m.datetime_id=d.datetime_id
JOIN `test_queries.tbl_analytics.Passenger_count` p  ON p.passenger_count_id=m.passenger_count_id  
JOIN `test_queries.tbl_analytics.trip_distance` t  ON t.trip_distance_id=m.trip_distance_id  
JOIN `test_queries.tbl_analytics.RatecodeID` r ON r.Rate_Code_ID=m.Rate_Code_ID  
JOIN `test_queries.tbl_analytics.Location_info_T` l ON l.Location_info=m.Location_info
JOIN `test_queries.tbl_analytics.Payment_type` pay ON pay.payment_type_id = m.payment_type_id
;