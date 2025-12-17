USE weather_db;

INSERT OVERWRITE LOCAL DIRECTORY '/results/seasonal_evapotranspiration'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','

SELECT
    l.city_name AS district,
    CASE
        WHEN month(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(w.`date`, 'M/d/yyyy')) AS DATE)) IN (9,10,11,12,1,2,3) THEN 'Sep-Mar'
        WHEN month(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(w.`date`, 'M/d/yyyy')) AS DATE)) IN (4,5,6,7,8) THEN 'Apr-Aug'
        ELSE 'Other'
    END AS season,
    AVG(w.et0_fao_evapotranspiration) AS avg_evapotranspiration,
    COUNT(*) AS record_count
FROM weather_data w
JOIN location_data l
ON w.location_id = l.location_id
WHERE w.et0_fao_evapotranspiration IS NOT NULL
    AND w.`date` IS NOT NULL
    AND w.`date` != ''
GROUP BY 
    l.city_name,
    CASE
        WHEN month(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(w.`date`, 'M/d/yyyy')) AS DATE)) IN (9,10,11,12,1,2,3) THEN 'Sep-Mar'
        WHEN month(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(w.`date`, 'M/d/yyyy')) AS DATE)) IN (4,5,6,7,8) THEN 'Apr-Aug'
        ELSE 'Other'
    END
HAVING season != 'Other'
ORDER BY district, season;
