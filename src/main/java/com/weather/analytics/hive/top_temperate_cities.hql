USE weather_db;

INSERT OVERWRITE LOCAL DIRECTORY '/results/top_temperate_cities'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','

SELECT
    l.city_name AS city,
    AVG(w.temperature_2m_max) AS avg_max_temperature
FROM weather_data w
JOIN location_data l
ON w.location_id = l.location_id
WHERE w.temperature_2m_max IS NOT NULL
GROUP BY l.city_name
ORDER BY avg_max_temperature ASC
LIMIT 10;
