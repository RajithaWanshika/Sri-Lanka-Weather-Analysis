USE weather_db;

INSERT OVERWRITE LOCAL DIRECTORY '/results/top_temperate_cities'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','

SELECT
    l.city_name,
    l.location_id,
    ROUND(AVG(w.temperature_2m_max), 2) AS avg_max_temperature,
    ROUND(MIN(w.temperature_2m_max), 2) AS min_temperature,
    ROUND(MAX(w.temperature_2m_max), 2) AS max_temperature,
    COUNT(*) AS observation_count
FROM
    weather_data w
JOIN
    location_data l ON w.location_id = l.location_id
WHERE
    w.temperature_2m_max IS NOT NULL
GROUP BY
    l.city_name, l.location_id
ORDER BY
    avg_max_temperature DESC
LIMIT 10;
