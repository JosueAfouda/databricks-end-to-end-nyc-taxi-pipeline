SELECT 
    pickup_date,
    pickup_borough,
    dropoff_borough,
    CONCAT(pickup_borough, ' - ', dropoff_borough) AS route,
    HOUR(tpep_pickup_datetime) AS pickup_hour,          -- pour le bar plot 0–23
    AVG(trip_distance) AS avg_trip_distance,  
    COUNT(*) AS number_trips,
    SUM(total_amount) AS total_revenue
FROM processed_trips_with_zones
WHERE pickup_borough NOT IN ('N/A', 'Unknown')
  AND dropoff_borough NOT IN ('N/A', 'Unknown')
GROUP BY 
    pickup_date,
    pickup_borough,
    dropoff_borough,
    HOUR(tpep_pickup_datetime);
