-- 1. Count of Incidents by Day of Week:
SELECT dd.day_name, COUNT(*) AS incident_count
FROM your_table_name fact
JOIN dim_date dd ON fact.incident_date = dd.date_actual
GROUP BY dd.day_name
ORDER BY dd.day_of_week;

-- 2. Average Number of Alarms by Month:
SELECT dd.month_name, AVG(fact.num_alarms) AS avg_alarms
FROM your_table_name fact
JOIN dim_date dd ON fact.incident_date = dd.date_actual
GROUP BY dd.month_name, dd.month_actual
ORDER BY dd.month_actual;

-- 3. Total Estimated Property Loss by Quarter:
SELECT dd.quarter_name, SUM(fact.estimated_property_loss) AS total_property_loss
FROM your_table_name fact
JOIN dim_date dd ON fact.incident_date = dd.date_actual
GROUP BY dd.quarter_name, dd.quarter_actual
ORDER BY dd.quarter_actual;

-- 4. Incident Count and Average Personnel Response by Year:
SELECT dd.year_actual, COUNT(*) AS incident_count, AVG(fact.suppression_personnel + fact.ems_personnel + fact.other_personnel) AS avg_personnel_response
FROM your_table_name fact
JOIN dim_date dd ON fact.incident_date = dd.date_actual
GROUP BY dd.year_actual
ORDER BY dd.year_actual;

-- 5. Percentage of Incidents with Fatalities by Weekend/Weekday:
SELECT dd.weekend_indr, (SUM(fact.fire_fatalities) + SUM(fact.civilian_fatalities)) / COUNT(*) * 100 AS percentage_fatalities
FROM your_table_name fact
JOIN dim_date dd ON fact.incident_date = dd.date_actual
GROUP BY dd.weekend_indr;

-- 6. Top 5 Days with the Highest Average Number of Alarms:
SELECT dd.date_actual, AVG(fact.num_alarms) AS avg_alarms
FROM your_table_name fact
JOIN dim_date dd ON fact.incident_date = dd.date_actual
GROUP BY dd.date_actual
ORDER BY avg_alarms DESC
LIMIT 5;
