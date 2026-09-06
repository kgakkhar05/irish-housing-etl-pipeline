-- CREATE TABLE IF NOT EXISTS airflow_housing.reporting_housing (
--     year VARCHAR(4),
--     hpa05_statistic_code VARCHAR(20),
--     hpa05_statistic_label VARCHAR(255),
--     region VARCHAR(50),
--     region_label VARCHAR(255),
--     dwelling_type VARCHAR(50),
--     dwelling_type_label VARCHAR(255),
--     dwelling_status VARCHAR(50),
--     dwelling_status_label VARCHAR(255),
--     stamp_duty_event VARCHAR(50),
--     stamp_duty_event_label VARCHAR(255),
--     hpa05_value DECIMAL(15,2),
--     price_index DECIMAL(15,2),
--     yoy_percentage_change DECIMAL(15,2)
-- );
-- SELECT COUNT(*)
-- FROM airflow_housing.reporting_housing;

DELIMITER $$


CREATE PROCEDURE airflow_housing.sp_refresh_housing_reporting()

BEGIN
	TRUNCATE TABLE airflow_housing.reporting_housing;
    
    INSERT INTO airflow_housing.reporting_housing
    (
		year,
        hpa05_statistic_code,
        hpa05_statistic_label,
        region,
        region_label,
        dwelling_type,
        dwelling_type_label,
        dwelling_status,
        dwelling_status_label,
        stamp_duty_event,
        stamp_duty_event_label,
        hpa05_value,
        price_index,
        yoy_percentage_change
    )
    
    SELECT
		h05.year,
		h05.statistic_code AS hpa05_statistic_code, 
		h05.statistic_label AS hpa05_statistic_label,
		h05.region,
		h05.region_label,
		h05.dwelling_type,
		h05.dwelling_type_label,
		h05.dwelling_status,
		h05.dwelling_status_label,
		h05.stamp_duty_event,
		h05.stamp_duty_event_label, 
		h05.value AS hpa05_value,
        
		h13.price_index,
		h13.yoy_percentage_change
    
	FROM airflow_housing.raw_hpa05_data AS h05
	LEFT JOIN 
			(
		SELECT 
		year,
		CASE
			WHEN property_type_label LIKE 'National excluding Dublin%' THEN 'National excluding Dublin'
			WHEN property_type_label LIKE 'National%' THEN 'All'
			WHEN property_type_label LIKE 'Dublin City%' THEN 'Dublin City'
			WHEN property_type_label LIKE 'Dun Laoghaire-Rathdown%' THEN 'Dún Laoghaire-Rathdown'
			WHEN property_type_label LIKE 'Fingal%' THEN 'Fingal'
			WHEN property_type_label LIKE 'South Dublin%' THEN 'South Dublin'
			WHEN property_type_label LIKE 'Dublin%' THEN 'Dublin'
			WHEN property_type_label LIKE 'Midland%' THEN 'Midland'
			WHEN property_type_label LIKE 'West%' THEN 'West'
			WHEN property_type_label LIKE 'South-West%' THEN 'South-West'
			WHEN property_type_label LIKE 'Border%' THEN 'Border Excluding Louth'
			WHEN property_type_label LIKE 'Mid-East%' THEN 'Mid-East including Louth'
			WHEN property_type_label LIKE 'Mid-West%' THEN 'Mid-West including South Tipperary'
			WHEN property_type_label LIKE 'South-East%' THEN 'South-East excluding South Tipperary'
		END AS region_mapped,
		CASE
			WHEN property_type_label LIKE '%house%' THEN 'House'
			WHEN property_type_label LIKE '%apartment%' THEN 'Apartment'
			WHEN property_type_label LIKE '%all residential properties%' THEN 'All Dwelling Types'
		END AS dwelling_type_mapped,
		MAX(CASE
			WHEN statistic_code = 'HPA13C01' THEN value
		END) AS price_index,
		MAX(CASE
			WHEN statistic_code = 'HPA13C02' THEN value
		END) AS yoy_percentage_change
		
        FROM airflow_housing.raw_hpa13_data
        
	GROUP BY year , region_mapped , dwelling_type_mapped

	) AS h13

	ON h05.year = h13.year
	AND h05.region_label = h13.region_mapped
	AND h05.dwelling_type_label = h13.dwelling_type_mapped

	WHERE h05.year>=2010;
    
END $$
DELIMITER ;

CALL airflow_housing.sp_refresh_housing_reporting();


