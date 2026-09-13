CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `vw_housing_reporting` AS
    SELECT `reporting_housing`.`year`,
    `reporting_housing`.`hpa05_statistic_code`,
    `reporting_housing`.`hpa05_statistic_label`,
    `reporting_housing`.`region`,
    `reporting_housing`.`region_label`,
    `reporting_housing`.`dwelling_type`,
    `reporting_housing`.`dwelling_type_label`,
    `reporting_housing`.`dwelling_status`,
    `reporting_housing`.`dwelling_status_label`,
    `reporting_housing`.`stamp_duty_event`,
    `reporting_housing`.`stamp_duty_event_label`,
    `reporting_housing`.`hpa05_value`,
    `reporting_housing`.`price_index`,
    `reporting_housing`.`yoy_percentage_change`
FROM `airflow_housing`.`reporting_housing`;