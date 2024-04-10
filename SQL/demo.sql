SELECT sku_name, usage_date, sum(usage_quantity) as `DBUs consumed`
    FROM system.billing.usage
WHERE year(usage_date) = year(CURRENT_DATE)
AND sku_name = "ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)"
AND usage_date > "2023-04-15"
GROUP BY sku_name, usage_date