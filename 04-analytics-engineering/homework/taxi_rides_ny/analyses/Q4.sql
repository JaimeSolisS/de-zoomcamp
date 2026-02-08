SELECT pickup_zone, sum(revenue_monthly_total_amount) as total_revenue
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE CAST(revenue_month as string) like '2020%'
AND service_type = 'Green'
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
