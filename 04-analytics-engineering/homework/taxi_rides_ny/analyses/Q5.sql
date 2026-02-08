SELECT SUM(total_monthly_trips) as total_trips
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE revenue_month = '2019-10-01'
and service_type = 'Green'