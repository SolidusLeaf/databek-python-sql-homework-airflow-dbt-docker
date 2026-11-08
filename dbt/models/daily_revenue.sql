select
    order_date,
    count(*)    as order_count,
    sum(amount) as total_revenue
from {{ source('staging', 'orders_staging') }}
where status = 'completed'
group by order_date