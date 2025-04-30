with
customer_orders as (
  select
    customer_id,
    min(order_date) as first_order,
    max(order_date) as most_recent_order,
    count(order_id) as number_of_orders,
  from {{ ref('stg_orders') }}
  group by all
)
,
customer_payments as (
    select
      o.customer_id,
      sum(amount) as total_amount,
    from {{ ref('stg_payments') }} p
    left join {{ ref('stg_orders') }} o using(order_id)
    group by all
)

select
  c.customer_id,
  c.first_name,
  c.last_name,
  co.first_order,
  co.most_recent_order,
  co.number_of_orders,
  cp.total_amount as customer_lifetime_value,
from {{ ref('stg_customers') }} c
left join customer_orders co using(customer_id)
left join customer_payments cp using(customer_id)
