{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with
order_payments as (
  select
    order_id,

    {%- for payment_method in payment_methods -%}
    sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
    {% endfor -%}

    sum(amount) as total_amount,
  from {{ ref('stg_payments') }}
  group by all
)

select
  o.order_id,
  o.customer_id,
  o.order_date,
  o.status,

  {%- for payment_method in payment_methods -%}
  op.{{ payment_method }}_amount,
  {% endfor -%}

    op.total_amount as amount,
from {{ ref('stg_orders') }} o
left join order_payments op using(order_id)
