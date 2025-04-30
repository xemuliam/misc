select
  id as order_id,
  user_id as customer_id,
  * exclude(id, user_id),
from {{ ref('raw_orders') }}
