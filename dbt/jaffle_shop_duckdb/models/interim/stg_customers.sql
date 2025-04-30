select
  id as customer_id,
  * exclude(id),
from {{ ref('raw_customers') }}
