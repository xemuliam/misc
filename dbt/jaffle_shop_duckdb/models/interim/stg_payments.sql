select
  id as payment_id,
  * exclude(id, amount),
  amount/100 as amount,
from {{ ref('raw_payments') }}
