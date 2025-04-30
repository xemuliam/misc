select *
from {{ source('external_source', 'raw_orders') }}
