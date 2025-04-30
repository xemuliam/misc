select *
from {{ source('external_source', 'raw_payments') }}
