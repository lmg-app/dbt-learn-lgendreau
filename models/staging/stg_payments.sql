select
    orderid as order_id,
    paymentmethod,
    status,
    amount / 100 as amount,
    created,
    _batched_at
from raw.stripe.payment