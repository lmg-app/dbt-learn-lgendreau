with customers AS (
    SELECT
        customer_id
    FROM
        {{ ref('stg_customers') }}
),

orders AS (
    SELECT
        order_id,
        customer_id,
        order_date
    FROM    
      {{ ref('stg_orders') }}

),

payments AS (
    SELECT  
        order_id,
        SUM(amount) as amount
    FROM    
        {{ ref('stg_payments')}}
    WHERE
        status = 'success'
    GROUP BY
        order_id
)

SELECT  
  customer_id,
  order_id,
  order_date,
  sum(payments.amount) as amount
FROM
    orders INNER JOIN payments using (order_id)
     INNER JOIN customers USING (customer_id)
GROUP BY
    1,2,3