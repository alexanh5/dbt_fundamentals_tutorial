with orders as (
    
    select * from {{ ref('stg_jaffle_shop__orders') }}

),
payments as (
    
    select * from {{ ref('stg_strip__payments') }}

),

order_payments as (

    select 
        order_id, 
        sum(case when payment_status = 'success' then payment_amount end) as amount 
    from payments 
    group by 1

),

final as (

    select 
        o.customer_id,
        o.order_id,
        o.order_date,
        coalesce(cop.amount,0) as amount

    from orders o
    left join order_payments cop using (order_id)
)

select * from final