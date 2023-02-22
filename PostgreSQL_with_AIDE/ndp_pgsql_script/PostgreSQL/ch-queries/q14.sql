/*+ Leading((order_line item))
    HashJoin(order_line item) */
select    100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1+sum(ol_amount)) as promo_revenue
from order_line, item
where ol_i_id = i_id
    LIMIT 10;
