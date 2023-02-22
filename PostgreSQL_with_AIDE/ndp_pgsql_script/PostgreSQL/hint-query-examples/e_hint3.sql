/*+ SeqScan(order_line)
    SeqScan(orders)
    SeqScan(new_order)
    SeqScan(customer)
    Leading(order_line orders new_order customer) 
    HashJoin(order_line orders) 
    HashJoin(order_line orders new_order) 
    HashJoin(order_line orders new_order customer) */
explain
select   ol_o_id, ol_w_id, ol_d_id,
     sum(ol_amount) as revenue, o_entry_d
from      customer, new_order, orders, order_line
where      c_state like 'A%'
     and c_id = o_c_id
     and c_w_id = o_w_id
     and c_d_id = o_d_id
     and no_w_id = o_w_id
     and no_d_id = o_d_id
     and no_o_id = o_id
     and ol_w_id = o_w_id
     and ol_d_id = o_d_id
     and ol_o_id = o_id
group by ol_o_id, ol_w_id, ol_d_id, o_entry_d
order by revenue desc, o_entry_d LIMIT 10;
