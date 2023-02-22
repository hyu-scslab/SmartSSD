/*+ Leading((order_line orders))
    HashJoin(order_line orders) */
select     c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
from     customer, orders, order_line, nation
where     c_w_id = o_w_id
     and c_d_id = o_d_id
     and c_id = o_c_id
     and ol_o_id = o_id
     and ol_w_id = o_w_id
     and ol_d_id = o_d_id
     and o_entry_d <= ol_delivery_d
--     and n_nationkey = ascii(substr(c_state,1,1))
     and n_nationkey = c_state_a
group by c_id, c_last, c_city, c_phone, n_name
order by revenue desc LIMIT 10;
