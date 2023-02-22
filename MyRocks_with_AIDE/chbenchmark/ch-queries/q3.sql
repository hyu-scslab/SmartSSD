select ol_o_id, ol_w_id, ol_d_id,
     sum(ol_amount) as revenue, o_entry_d
from      orders, customer, new_order, order_line
where      c_state like 'A%'
     and c_wdid = o_wdc
     and no_owd = o_idwd
     and ol_owd = o_idwd
group by ol_o_id, ol_w_id, ol_d_id, o_entry_d
order by revenue desc, o_entry_d LIMIT 10;
