SET @@session.s3d_forced_table_scan=TRUE;
SET @@session.ndp_src_table="CUSTOMER";
SET @@session.ndp_max_joins=4;

select straight_join     c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
from     customer, orders, order_line, nation
where c_wdid = o_wdc
     and o_idwd = ol_owd
     and o_entry_d <= ol_delivery_d
     and n_nationkey = c_state_a
group by c_id, c_last, c_city, c_phone, n_name
order by revenue desc LIMIT 10;
