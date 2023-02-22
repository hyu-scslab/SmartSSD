select straight_join     o_ol_cnt,
     sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
     sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
from     order_line, orders
where ol_owd = o_idwd
     and o_entry_d <= ol_delivery_d
group by o_ol_cnt
order by o_ol_cnt LIMIT 10;
