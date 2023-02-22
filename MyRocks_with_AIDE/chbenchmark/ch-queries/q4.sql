select    o_ol_cnt, count(*) as order_count
from    orders
    where exists (select *
            from order_line
            where o_wdid = ol_owd
            and ol_delivery_d >= o_entry_d)
group    by o_ol_cnt
order    by o_ol_cnt LIMIT 10;
