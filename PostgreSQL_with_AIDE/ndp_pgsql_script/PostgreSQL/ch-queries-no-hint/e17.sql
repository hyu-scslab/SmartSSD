explain
select    sum(ol_amount) / 2.0 as avg_yearly
from order_line, (select   i_id, avg(ol_quantity) as a
            from     item, order_line
--            where    i_data like '%b'
--            where    i_data_r like 'b%'
            where    i_data_ra1 = 98
                 and ol_i_id = i_id
            group by i_id) t
where ol_i_id = t.i_id
    and ol_quantity < t.a LIMIT 10;
