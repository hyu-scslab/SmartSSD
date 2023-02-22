select   su_name, su_address
from     supplier, nation
where    su_suppkey in
--        (select  mod(s_i_id * s_w_id, 10000)
        (select s_m_id
        from     stock, order_line
        where    s_i_id in
                (select i_id
                 from item
                 where i_data like 'co%')
             and ol_i_id=s_i_id
        group by s_i_id, s_w_id, s_quantity
        having   2*s_quantity > sum(ol_quantity))
     and su_nationkey = n_nationkey
     and n_name = 'Germany'
order by su_name LIMIT 10;
