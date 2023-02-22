select straight_join    su_name, count(*) as numwait
from     supplier, nation, stock, order_line l1, orders
where   ol_owd = o_idwd    
     and ol_wi = s_wi
     and s_m_id  = su_suppkey
     and l1.ol_delivery_d > o_entry_d
     and not exists (select *
             from order_line l2
             where  l2.ol_owd = l1.ol_owd
                and l2.ol_delivery_d > l1.ol_delivery_d)
     and su_nationkey = n_nationkey
     and n_name = 'Germany'
group by su_name
order by numwait desc, su_name LIMIT 10;
