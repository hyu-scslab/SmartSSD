explain
select straight_join     su_nationkey as supp_nation,
     c_state_a as cust_nation,
     extract(year from o_entry_d) as l_year,
     sum(ol_amount) as revenue
from     supplier, stock, order_line, orders, customer, nation n1, nation n2
where     ol_supply_w_id = s_w_id
     and ol_i_id = s_i_id
     and s_m_id = su_suppkey
     and ol_o_id = o_id
     and ol_w_id = o_w_id
     and ol_d_id = o_d_id
     and c_w_id = o_w_id
     and c_d_id = o_d_id
     and c_id = o_c_id
     and su_nationkey = n1.n_nationkey
     and c_state_a = n2.n_nationkey
     and (
        (n1.n_name = 'Germany' and n2.n_name = 'Cambodia')
         or
        (n1.n_name = 'Cambodia' and n2.n_name = 'Germany')
         )
group by su_nationkey, c_state_a, extract(year from o_entry_d)
order by su_nationkey, cust_nation, l_year LIMIT 10;
