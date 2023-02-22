select     extract(year from o_entry_d) as l_year,
     sum(case when n2.n_name = 'Germany' then ol_amount else 0 end) / sum(ol_amount) as mkt_share
from     item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region
where     i_id = s_i_id
     and ol_suwi = s_wi
     and s_m_id = su_suppkey
     and ol_owd = o_idwd
     and c_wdid = o_wdc
     and n1.n_nationkey = c_state_a
     and n1.n_regionkey = r_regionkey
     and ol_i_id < 1000
     and r_name = 'Europe'
     and su_nationkey = n2.n_nationkey
--     and i_data like '%b'
--     and i_data_r like 'b%'
     and i_data_ra1 = 98
     and i_id = ol_i_id
group by extract(year from o_entry_d)
order by l_year LIMIT 10;
