SET @@session.s3d_forced_table_scan=TRUE;
SET @@session.ndp_src_table="STOCK";
SET @@session.ndp_max_joins=4;

explain select straight_join    n_name, extract(year from o_entry_d) as l_year, sum(ol_amount) as sum_profit
from     stock, supplier, item, order_line, orders, nation
where ol_suwi = s_wi
     and s_m_id = su_suppkey_val
     and ol_owd = o_idwd
     and ol_i_id = i_id_val
     and su_nationkey = n_nationkey
     and i_data_ra2 = 16962
group by n_name, extract(year from o_entry_d)
order by n_name, l_year desc LIMIT 10;
