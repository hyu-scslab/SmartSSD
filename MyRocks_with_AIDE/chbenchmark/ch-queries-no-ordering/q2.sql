select      su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
from     item, supplier, stock, nation, region,
     (select s_i_id as m_i_id,
         min(s_quantity) as m_s_quantity
     from     stock, supplier, nation, region
     where s_m_id=su_suppkey
          and su_nationkey=n_nationkey
          and n_regionkey=r_regionkey
          and r_name like 'Europ%'
     group by s_i_id) m
where      i_id = s_i_id
     and s_m_id = su_suppkey
     and su_nationkey = n_nationkey
     and n_regionkey = r_regionkey
     and i_data_ra1 = 98
     and r_name like 'Europ%'
     and i_id=m_i_id
     and s_quantity = m_s_quantity
order by n_name, su_name, i_id LIMIT 10;
