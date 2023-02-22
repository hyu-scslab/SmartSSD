select     i_name,
     substr(i_data, 1, 3) as brand,
     i_price,
--     count(distinct (mod((s_w_id * s_i_id),10000))) as supplier_cnt
     count(distinct s_m_id) as supplier_cnt
from     stock, item
where     i_id = s_i_id
     and i_data not like 'zz%'
--     and (mod((s_w_id * s_i_id),10000) not in
     and (s_m_id not in
        (select su_suppkey
         from supplier
         where su_comment like '%bad%'))
group by i_name, substr(i_data, 1, 3), i_price
order by supplier_cnt desc LIMIT 10;
