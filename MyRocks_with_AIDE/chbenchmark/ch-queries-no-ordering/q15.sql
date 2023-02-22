select     su_suppkey, su_name, su_address, su_phone, total_revenue
from     supplier, revenue
where     su_suppkey = supplier_no
     and total_revenue = (select max(total_revenue) from revenue)
order by su_suppkey LIMIT 10;
