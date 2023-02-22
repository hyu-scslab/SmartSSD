select     c_count, count(*) as custdist
from     (select c_id, count(o_id) as c_count
     from customer left outer join orders on (
        c_wdid = o_wdc
        and o_carrier_id > 8)
     group by c_id) as c_orders
group by c_count
order by custdist desc, c_count desc LIMIT 10;
