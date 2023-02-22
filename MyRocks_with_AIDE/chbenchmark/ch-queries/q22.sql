select     substr(c_state,1,1) as country,
     count(*) as numcust,
     sum(c_balance) as totacctbal
from     customer
where     c_phone_p <= 7
     and c_balance > (select avg(c_BALANCE)
              from      customer
              where  c_balance > 0.00
                  and c_phone_p <= 7)
     and not exists (select *
             from    orders
             where    o_wdc = c_wdid)
group by substr(c_state,1,1)
order by substr(c_state,1,1) LIMIT 10;
