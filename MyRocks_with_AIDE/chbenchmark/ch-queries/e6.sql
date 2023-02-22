explain
select    sum(ol_amount) as revenue
from order_line
where ol_quantity between 1 and 100000 LIMIT 10;
