select straight_join   sum(ol_amount) as revenue
from order_line, item
where    (
      ol_i_id = i_id
          and i_data like '%a'
          and ol_w_id_val <= 5
          and ol_quantity between 1 and 10
    ) or (
      ol_i_id = i_id
      and i_data like '%b'
      and ol_w_id_val <= 5
      and ol_quantity between 1 and 10
    ) or (
      ol_i_id = i_id
      and i_data like '%c'
      and ol_w_id_val <= 5
      and ol_quantity between 1 and 10
    ) LIMIT 10;
