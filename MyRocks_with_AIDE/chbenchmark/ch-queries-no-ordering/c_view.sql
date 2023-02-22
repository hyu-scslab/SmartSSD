create or replace view revenue as (
     select s_m_id as supplier_no,
        sum(ol_amount) as total_revenue
     from order_line, stock
        where ol_i_id = s_i_id and ol_supply_w_id = s_w_id
     group by s_m_id);
