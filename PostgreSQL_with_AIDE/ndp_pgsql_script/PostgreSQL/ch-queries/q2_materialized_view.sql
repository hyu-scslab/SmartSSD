create materialized view m as
	select s_i_id as m_i_id, min(s_quantity) as m_s_quantity
		from     stock, supplier, nation, region
--		where     mod((s_w_id*s_i_id),10000)=su_suppkey
		where     s_m_id=su_suppkey
    		and su_nationkey=n_nationkey
    		and n_regionkey=r_regionkey
    		and r_name like 'Europ%'
		group by s_i_id;
