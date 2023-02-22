#/bin/bash

for t in "customer" "item" "nation" "new_order" "order_line" "orders" "stock" "supplier" "region"
do
  oid=`echo "select '${t}'::regclass::oid;" > ctid.sql ; pgsql/bin/psql -p 7777 chbench -f ctid.sql | xargs | awk '{print$3}'`
  echo "select pg_open_ctids(${oid}); select * from ${t}; select pg_close_ctids(${oid});" > ctid.sql
  pgsql/bin/psql -p 7777 chbench -f ctid.sql
done

ls -alR ./data/base | grep ctids
