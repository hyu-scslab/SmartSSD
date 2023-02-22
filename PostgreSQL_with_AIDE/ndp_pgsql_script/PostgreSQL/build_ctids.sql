select pg_open_ctids(16402);
select * from customer;
select pg_close_ctids(16402);

select pg_open_ctids(16408);
select * from district;
select pg_close_ctids(16408);

select pg_open_ctids(16411);
select * from history;
select pg_close_ctids(16411);

select pg_open_ctids(16414);
select * from item;
select pg_close_ctids(16414);

select pg_open_ctids(16484);
select * from nation;
select pg_close_ctids(16484);

select pg_open_ctids(16423);
select * from new_order;
select pg_close_ctids(16423);

select pg_open_ctids(16429);
select * from order_line;
select pg_close_ctids(16429);

select pg_open_ctids(16426);
select * from orders;
select pg_close_ctids(16426);

select pg_open_ctids(16420);
select * from stock;
select pg_close_ctids(16420);

select pg_open_ctids(16490);
select * from supplier;
select pg_close_ctids(16490);

select pg_open_ctids(16487);
select * from region;
select pg_close_ctids(16487);

select pg_open_ctids(16417);
select * from warehouse;
select pg_close_ctids(16417);
