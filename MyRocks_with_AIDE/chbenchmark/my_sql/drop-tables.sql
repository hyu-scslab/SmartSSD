-- ch benchamrk view
DROP VIEW IF EXISTS revenue;

DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS item;

-- ch benchmark tables
DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;

-- ch drop procedure
DROP PROCEDURE IF EXISTS NEWORD;
DROP PROCEDURE IF EXISTS DELIVERY;
DROP PROCEDURE IF EXISTS PAYMENT;
DROP PROCEDURE IF EXISTS OSTAT;
DROP PROCEDURE IF EXISTS SLEV;
