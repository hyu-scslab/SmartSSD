SET global local_infile=1;

CREATE TABLE nation (
    n_nationkey int NOT NULL,
    n_nationkey_val int NOT NULL,
    n_name char(25) NOT NULL,
    n_regionkey int NOT NULL,
    n_comment char(152) NOT NULL,
    PRIMARY KEY(n_nationkey) COMMENT 'cf_name=cf25'
) ENGINE = rocksdb;


CREATE TABLE region (
    r_regionkey int NOT NULL,
    r_regionkey_val int NOT NULL,
    r_name char(55) NOT NULL,
    r_comment char(152) NOT NULL,
    PRIMARY KEY(r_regionkey) COMMENT 'cf_name=cf26'
) ENGINE = rocksdb;


CREATE TABLE supplier (
    su_suppkey int NOT NULL,
    su_suppkey_val int NOT NULL,
    su_nationkey int NOT NULL,
    su_name char(25) NOT NULL,
    su_address char(40) NOT NULL,
    su_phone char(15) NOT NULL,
    su_acctbal decimal(12,2) NOT NULL,
    su_comment char(101) NOT NULL,
    PRIMARY KEY(su_suppkey) COMMENT 'cf_name=cf27'
) ENGINE = rocksdb;


LOAD  DATA LOCAL INFILE './my_sql/nation_data.txt' INTO TABLE nation FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';


LOAD  DATA LOCAL INFILE './my_sql/region_data.txt' INTO TABLE region FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';


LOAD  DATA LOCAL INFILE './my_sql/supplier_data.txt' INTO TABLE supplier FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

#ALTER TABLE nation
#    ADD CONSTRAINT nation_n_regionkey_fkey FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey) ON DELETE CASCADE;


#ALTER TABLE supplier
#    ADD CONSTRAINT supplier_su_nationkey_fkey FOREIGN KEY (su_nationkey) REFERENCES nation(n_nationkey) ON DELETE CASCADE;
