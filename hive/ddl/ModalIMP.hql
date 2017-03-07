    creating hive table:

    ------------------------------

    SET hive.execution.engine=tez;

    SET tez.queue.name=etl;

    CREATE DATABASE IF NOT EXISTS ${hiveconf:cae_database};

    USE ${hiveconf:cae_database};

    DROP TABLE IF EXISTS cae.cae_report;

    create table cae_report(

        store_id int,

        reason string,

        reasoncount int)

        PARTITIONED BY (dt string) 

    ROW FORMAT DELIMITED

    FIELDS TERMINATED BY ','

    STORED AS TEXTFILE;

    Regular expersion table:

    ------------------------------------

    SET hive.execution.engine=tez;

    SET tez.queue.name=etl;

    CREATE DATABASE IF NOT EXISTS ${hiveconf:cae_database};

    USE ${hiveconf:cae_database};

    DROP TABLE IF EXISTS prsmn_npog_item;

    CREATE EXTERNAL TABLE prsmn_npog_item(

        NPOG_SEQ_I STRING,

        DEPT_I STRING,

        CLASS_I STRING,

        ITEM_I STRING,

        DEL_F STRING,

        UPDT_USER_I STRING ,

        UPDT_TS STRING)

    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'

    WITH SERDEPROPERTIES

        ("input.regex" = "(.{11})(.{6})(.{6})(.{6})(.{1})(.{10})(.*)")

    STORED AS ${hiveconf:prsmn_npog_item_stored_format}

    LOCATION ${hiveconf:prsmn_npog_item_location};

    ----------------------------------------

    Decimal point declaration 

    thresholdamount decimal(10,0),

    maxdiscount decimal(10,0),

    storeidentifiers array<int>,

    COLLECTION ITEMS TERMINATED BY ','

    ----------------------------------------------------------

    SET PARTITION_DATE;

    --SET c=to_date(from_unixtime(unix_timestamp()));

    --alter table item_tgtexpress_loc_rel  drop if exists partition (last_update_ts='to_date(from_unixtime(unix_timestamp()))');

    --alter table cae.item_tgtexpress_loc_rel drop if exists partition (last_update_ts='${hiveconf:PARTITION_DATE}');

    alter table cae.item_tgtexpress_loc_rel drop if exists partition (last_update_ts='${PARTITION_DATE}');
