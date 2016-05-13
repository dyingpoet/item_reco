SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

USE ${hiveconf:Database};



--CREATE EXTERNAL TABLE IF NOT EXISTS pythia.pis_member_item_trans_score_partition (
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:task_table} (
                membership_nbr INT
                , cardholder_nbr SMALLINT
                , membership_create_date STRING
                , system_item_nbr INT
                , score FLOAT
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS TEXTFILE
--LOCATION '${hiveconf:task_path}'
LOCATION '/user/pythia/Workspaces/SamsMEP/pis_member_item_trans_score_partition'
;


