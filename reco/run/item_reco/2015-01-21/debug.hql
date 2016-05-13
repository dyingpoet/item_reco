set mapred.task.timeout 1800000
set hive.exec.reducers.bytes.per.reducer=81920000;
--set mapred.reduce.tasks=200;
set mapreduce.job.reduces=1000;
--SET mapred.output.compress 'true';
--SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
----set hive.exec.compress.output=false;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;
set hive.auto.convert.join=true;
set hive.mapred.supports.subdirectories=true;

USE jli21;


drop table if exists tmpItemReco;
create table tmpItemReco as
    SELECT
        a.membership_nbr
        , a.cardholder_nbr
        , a.membership_create_date
        , a.partition_system_item_nbr
        , MAX(a.score) as score
    FROM
        (
        SELECT
            b.membership_nbr
            , b.cardholder_nbr
            , b.membership_create_date
            , a.partition_system_item_nbr
            , a.score * b.score  as score
            --, a.system_item_nbr
        FROM            (
                SELECT
                    a.system_item_nbr
                    , a.seasonality_vst * b.score as score
                    , b.partition_system_item_nbr
                FROM
                    --(SELECT system_item_nbr, seasonality_vst, start_of_month FROM pythia.sams_combined_global_seasonality_by_month_item WHERE start_of_month='${hiveconf:seasonality_month}') a
                    (SELECT system_item_nbr, seasonality_vst, start_of_month FROM ${hiveconf:seasonality_tbl} WHERE start_of_month='${hiveconf:seasonality_month}') a
                JOIN
                    (
                    SELECT
                        system_item_nbr, partition_system_item_nbr, score
                    FROM
                        (SELECT system_item_nbr, partition_system_item_nbr, score, rank() over (PARTITION BY system_item_nbr ORDER BY score DESC) as rank FROM ${hiveconf:cobought_tbl} WHERE ds='${hiveconf:cobought_dt}' AND membership_type_code='${hiveconf:member_type}' AND region_nbr='all' AND system_item_nbr NOT IN (${hiveconf:anchor_blacklist}) AND partition_system_item_nbr NOT IN (${hiveconf:reco_blacklist})) a
                    WHERE rank == 1
                    ) b
                ON a.system_item_nbr = b.system_item_nbr
            ) a
        JOIN
            --(SELECT * FROM pythia.pis_member_item_trans_score_partition WHERE campaign_month='${hiveconf:trans_partition}') b
            (SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
        ON a.system_item_nbr = b.system_item_nbr
        ) a
        GROUP BY a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.partition_system_item_nbr
;




