set mapred.task.timeout 1800000
set hive.exec.reducers.bytes.per.reducer=81920000;
--set mapred.reduce.tasks=200;
set mapreduce.job.reduces=1000;
--SET mapred.output.compress 'true';
--SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
----set hive.exec.compress.output=false;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;
set hive.auto.convert.join=true;
set hive.mapred.supports.subdirectories=true;

USE jli21;


drop table if exists tmpItemReco;
create table tmpItemReco as
--INSERT OVERWRITE TABLE tmpItemReco
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
        FROM 
            (
                SELECT
                    a.system_item_nbr
                    , a.seasonality_vst * b.score as score
                    , b.partition_system_item_nbr
                    , b.rank
                FROM
                    --(SELECT system_item_nbr, seasonality_vst, start_of_month FROM pythia.sams_combined_global_seasonality_by_month_item WHERE start_of_month='${hiveconf:seasonality_month}') a
                    (SELECT CAST(system_item_nbr as INT) AS system_item_nbr, seasonality_vst, start_of_month FROM ${hiveconf:seasonality_tbl} WHERE start_of_month='${hiveconf:seasonality_month}') a
                JOIN
                    (
                    SELECT
                        system_item_nbr, partition_system_item_nbr, score, rank
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


drop table if exists tmpCatCnt;
create table tmpCatCnt as
        SELECT 
            membership_nbr, cardholder_nbr, membership_create_date, category_nbr, count(distinct system_item_nbr) AS num_items
        FROM
            (
            SELECT a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.system_item_nbr, b.category_nbr, b.sub_category_nbr FROM (SELECT membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}')  a JOIN sams_us_clubs.item_info b ON a.system_item_nbr = b.system_item_nbr
            ) a
        GROUP BY membership_nbr, cardholder_nbr, membership_create_date,  category_nbr
;

set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;

--INSERT OVERWRITE DIRECTORY '/user/pythia/Workspaces/SamsMEP/Recommend/Scoring/cnp_member/2016-01-04/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug'
INSERT OVERWRITE DIRECTORY '${hiveconf:item_reco_landing_dir}'
SELECT
    a.*
FROM
    (
    SELECT
        a.membership_nbr
        , a.cardholder_nbr
        , a.membership_create_date
        , a.partition_system_item_nbr
        , CASE WHEN b.num_items IS NULL THEN a.score ELSE a.score * (1+num_items) END as score
    FROM
        (
        SELECT
            a.*, b.category_nbr, b.sub_category_nbr 
        FROM
            tmpItemReco a
        LEFT OUTER JOIN
            sams_us_clubs.item_info b
        ON a.partition_system_item_nbr = b.system_item_nbr
        ) a
    LEFT OUTER JOIN
        tmpCatCnt b
    ON a.category_nbr = b.category_nbr and a.membership_nbr = b.membership_nbr and a.cardholder_nbr = b.cardholder_nbr and a.membership_create_date = b.membership_create_date
    ) a
LEFT OUTER JOIN
    --(SELECT * FROM pythia.pis_member_item_trans_score_partition WHERE campaign_month='2016-01-04') b
    (SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b 
ON a.partition_system_item_nbr = b.system_item_nbr
    AND a.membership_nbr = b.membership_nbr
    AND a.cardholder_nbr = b.cardholder_nbr
    AND a.membership_create_date = b.membership_create_date
WHERE b.system_item_nbr IS NULL
;




